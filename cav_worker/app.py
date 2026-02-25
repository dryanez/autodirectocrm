# CAV Worker — Registro Civil Robot Agent
#
# Standalone microservice that uses Playwright (headless Chromium)
# to fetch CAV (Certificado de Anotaciones Vigentes) from
# registrocivil.cl for any given vehicle plate.
#
# CAPTCHA solving: Uses 2captcha.com (human solvers, ~$1/1000 CAPTCHAs).
#
# Deploy on Railway (needs a real server, not Vercel serverless).
#
# Environment variables:
#   TWOCAPTCHA_API_KEY  — your 2captcha.com API key
#   CAV_SECRET          — shared secret so only your CRM can call this
#   PORT                — defaults to 8090
#
# Usage:
#   POST /cav
#   Body: { "plate": "ABCD12", "secret": "<CAV_SECRET>" }
#   Returns: { "ok": true, "status": "clean"|"annotations", "owner": "...", "annotations": [...] }

import os
import base64
import json
import re
import time
import traceback

from flask import Flask, jsonify, request, Response

app = Flask(__name__)

CAV_SECRET = os.environ.get("CAV_SECRET", "cav-autodirecto-2026")
TWOCAPTCHA_API_KEY = os.environ.get("TWOCAPTCHA_API_KEY", "")
MAX_RETRIES = 3

# ─── Lazy-load Playwright to avoid startup cost on healthchecks ───────────────
_browser = None
_playwright = None


def _get_browser():
    """Get or create a persistent Chromium browser instance."""
    global _browser, _playwright
    if _browser and _browser.is_connected():
        return _browser
    from playwright.sync_api import sync_playwright
    _playwright = sync_playwright().start()
    _browser = _playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--single-process",
        ],
    )
    print("[cav_worker] Chromium browser launched", flush=True)
    return _browser


def _solve_captcha_with_2captcha(image_bytes: bytes) -> str:
    """
    Send the CAPTCHA image to 2captcha.com (human solvers).
    Returns the CAPTCHA text string.
    Cost: ~$0.001 per solve (humans solve it in ~15-30 seconds).
    """
    from twocaptcha import TwoCaptcha

    if not TWOCAPTCHA_API_KEY:
        raise RuntimeError("TWOCAPTCHA_API_KEY not set — cannot solve CAPTCHA")

    solver = TwoCaptcha(TWOCAPTCHA_API_KEY)

    # Save image bytes to a temp file (2captcha SDK needs a file path or base64)
    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        tmp.write(image_bytes)
        tmp_path = tmp.name

    try:
        result = solver.normal(tmp_path)
        captcha_text = result["code"].strip()
        print(f"[cav_worker] 2captcha solved: '{captcha_text}'", flush=True)
        return captcha_text
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def _fetch_cav(plate: str, debug: bool = False, on_step=None) -> dict:
    """
    Use Playwright to navigate registrocivil.cl, solve the entry CAPTCHA,
    then navigate: Vehículos → Certificado de Anotaciones Vigentes →
    enter plate → Agregar a carro → scrape whatever data is available.
    When debug=True, captures screenshots at every step.
    on_step(data_dict) is called for each step in real-time (SSE streaming).
    """
    steps = []  # list of {"step": str, "screenshot": base64_str}

    def _snap(page, step_name):
        """Capture a screenshot for debug/stream mode."""
        if not debug and not on_step:
            return
        try:
            shot = page.screenshot(type="jpeg", quality=50, full_page=True)
            step_data = {
                "step": step_name,
                "screenshot": base64.b64encode(shot).decode(),
                "time": round(time.time() - _start, 1),
                "url": page.url,
            }
        except Exception:
            step_data = {"step": step_name, "screenshot": None, "time": round(time.time() - _start, 1)}
        if debug:
            steps.append(step_data)
        if on_step:
            on_step(step_data)

    _start = time.time()
    plate = plate.replace("-", "").replace(" ", "").upper()
    browser = _get_browser()
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
        locale="es-CL",
    )
    page = context.new_page()

    try:
        # ── Step 1: Go to the main page ──
        url = "https://www.registrocivil.cl/OficinaInternet/"
        print(f"[cav_worker] Step 1: Navigating to {url}", flush=True)
        page.goto(url, timeout=60000, wait_until="domcontentloaded")
        page.wait_for_timeout(3000)
        _snap(page, "1. Página principal cargada")

        # ── Step 2: Solve the entry CAPTCHA ──
        for attempt in range(MAX_RETRIES):
            print(f"[cav_worker] CAPTCHA attempt {attempt + 1}/{MAX_RETRIES}", flush=True)
            body_text = page.content()

            # Check if we're already past the CAPTCHA (no challenge on page)
            if "código de la imagen" not in body_text.lower() and "resolver el desafío" not in body_text.lower():
                print("[cav_worker] No CAPTCHA gate — already inside!", flush=True)
                _snap(page, "✅ Sin CAPTCHA — acceso directo")
                break

            # Find the CAPTCHA image
            captcha_img = None
            img_elements = page.query_selector_all("img")
            for img in img_elements:
                src = img.get_attribute("src") or ""
                alt = (img.get_attribute("alt") or "").lower()
                if "logo" in src.lower() or "icon" in src.lower():
                    continue
                if "red dot" in alt or "Red dot" in (img.get_attribute("alt") or ""):
                    continue
                if src.startswith("data:image") or "servlet" in src.lower() or "captcha" in src.lower():
                    try:
                        captcha_bytes = img.screenshot()
                        if captcha_bytes and len(captcha_bytes) > 500:
                            captcha_img = captcha_bytes
                            print(f"[cav_worker] Found CAPTCHA image ({len(captcha_bytes)} bytes)", flush=True)
                            break
                    except Exception:
                        continue

            # Fallback: find by size
            if not captcha_img:
                for img in img_elements:
                    try:
                        box = img.bounding_box()
                        if box and box["width"] > 80 and box["height"] > 30 and box["width"] < 400:
                            captcha_bytes = img.screenshot()
                            if captcha_bytes and len(captcha_bytes) > 500:
                                captcha_img = captcha_bytes
                                break
                    except Exception:
                        continue

            if not captcha_img:
                _snap(page, "❌ No se encontró imagen CAPTCHA")
                result = {"ok": False, "error": "Could not locate CAPTCHA image on page"}
                if debug:
                    result["steps"] = steps
                return result

            _snap(page, f"2. CAPTCHA encontrado (intento {attempt+1})")
            captcha_text = _solve_captcha_with_2captcha(captcha_img)
            if not captcha_text:
                continue

            # Type the CAPTCHA answer
            input_el = page.query_selector('input[type="text"]')
            if not input_el:
                input_el = page.query_selector('input[name*="captcha"]') or \
                           page.query_selector('input[name*="codigo"]') or \
                           page.query_selector('input[name*="code"]') or \
                           page.query_selector('input:not([type="hidden"]):not([type="submit"])')
            if not input_el:
                result = {"ok": False, "error": "Could not find CAPTCHA input field"}
                if debug:
                    result["steps"] = steps
                return result

            input_el.click()
            input_el.fill("")
            input_el.type(captcha_text, delay=50)
            _snap(page, f"3. CAPTCHA escrito: '{captcha_text}'")

            # Submit
            submit_btn = page.query_selector('input[type="submit"]') or \
                         page.query_selector('button[type="submit"]') or \
                         page.query_selector('input[value*="Enviar"]') or \
                         page.query_selector('input[value*="Consultar"]')
            if not submit_btn:
                submit_btn = page.query_selector('button') or \
                             page.query_selector('input[type="button"]')
            if submit_btn:
                submit_btn.click()
            else:
                input_el.press("Enter")

            page.wait_for_timeout(3000)
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
            _snap(page, f"4. Después de enviar CAPTCHA")

            # Check if CAPTCHA was wrong (still on challenge page)
            result_body = page.content()
            if "código de la imagen" in result_body.lower() or "resolver el desafío" in result_body.lower():
                print(f"[cav_worker] CAPTCHA attempt {attempt + 1} failed", flush=True)
                _snap(page, f"🔄 Intento {attempt + 1} falló — CAPTCHA incorrecto")
                page.goto(url, timeout=60000, wait_until="domcontentloaded")
                page.wait_for_timeout(3000)
                continue

            # We got through!
            print("[cav_worker] CAPTCHA solved! Inside the site.", flush=True)
            _snap(page, "✅ CAPTCHA resuelto — dentro del sitio")
            break
        else:
            # All CAPTCHA attempts failed
            _snap(page, f"❌ CAPTCHA incorrecto tras {MAX_RETRIES} intentos")
            fail_result = {"ok": False, "error": f"Could not solve CAPTCHA after {MAX_RETRIES} attempts"}
            if debug:
                fail_result["steps"] = steps
            return fail_result

        # ── DIAGNOSTIC: Dump page structure right after CAPTCHA ──
        # This tells us the exact text/links available on the post-CAPTCHA page
        try:
            page_links = page.evaluate("""() => {
                const results = [];
                document.querySelectorAll('a, button, input[type=submit], input[type=button]').forEach(el => {
                    const txt = (el.innerText || el.value || el.textContent || '').trim();
                    const href = el.href || '';
                    const id = el.id || '';
                    const cls = el.className || '';
                    if (txt) results.push({tag: el.tagName, txt: txt.substring(0,80), href: href.substring(0,80), id, cls: cls.substring(0,40)});
                });
                return results;
            }""")
            print("[cav_worker] CLICKABLE ELEMENTS after CAPTCHA:", flush=True)
            for el in page_links:
                print(f"  | <{el['tag']}> '{el['txt']}' id='{el['id']}' href='{el['href']}'", flush=True)
        except Exception as e:
            print(f"[cav_worker] Could not dump links: {e}", flush=True)

        # ── Step 3: Click on "Vehículos" to expand the section ──
        print("[cav_worker] Step 3: Clicking Vehículos...", flush=True)
        page.wait_for_timeout(2000)

        vehiculos_clicked = False

        # Try XPath which is the most reliable for text matching
        for xpath in [
            "//a[normalize-space(text())='Vehículos']",
            "//span[normalize-space(text())='Vehículos']",
            "//li[normalize-space(text())='Vehículos']",
            "//td[normalize-space(text())='Vehículos']",
            "//*[normalize-space(text())='Vehículos']",
            "//*[contains(text(),'Vehículos')]",
            "//*[contains(text(),'Vehiculos')]",
        ]:
            try:
                el = page.locator(f"xpath={xpath}").first
                if el.count() > 0 and el.is_visible():
                    el.click()
                    vehiculos_clicked = True
                    print(f"[cav_worker] Clicked Vehículos via XPath: {xpath}", flush=True)
                    break
            except Exception:
                continue

        page.wait_for_timeout(3000)  # Give accordion time to animate open
        _snap(page, "5. Después de clickear Vehículos")

        # Dump ALL clickable elements again to see what appeared after clicking Vehículos
        try:
            post_v_links = page.evaluate("""() => {
                const results = [];
                document.querySelectorAll('a, button, li, td, label, span').forEach(el => {
                    const txt = (el.innerText || el.textContent || '').trim();
                    if (txt && txt.length < 100) results.push({tag: el.tagName, txt, visible: el.offsetParent !== null});
                });
                return results.filter(r => r.visible).slice(0, 60);
            }""")
            print("[cav_worker] VISIBLE ELEMENTS after clicking Vehículos:", flush=True)
            for el in post_v_links:
                print(f"  | <{el['tag']}> '{el['txt']}'", flush=True)
        except Exception as e:
            print(f"[cav_worker] Could not dump post-click elements: {e}", flush=True)

        if not vehiculos_clicked:
            _snap(page, "❌ No se pudo clickear 'Vehículos'")
            result = {"ok": False, "error": "Could not find/click 'Vehículos' menu item"}
            if debug:
                result["steps"] = steps
            return result

        # ── Step 4: Click "Certificado Vehículos de anotaciones Vigentes" ──
        print("[cav_worker] Step 4: Clicking Certificado de Anotaciones Vigentes...", flush=True)
        page.wait_for_timeout(2000)

        cav_clicked = False

        # Try XPath text matching — most reliable approach
        for xpath in [
            "//*[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚÑÜ','abcdefghijklmnopqrstuvwxyzáéíóúñü'),'anotaciones vigentes')]",
            "//*[contains(text(),'anotaciones Vigentes')]",
            "//*[contains(text(),'Anotaciones Vigentes')]",
            "//*[contains(text(),'anotaciones vigentes')]",
            "//*[contains(text(),'Anotaciones vigentes')]",
            "//*[contains(text(),'Certificado Vehículos')]",
            "//a[contains(text(),'anotacion')]",
            "//li[contains(text(),'anotacion')]",
            "//td[contains(text(),'anotacion')]",
            "//label[contains(text(),'anotacion')]",
            "//span[contains(text(),'anotacion')]",
        ]:
            try:
                el = page.locator(f"xpath={xpath}").first
                if el.count() > 0 and el.is_visible():
                    el.click()
                    cav_clicked = True
                    print(f"[cav_worker] Clicked CAV via XPath: {xpath}", flush=True)
                    break
            except Exception:
                continue

        # Fallback: use JS to find and click anything with "anotacion" in text
        if not cav_clicked:
            try:
                result_js = page.evaluate("""() => {
                    const all = document.querySelectorAll('a, li, td, label, span, div, p, button');
                    for (const el of all) {
                        const txt = (el.innerText || el.textContent || '').toLowerCase();
                        if (txt.includes('anotacion') && el.offsetParent !== null) {
                            el.click();
                            return el.innerText || el.textContent || 'clicked';
                        }
                    }
                    return null;
                }""")
                if result_js:
                    cav_clicked = True
                    print(f"[cav_worker] Clicked CAV via JS: '{result_js[:60]}'", flush=True)
            except Exception as e:
                print(f"[cav_worker] JS click failed: {e}", flush=True)

        page.wait_for_timeout(3000)  # Wait for the plate input form to appear
        _snap(page, "6. Después de clickear Certificado Anotaciones")

        if not cav_clicked:
            # Dump full body text so we see exactly what's on screen
            try:
                body_txt = page.inner_text("body")
                lines = [l.strip() for l in body_txt.splitlines() if l.strip()]
                print("[cav_worker] FULL PAGE TEXT (all lines):", flush=True)
                for l in lines:
                    print(f"  | {l}", flush=True)
            except Exception:
                pass
            _snap(page, "❌ No se encontró opción 'Certificado de Anotaciones Vigentes'")
            result = {"ok": False, "error": "Could not find 'Certificado de Anotaciones Vigentes' option"}
            if debug:
                result["steps"] = steps
            return result

        # ── Step 5: Enter the plate number ──
        print(f"[cav_worker] Step 5: Entering plate {plate}...", flush=True)
        page.wait_for_timeout(2000)

        # Look for the plate input field
        plate_input = None
        for selector in [
            'input[name*="patente" i]',
            'input[name*="placa" i]',
            'input[name*="ppu" i]',
            'input[name*="PPU"]',
            'input[name*="vehiculo" i]',
            'input[placeholder*="patente" i]',
            'input[placeholder*="placa" i]',
            'input[placeholder*="PPU" i]',
            'input[placeholder*="ppu" i]',
            'input[placeholder*="ingrese" i]',
            'input[id*="patente" i]',
            'input[id*="ppu" i]',
            'input[id*="placa" i]',
        ]:
            try:
                el = page.query_selector(selector)
                if el and el.is_visible():
                    plate_input = el
                    print(f"[cav_worker] Found plate input: {selector}", flush=True)
                    break
            except Exception:
                continue

        # Fallback: find any visible text input that's NOT the captcha field
        if not plate_input:
            try:
                text_inputs = page.query_selector_all('input[type="text"], input:not([type])')
                for inp in text_inputs:
                    try:
                        if inp.is_visible():
                            name = inp.get_attribute("name") or ""
                            id_ = inp.get_attribute("id") or ""
                            placeholder = inp.get_attribute("placeholder") or ""
                            # Skip the captcha input
                            if "captcha" in name.lower() or "captcha" in id_.lower() or "codigo" in name.lower():
                                continue
                            plate_input = inp
                            print(f"[cav_worker] Found fallback text input: name='{name}' id='{id_}' placeholder='{placeholder}'", flush=True)
                            break
                    except Exception:
                        continue
            except Exception:
                pass

        if not plate_input:
            # Dump all inputs for debugging
            try:
                inputs = page.query_selector_all('input')
                print("[cav_worker] ALL INPUTS ON PAGE:", flush=True)
                for inp in inputs:
                    try:
                        attrs = inp.evaluate("""el => ({
                            type: el.type, name: el.name, id: el.id,
                            placeholder: el.placeholder, visible: el.offsetParent !== null,
                            value: el.value
                        })""")
                        print(f"  | {attrs}", flush=True)
                    except Exception:
                        pass
            except Exception:
                pass
            _snap(page, "❌ No se encontró campo para ingresar patente")
            result = {"ok": False, "error": "Could not find plate input field"}
            if debug:
                result["steps"] = steps
            return result

        plate_input.click()
        plate_input.fill("")
        plate_input.type(plate, delay=50)
        _snap(page, f"7. Patente escrita: {plate}")

        # ── Step 6: Click "Agregar a Carro" or similar button ──
        print("[cav_worker] Step 6: Clicking Agregar a Carro...", flush=True)
        page.wait_for_timeout(500)

        agregar_clicked = False
        for text_match in [
            "Agregar",
            "agregar",
            "Agregar a Carro",
            "Agregar al Carro",
            "Consultar",
            "Buscar",
        ]:
            try:
                el = page.locator(f"text={text_match}").first
                if el.is_visible():
                    el.click()
                    agregar_clicked = True
                    print(f"[cav_worker] Clicked: '{text_match}'", flush=True)
                    break
            except Exception:
                continue

        # Also try submit buttons
        if not agregar_clicked:
            for selector in [
                'input[type="submit"]',
                'button[type="submit"]',
                'input[value*="Agregar"]',
                'input[value*="Consultar"]',
            ]:
                try:
                    el = page.query_selector(selector)
                    if el and el.is_visible():
                        el.click()
                        agregar_clicked = True
                        break
                except Exception:
                    continue

        page.wait_for_timeout(3000)
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        _snap(page, "8. Después de Agregar a Carro")

        # ── Step 7: Capture the result page ──
        print("[cav_worker] Step 7: Reading result page...", flush=True)
        result_html = page.content()
        _snap(page, "9. Página de resultado final")

        # Check if we got any vehicle/CAV data
        if _has_cav_data(result_html):
            print("[cav_worker] Found CAV data on page!", flush=True)
            _snap(page, "✅ Datos CAV encontrados")
            result = _parse_cav_page(result_html, plate)
            if debug:
                result["steps"] = steps
            return result

        # Check if it's a payment/cart page (common for registrocivil.cl)
        html_lower = result_html.lower()
        is_cart = "carro de certificados" in html_lower or "total $" in html_lower or "carro está vacío" in html_lower
        is_payment = "pagar" in html_lower or "webpay" in html_lower or "tarjeta" in html_lower

        if is_cart or is_payment:
            print("[cav_worker] Hit the payment/cart page — CAV requires payment", flush=True)
            _snap(page, "💰 Página de pago — CAV es un certificado pagado")

            # Extract whatever info we can from the cart page
            text = re.sub(r'<[^>]+>', ' ', result_html)
            text = re.sub(r'\s+', ' ', text).strip()

            # Look for price
            price_match = re.search(r'Total\s*\$\s*([\d.,]+)', text)
            price = price_match.group(1) if price_match else "desconocido"

            result = {
                "ok": False,
                "error": "CAV requires payment on registrocivil.cl",
                "action": "payment_required",
                "message": f"El Certificado de Anotaciones Vigentes es un documento pagado (${price}). Debe obtenerse manualmente en registrocivil.cl.",
                "price": price,
                "page_text": text[:2000],
            }
            if debug:
                result["steps"] = steps
            return result

        # Check for error messages from the site
        if "error" in html_lower or "ha ocurrido un error" in html_lower:
            _snap(page, "⚠️ Página de error del sitio")
            result = {
                "ok": False,
                "error": "registrocivil.cl returned an error page",
                "page_text": re.sub(r'<[^>]+>', ' ', result_html)[:2000].strip(),
            }
            if debug:
                result["steps"] = steps
            return result

        # Unknown page state
        _snap(page, "⚠️ Estado desconocido de la página")
        result = {
            "ok": False,
            "error": "Unknown page state after navigation",
            "page_text": re.sub(r'<[^>]+>', ' ', result_html)[:2000].strip(),
        }
        if debug:
            result["steps"] = steps
        return result

    except Exception as e:
        print(f"[cav_worker] Error: {e}", flush=True)
        traceback.print_exc()
        err_result = {"ok": False, "error": str(e)}
        if debug:
            try:
                _snap(page, f"💥 Error: {str(e)[:80]}")
            except Exception:
                pass
            err_result["steps"] = steps
        return err_result
    finally:
        try:
            context.close()
        except Exception:
            pass


def _has_cav_data(html: str) -> bool:
    """Check if the HTML contains actual CAV result data (not a CAPTCHA page)."""
    html_lower = html.lower()
    # Must NOT have captcha indicators
    if "código de la imagen" in html_lower or "resolver el desafío" in html_lower:
        return False
    # Must HAVE vehicle/owner data indicators
    indicators = [
        "propietario", "inscripci", "anotacion", "prenda",
        "vehículo", "vehiculo", "placa", "patente",
        "nombre", "rut", "marca", "modelo", "año",
        "nº motor", "chasis",
    ]
    found = sum(1 for ind in indicators if ind in html_lower)
    return found >= 3  # At least 3 indicators = likely real data


def _parse_cav_page(html: str, plate: str) -> dict:
    """Parse the CAV result page and extract structured data."""
    result = {
        "ok": True,
        "plate": plate,
        "status": "clean",
        "owner_name": "",
        "owner_rut": "",
        "vehicle_info": {},
        "annotations": [],
        "raw_text": "",
    }

    # Strip HTML tags for easier text parsing
    import re
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text).strip()
    result["raw_text"] = text[:3000]  # Keep first 3000 chars for debugging

    # ─── Owner name ───
    owner_patterns = [
        r'(?:Nombre|Propietario)\s*:?\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑa-záéíóúñ\s]{3,50})',
        r'(?:nombre|propietario)\s*:?\s*([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñÁÉÍÓÚÑ\s]{3,50})',
    ]
    for pat in owner_patterns:
        m = re.search(pat, text)
        if m:
            name = m.group(1).strip()
            # Avoid capturing generic labels
            if name.lower() not in ("del propietario", "del vehiculo", "del vehículo"):
                result["owner_name"] = name
                break

    # ─── Owner RUT ───
    rut_match = re.search(r'(?:RUT|Rut|R\.U\.T)\s*:?\s*([\d]{1,2}\.[\d]{3}\.[\d]{3}-[\dkK])', text)
    if rut_match:
        result["owner_rut"] = rut_match.group(1)

    # ─── Vehicle info ───
    marca_match = re.search(r'(?:Marca)\s*:?\s*([A-Za-záéíóúñÁÉÍÓÚÑ\s]{2,30})', text)
    if marca_match:
        result["vehicle_info"]["marca"] = marca_match.group(1).strip()

    modelo_match = re.search(r'(?:Modelo)\s*:?\s*([A-Za-z0-9áéíóúñÁÉÍÓÚÑ\s\-]{2,40})', text)
    if modelo_match:
        result["vehicle_info"]["modelo"] = modelo_match.group(1).strip()

    year_match = re.search(r'(?:Año|año)\s*:?\s*(\d{4})', text)
    if year_match:
        result["vehicle_info"]["year"] = int(year_match.group(1))

    color_match = re.search(r'(?:Color)\s*:?\s*([A-Za-záéíóúñÁÉÍÓÚÑ\s]{2,20})', text)
    if color_match:
        result["vehicle_info"]["color"] = color_match.group(1).strip()

    motor_match = re.search(r'(?:Motor|Nº Motor|N° Motor)\s*:?\s*([A-Za-z0-9\-]{3,30})', text)
    if motor_match:
        result["vehicle_info"]["motor"] = motor_match.group(1).strip()

    chasis_match = re.search(r'(?:Chasis|VIN)\s*:?\s*([A-Za-z0-9\-]{5,30})', text)
    if chasis_match:
        result["vehicle_info"]["chasis"] = chasis_match.group(1).strip()

    # ─── Annotations (prendas, embargos, prohibiciones) ───
    annotation_keywords = [
        "prenda", "embargo", "prohibición", "prohibicion",
        "alzamiento", "gravamen", "limitación", "limitacion",
        "medida precautoria", "anotación vigente", "anotacion vigente",
    ]

    text_lower = text.lower()
    for kw in annotation_keywords:
        if kw in text_lower:
            # Try to extract the full annotation text
            pattern = rf'({kw}[^.;]*[.;]?)'
            matches = re.findall(pattern, text_lower)
            for m in matches:
                ann_text = m.strip().capitalize()
                if ann_text and len(ann_text) > 5:
                    result["annotations"].append(ann_text)

    # Deduplicate annotations
    result["annotations"] = list(dict.fromkeys(result["annotations"]))

    # Determine status
    if result["annotations"]:
        result["status"] = "annotations"
    else:
        # Also check for explicit "sin anotaciones" or "no registra"
        if "sin anotaciones" in text_lower or "no registra" in text_lower or "libre" in text_lower:
            result["status"] = "clean"
        elif any(kw in text_lower for kw in annotation_keywords):
            result["status"] = "annotations"
        else:
            result["status"] = "clean"

    # Clean up annotations text
    result["annotations_text"] = "; ".join(result["annotations"][:10]) if result["annotations"] else ""

    return result


# ─── Flask Routes ─────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "cav-worker", "version": "1.0.0"})


@app.route("/health", methods=["GET"])
def healthcheck():
    return jsonify({"status": "ok"})


@app.route("/cav", methods=["POST"])
def fetch_cav():
    """
    Main endpoint. Accepts a plate number, navigates registrocivil.cl,
    solves the CAPTCHA with Claude Vision, and returns the CAV data.

    Body: { "plate": "ABCD12", "secret": "<CAV_SECRET>" }
    """
    data = request.json or {}

    # Auth check
    secret = data.get("secret", "")
    if secret != CAV_SECRET:
        return jsonify({"ok": False, "error": "Invalid secret"}), 403

    plate = (data.get("plate") or "").strip().upper().replace("-", "").replace(" ", "")
    if not plate or len(plate) < 4:
        return jsonify({"ok": False, "error": "Invalid plate number"}), 400

    print(f"\n{'='*60}", flush=True)
    print(f"[cav_worker] Fetching CAV for plate: {plate}", flush=True)
    print(f"{'='*60}", flush=True)

    start_time = time.time()
    result = _fetch_cav(plate)
    elapsed = time.time() - start_time

    result["elapsed_seconds"] = round(elapsed, 1)
    print(f"[cav_worker] Done in {elapsed:.1f}s — status: {result.get('status', 'error')}", flush=True)

    # Don't send the raw_text or debug_screenshot in success responses (too big)
    if result.get("ok"):
        result.pop("raw_text", None)
        result.pop("debug_screenshot", None)

    status_code = 200 if result.get("ok") else 500
    return jsonify(result), status_code


@app.route("/cav-debug", methods=["POST"])
def fetch_cav_debug():
    """
    Debug endpoint — same as /cav but returns step-by-step screenshots.
    Body: { "plate": "ABCD12", "secret": "<CAV_SECRET>" }
    """
    data = request.json or {}

    secret = data.get("secret", "")
    if secret != CAV_SECRET:
        return jsonify({"ok": False, "error": "Invalid secret"}), 403

    plate = (data.get("plate") or "").strip().upper().replace("-", "").replace(" ", "")
    if not plate or len(plate) < 4:
        return jsonify({"ok": False, "error": "Invalid plate number"}), 400

    print(f"\n{'='*60}", flush=True)
    print(f"[cav_worker] 🔍 DEBUG — Fetching CAV for plate: {plate}", flush=True)
    print(f"{'='*60}", flush=True)

    start_time = time.time()
    result = _fetch_cav(plate, debug=True)
    elapsed = time.time() - start_time

    result["elapsed_seconds"] = round(elapsed, 1)
    print(f"[cav_worker] DEBUG done in {elapsed:.1f}s — {len(result.get('steps', []))} screenshots captured", flush=True)

    status_code = 200 if result.get("ok") else 500
    return jsonify(result), status_code


@app.route("/cav-stream", methods=["GET"])
def fetch_cav_stream():
    """
    SSE (Server-Sent Events) endpoint — streams screenshots in real-time.
    GET /cav-stream?plate=ABCD12&secret=xxx
    Each event: data: {"step": "...", "screenshot": "base64...", "time": 1.2, "url": "..."}
    Final event: data: {"done": true, "result": {...}}
    """
    plate = (request.args.get("plate") or "").strip().upper().replace("-", "").replace(" ", "")
    secret = request.args.get("secret", "")

    if secret != CAV_SECRET:
        def error_stream():
            yield f"data: {json.dumps({'error': 'Invalid secret', 'done': True})}\n\n"
        return Response(error_stream(), mimetype="text/event-stream")

    if not plate or len(plate) < 4:
        def error_stream():
            yield f"data: {json.dumps({'error': 'Invalid plate', 'done': True})}\n\n"
        return Response(error_stream(), mimetype="text/event-stream")

    print(f"\n{'='*60}", flush=True)
    print(f"[cav_worker] 📡 STREAM — Fetching CAV for plate: {plate}", flush=True)
    print(f"{'='*60}", flush=True)

    def generate():
        import queue, threading

        q = queue.Queue()
        start_time = time.time()

        def on_step(step_data):
            q.put(("step", step_data))

        def worker():
            try:
                result = _fetch_cav(plate, debug=False, on_step=on_step)
                q.put(("done", result))
            except Exception as e:
                q.put(("done", {"ok": False, "error": str(e)}))

        t = threading.Thread(target=worker, daemon=True)
        t.start()

        while True:
            try:
                msg_type, data = q.get(timeout=300)
                if msg_type == "step":
                    yield f"data: {json.dumps(data)}\n\n"
                elif msg_type == "done":
                    data["done"] = True
                    data["elapsed_seconds"] = round(time.time() - start_time, 1)
                    yield f"data: {json.dumps(data, default=str)}\n\n"
                    break
            except Exception:
                yield f"data: {json.dumps({'error': 'Timeout', 'done': True})}\n\n"
                break

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
    port = int(os.environ.get("PORT", 8090))
    print(f"\n🤖 CAV Worker — Registro Civil Robot Agent")
    print(f"   http://127.0.0.1:{port}")
    print(f"   POST /cav  {'{'}\"plate\": \"ABCD12\", \"secret\": \"...\"{'}'}")
    print(f"\n   Requires: ANTHROPIC_API_KEY, CAV_SECRET env vars\n")
    app.run(debug=True, port=port, host="0.0.0.0")
