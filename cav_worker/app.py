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
import re
import time
import traceback

from flask import Flask, jsonify, request

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


def _fetch_cav(plate: str) -> dict:
    """
    Use Playwright to navigate registrocivil.cl, solve the CAPTCHA,
    and scrape the CAV data for the given plate.
    """
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
        url = f"https://www.registrocivil.cl/OficinaInternet/servlet/DetalleCarro?carro={plate}"
        print(f"[cav_worker] Navigating to {url}", flush=True)
        page.goto(url, timeout=60000, wait_until="domcontentloaded")

        # Wait for the page to fully load
        page.wait_for_timeout(3000)

        for attempt in range(MAX_RETRIES):
            print(f"[cav_worker] CAPTCHA attempt {attempt + 1}/{MAX_RETRIES}", flush=True)

            # Check if we already got through (no CAPTCHA)
            body_text = page.content()
            if _has_cav_data(body_text):
                print("[cav_worker] No CAPTCHA — direct access!", flush=True)
                return _parse_cav_page(body_text, plate)

            # Look for the CAPTCHA image
            captcha_img = None

            # Try to find the captcha image element
            # The page shows an image followed by a text input
            img_elements = page.query_selector_all("img")
            for img in img_elements:
                src = img.get_attribute("src") or ""
                alt = (img.get_attribute("alt") or "").lower()
                # Skip known non-captcha images (logos, icons, red dot)
                if "logo" in src.lower() or "icon" in src.lower():
                    continue
                if "red dot" in alt or "Red dot" in (img.get_attribute("alt") or ""):
                    continue
                # The CAPTCHA image is typically inline base64 or a servlet URL
                if src.startswith("data:image") or "servlet" in src.lower() or "captcha" in src.lower():
                    # Screenshot this specific element
                    try:
                        captcha_bytes = img.screenshot()
                        if captcha_bytes and len(captcha_bytes) > 500:
                            captcha_img = captcha_bytes
                            print(f"[cav_worker] Found CAPTCHA image ({len(captcha_bytes)} bytes)", flush=True)
                            break
                    except Exception:
                        continue

            if not captcha_img:
                # Fallback: try to find by taking a screenshot of a specific region
                # or look for any substantial image
                for img in img_elements:
                    try:
                        box = img.bounding_box()
                        if box and box["width"] > 80 and box["height"] > 30 and box["width"] < 400:
                            captcha_bytes = img.screenshot()
                            if captcha_bytes and len(captcha_bytes) > 500:
                                captcha_img = captcha_bytes
                                print(f"[cav_worker] Found CAPTCHA by size ({box['width']}x{box['height']})", flush=True)
                                break
                    except Exception:
                        continue

            if not captcha_img:
                print("[cav_worker] Could not find CAPTCHA image on page", flush=True)
                # Take full page screenshot for debugging
                debug_shot = page.screenshot()
                return {
                    "ok": False,
                    "error": "Could not locate CAPTCHA image on page",
                    "debug_screenshot": base64.b64encode(debug_shot).decode() if debug_shot else None,
                }

            # Solve the CAPTCHA with 2captcha (human solvers, ~15-30s)
            captcha_text = _solve_captcha_with_2captcha(captcha_img)

            if not captcha_text:
                print("[cav_worker] Claude returned empty CAPTCHA text", flush=True)
                continue

            # Find the text input and type the CAPTCHA answer
            input_el = page.query_selector('input[type="text"]')
            if not input_el:
                # Try other selectors
                input_el = page.query_selector('input[name*="captcha"]') or \
                           page.query_selector('input[name*="codigo"]') or \
                           page.query_selector('input[name*="code"]') or \
                           page.query_selector('input:not([type="hidden"]):not([type="submit"])')

            if not input_el:
                return {"ok": False, "error": "Could not find CAPTCHA input field"}

            # Clear and type
            input_el.click()
            input_el.fill("")
            input_el.type(captcha_text, delay=50)

            # Find and click the submit button
            submit_btn = page.query_selector('input[type="submit"]') or \
                         page.query_selector('button[type="submit"]') or \
                         page.query_selector('input[value*="submit"]') or \
                         page.query_selector('input[value*="Enviar"]') or \
                         page.query_selector('input[value*="Consultar"]')

            if not submit_btn:
                # Try clicking any button-like element
                submit_btn = page.query_selector('button') or \
                             page.query_selector('input[type="button"]')

            if submit_btn:
                submit_btn.click()
            else:
                # Press Enter as fallback
                input_el.press("Enter")

            # Wait for response
            page.wait_for_timeout(3000)
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass

            # Check if we got through
            result_body = page.content()

            # Check if CAPTCHA was wrong (page reloads with CAPTCHA again)
            if "código de la imagen" in result_body.lower() or "resolver el desafío" in result_body.lower():
                print(f"[cav_worker] CAPTCHA attempt {attempt + 1} failed — wrong code", flush=True)
                # Reload page for fresh CAPTCHA
                page.goto(url, timeout=60000, wait_until="domcontentloaded")
                page.wait_for_timeout(3000)
                continue

            # Check if we got CAV data
            if _has_cav_data(result_body):
                print("[cav_worker] CAPTCHA solved! Parsing CAV data...", flush=True)
                return _parse_cav_page(result_body, plate)

            # Unknown state — maybe partial load
            print(f"[cav_worker] Attempt {attempt + 1}: unclear response, retrying...", flush=True)
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            page.wait_for_timeout(3000)

        # All retries exhausted
        return {
            "ok": False,
            "error": f"Could not solve CAPTCHA after {MAX_RETRIES} attempts",
            "retries": MAX_RETRIES,
        }

    except Exception as e:
        print(f"[cav_worker] Error: {e}", flush=True)
        traceback.print_exc()
        return {"ok": False, "error": str(e)}
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8090))
    print(f"\n🤖 CAV Worker — Registro Civil Robot Agent")
    print(f"   http://127.0.0.1:{port}")
    print(f"   POST /cav  {'{'}\"plate\": \"ABCD12\", \"secret\": \"...\"{'}'}")
    print(f"\n   Requires: ANTHROPIC_API_KEY, CAV_SECRET env vars\n")
    app.run(debug=True, port=port, host="0.0.0.0")
