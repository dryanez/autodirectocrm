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
            "--disable-blink-features=AutomationControlled",
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


def _fetch_cav(plate: str, email: str = "felipe@autodirecto.cl", debug: bool = False, on_step=None) -> dict:
    """
    Use Playwright to navigate registrocivil.cl, solve the entry CAPTCHA,
    then navigate: Vehículos → Certificado de Anotaciones Vigentes →
    enter plate → Agregar a carro → fill email → Continuar → payment page.
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

    # Anti-bot detection: override navigator.webdriver and other headless indicators.
    # registrocivil.cl detects headless Chromium and closes the page without this.
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        Object.defineProperty(navigator, 'languages', {
            get: () => ['es-CL', 'es', 'en']
        });
        window.chrome = { runtime: {} };
    """)

    page = context.new_page()

    try:
        # ── Step 1: Go to the main page ──
        url = "https://www.registrocivil.cl/OficinaInternet/"
        print(f"[cav_worker] Step 1: Navigating to {url}", flush=True)
        page.goto(url, timeout=60000, wait_until="domcontentloaded")
        # Wait for all JS to initialize (CRITICAL: the certificate list
        # is rendered by JS after domcontentloaded)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        page.wait_for_timeout(5000)
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

        # ── Step 3: Expand the "Vehículos" accordion ──
        # Structure from local testing:
        #   <div class="titleGrupos" id="title_5"> → click to expand
        #   <div class="divListaClass" id="divLista_5" style="display:none"> → becomes visible
        #   Inside: <table id="certificadosTable">
        #
        # The site uses jQuery + its own JS. The accordion click handler calls
        # a function that toggles display AND may lazy-load certificate rows.
        # On Railway (headless), Playwright .click() fires but the accordion
        # doesn't expand (even though it works locally). So we use a multi-layer
        # strategy: try clicks, then find & call the page's own JS toggle function,
        # then as last resort force the DOM open AND check if content exists.
        print("[cav_worker] Step 3: Expanding 'Vehículos' accordion...", flush=True)
        page.wait_for_timeout(2000)

        # VERIFY we're on the right page first
        current_url = page.url
        print(f"[cav_worker] Current URL: {current_url}", flush=True)

        page_state = page.evaluate("""() => {
            const hasCaptcha = document.body.innerText.toLowerCase().includes('código de la imagen');
            const title5 = document.getElementById('title_5');
            const divLista5 = document.getElementById('divLista_5');
            const certTable = document.getElementById('certificadosTable');
            const allTitleGrupos = Array.from(document.querySelectorAll('.titleGrupos')).map(
                el => ({ id: el.id, text: el.innerText.trim().substring(0,50) })
            );
            // Find all global functions that contain "grupo" or "lista" or "accordion"
            const fnNames = Object.getOwnPropertyNames(window).filter(n => {
                try {
                    return typeof window[n] === 'function' &&
                        /grupo|lista|accordion|toggle|expand|mostrar|cert/i.test(n);
                } catch(e) { return false; }
            });
            // Check for onclick handler on title_5
            let onclickStr = '';
            if (title5) {
                onclickStr = title5.getAttribute('onclick') || '';
                if (!onclickStr && title5.onclick) onclickStr = title5.onclick.toString().substring(0,200);
            }
            return {
                hasCaptcha,
                hasTitle5: !!title5,
                title5Onclick: onclickStr,
                hasDivLista5: !!divLista5,
                divLista5Display: divLista5 ? window.getComputedStyle(divLista5).display : null,
                divLista5ChildCount: divLista5 ? divLista5.children.length : 0,
                hasCertTable: !!certTable,
                certTableRows: certTable ? certTable.querySelectorAll('tr').length : 0,
                titleGrupos: allTitleGrupos,
                relevantFunctions: fnNames.slice(0,20),
                jqueryLoaded: typeof jQuery !== 'undefined',
                bodyLen: document.body.innerHTML.length,
            };
        }""")
        print(f"[cav_worker] Page state: {json.dumps(page_state, indent=2)}", flush=True)

        if page_state.get("hasCaptcha"):
            print("[cav_worker] ❌ STILL ON CAPTCHA PAGE!", flush=True)
            _snap(page, "❌ Todavía en página CAPTCHA")
            result = {"ok": False, "error": "CAPTCHA was not solved — still on challenge page"}
            if debug:
                result["steps"] = steps
            return result

        _snap(page, "5. Página post-CAPTCHA (buscando accordion Vehículos)")

        # ── Helper to check if accordion is expanded ──
        def _is_vehiculos_expanded():
            return page.evaluate("""() => {
                const d = document.getElementById('divLista_5');
                if (!d) return { expanded: false, reason: 'divLista_5 not found' };
                const display = window.getComputedStyle(d).display;
                const hasContent = d.children.length > 0;
                const certTable = document.getElementById('certificadosTable');
                const hasRows = certTable ? certTable.querySelectorAll('tr').length > 0 : false;
                return {
                    expanded: display !== 'none',
                    display,
                    hasContent,
                    childCount: d.children.length,
                    hasCertTable: !!certTable,
                    certRows: hasRows ? certTable.querySelectorAll('tr').length : 0,
                };
            }""")

        # ── Method 1: Playwright click on #title_5 ──
        try:
            el = page.locator("#title_5")
            if el.count() > 0:
                el.scroll_into_view_if_needed()
                el.click()
                page.wait_for_timeout(2000)
                state = _is_vehiculos_expanded()
                print(f"[cav_worker] Method 1 (Playwright click #title_5): {state}", flush=True)
                if state.get("expanded"):
                    print("[cav_worker] ✅ Method 1 worked!", flush=True)
        except Exception as e:
            print(f"[cav_worker] Method 1 failed: {e}", flush=True)

        state = _is_vehiculos_expanded()
        if not state.get("expanded"):
            # ── Method 2: Call the page's own onclick handler ──
            try:
                onclick_attr = page_state.get("title5Onclick", "")
                if onclick_attr:
                    print(f"[cav_worker] Method 2: Calling onclick directly: {onclick_attr}", flush=True)
                    page.evaluate(f"() => {{ {onclick_attr} }}")
                    page.wait_for_timeout(2000)
                    state = _is_vehiculos_expanded()
                    print(f"[cav_worker] Method 2 result: {state}", flush=True)
                else:
                    print("[cav_worker] Method 2: No onclick attribute found", flush=True)
            except Exception as e:
                print(f"[cav_worker] Method 2 failed: {e}", flush=True)

        state = _is_vehiculos_expanded()
        if not state.get("expanded"):
            # ── Method 3: Find the site's toggle function by inspecting event handlers ──
            try:
                toggle_result = page.evaluate("""() => {
                    // Many Chilean gov sites use a function pattern like:
                    // function mostrarGrupo(num) or similar
                    // Let's try common function names with argument 5
                    const fns = ['mostrarGrupo', 'toggleGrupo', 'expandGrupo', 'abrirGrupo',
                                 'mostrarLista', 'toggleLista', 'cargarCertificados',
                                 'showGroup', 'toggleGroup', 'openGroup'];
                    for (const fn of fns) {
                        if (typeof window[fn] === 'function') {
                            try {
                                window[fn](5);
                                return 'called ' + fn + '(5)';
                            } catch(e) {
                                try { window[fn]('5'); return 'called ' + fn + "('5')"; }
                                catch(e2) {}
                            }
                        }
                    }
                    // Try to extract the function from jQuery event handlers on #title_5
                    if (typeof jQuery !== 'undefined') {
                        try {
                            const events = jQuery._data(jQuery('#title_5')[0], 'events');
                            if (events && events.click) {
                                for (const h of events.click) {
                                    try { h.handler.call(jQuery('#title_5')[0]); return 'called jQuery handler'; }
                                    catch(e) {}
                                }
                            }
                        } catch(e) {}
                        // Also try triggering via jQuery namespace
                        try {
                            jQuery('#title_5').trigger('click');
                            return 'jQuery trigger';
                        } catch(e) {}
                    }
                    // Try simulating the full click event chain
                    const el = document.getElementById('title_5');
                    if (el) {
                        const rect = el.getBoundingClientRect();
                        const x = rect.left + rect.width/2;
                        const y = rect.top + rect.height/2;
                        for (const evtType of ['pointerdown','mousedown','pointerup','mouseup','click']) {
                            el.dispatchEvent(new PointerEvent(evtType, {
                                bubbles: true, cancelable: true, view: window,
                                clientX: x, clientY: y, pointerId: 1,
                                pointerType: 'mouse', button: 0, buttons: 1
                            }));
                        }
                        return 'dispatched pointer+mouse events';
                    }
                    return 'no method worked';
                }""")
                print(f"[cav_worker] Method 3 result: {toggle_result}", flush=True)
                page.wait_for_timeout(2000)
                state = _is_vehiculos_expanded()
                print(f"[cav_worker] Method 3 state: {state}", flush=True)
            except Exception as e:
                print(f"[cav_worker] Method 3 failed: {e}", flush=True)

        state = _is_vehiculos_expanded()
        if not state.get("expanded"):
            # ── Method 4: Force-click with Playwright + force=True on multiple targets ──
            for selector in ["#title_5", "#arrowDown_5", ".titleGrupos >> text=Vehículos", "text=Vehículos"]:
                try:
                    el = page.locator(selector).first
                    if el.count() > 0:
                        el.click(force=True)
                        page.wait_for_timeout(1500)
                        state = _is_vehiculos_expanded()
                        print(f"[cav_worker] Method 4 ({selector}): {state}", flush=True)
                        if state.get("expanded"):
                            break
                except Exception as e:
                    print(f"[cav_worker] Method 4 ({selector}) failed: {e}", flush=True)

        state = _is_vehiculos_expanded()
        if not state.get("expanded"):
            # ── Method 5 (Nuclear): Force divLista_5 visible AND ensure content exists ──
            print("[cav_worker] ⚠️ All click methods failed. Nuclear fallback...", flush=True)
            nuclear_result = page.evaluate("""() => {
                const d = document.getElementById('divLista_5');
                if (!d) return { ok: false, reason: 'divLista_5 not found in DOM' };
                
                // Force visible
                d.style.display = 'block';
                d.style.visibility = 'visible';
                d.style.height = 'auto';
                d.style.overflow = 'visible';
                d.style.opacity = '1';
                
                // Also remove any 'collapsed' or 'hidden' classes
                d.classList.remove('collapsed', 'hidden', 'hide');
                
                // Rotate the arrow to indicate expanded state
                const arrow = document.getElementById('arrowDown_5');
                if (arrow) arrow.style.transform = 'rotate(0deg)';
                
                // Check what's inside
                const certTable = document.getElementById('certificadosTable');
                const hasRows = certTable ? certTable.querySelectorAll('tr').length : 0;
                const innerHTML = d.innerHTML.substring(0, 500);
                
                return {
                    ok: true,
                    display: window.getComputedStyle(d).display,
                    childCount: d.children.length,
                    hasCertTable: !!certTable,
                    certRows: hasRows,
                    contentPreview: innerHTML,
                };
            }""")
            print(f"[cav_worker] Nuclear result: {json.dumps(nuclear_result, indent=2)}", flush=True)
            _snap(page, "6b. Nuclear fallback — divLista_5 forzado visible")

            # If certTable has no rows, the content was loaded by AJAX when accordion clicked.
            # We need to trigger that AJAX call.
            if nuclear_result.get("ok") and nuclear_result.get("certRows", 0) == 0:
                print("[cav_worker] ⚠️ divLista_5 is visible but EMPTY — content loaded via AJAX", flush=True)
                # Try to find & call the AJAX loading function
                ajax_result = page.evaluate("""() => {
                    // Look for any XHR/fetch that loads certificates
                    // Common patterns on registrocivil.cl:
                    // 1. Look at jQuery.ajax calls or $.get calls
                    // 2. Try window.cargarCertificados or similar
                    
                    // Search ALL functions on window for anything related
                    const results = [];
                    for (const key of Object.getOwnPropertyNames(window)) {
                        try {
                            if (typeof window[key] === 'function') {
                                const src = window[key].toString().substring(0, 300);
                                if (/certificado|divLista|title_|grupo/i.test(src)) {
                                    results.push({ name: key, preview: src.substring(0, 200) });
                                }
                            }
                        } catch(e) {}
                    }
                    
                    // Also check for inline scripts
                    const scripts = Array.from(document.querySelectorAll('script')).map(
                        s => s.textContent.substring(0, 300)
                    ).filter(s => /divLista|title_|grupo|certificado/i.test(s));
                    
                    return { functions: results.slice(0,10), scripts: scripts.slice(0,5) };
                }""")
                print(f"[cav_worker] AJAX detection: {json.dumps(ajax_result, indent=2)}", flush=True)

        page.wait_for_timeout(2000)
        _snap(page, "6. Después de expandir Vehículos")

        # Final verification — is the accordion expanded AND has content?
        final_state = _is_vehiculos_expanded()
        print(f"[cav_worker] Step 3 FINAL state: {json.dumps(final_state)}", flush=True)

        if not final_state.get("expanded"):
            _snap(page, "❌ No se pudo expandir 'Vehículos'")
            result = {"ok": False, "error": "Could not expand 'Vehículos' accordion",
                      "page_state": page_state, "final_state": final_state}
            if debug:
                result["steps"] = steps
            return result

        # ── Step 4: Click checkbox for "Certificado Vehículos de anotaciones Vigentes" ──
        # The site uses iCheck jQuery plugin — native checkboxes are hidden behind
        # <ins class="iCheck-helper"> which intercepts pointer events.
        # Must use jQuery iCheck API: $('input').iCheck('check')
        print("[cav_worker] Step 4: Clicking CAV certificate...", flush=True)
        page.wait_for_timeout(1000)

        cav_clicked = False

        # Strategy 1: Use jQuery iCheck API (the correct way for this site)
        try:
            icheck_result = page.evaluate("""() => {
                const rows = document.querySelectorAll('tr');
                for (const row of rows) {
                    const txt = (row.innerText || '').toLowerCase();
                    if (txt.includes('anotaciones vigentes') && !txt.includes('multas')) {
                        const cb = row.querySelector('input[type=checkbox], input[type=radio]');
                        if (!cb) continue;
                        cb.scrollIntoView({block: 'center'});
                        if (typeof jQuery !== 'undefined' && typeof jQuery.fn.iCheck !== 'undefined') {
                            jQuery(cb).iCheck('check');
                            return { method: 'iCheck_api', cbId: cb.id, cbName: cb.name, checked: cb.checked, rowText: txt.substring(0,80) };
                        }
                        cb.checked = true;
                        cb.dispatchEvent(new Event('change', {bubbles: true}));
                        const parent = cb.closest('[class*=icheckbox]');
                        if (parent) { parent.classList.add('checked'); parent.click(); }
                        return { method: 'manual_check', cbId: cb.id, checked: cb.checked, rowText: txt.substring(0,80) };
                    }
                }
                return null;
            }""")
            if icheck_result:
                cav_clicked = True
                print(f"[cav_worker] ✅ CAV checked: {json.dumps(icheck_result)}", flush=True)
        except Exception as e:
            print(f"[cav_worker] iCheck API failed: {e}", flush=True)

        # Fallback: Playwright force-click
        if not cav_clicked:
            for sel in ["input[name='nameCert_4_4_1']", "#checkCert_4_4_1_false", "td:has-text('anotaciones Vigentes')"]:
                try:
                    page.locator(sel).first.click(force=True)
                    cav_clicked = True
                    print(f"[cav_worker] ✅ Playwright force-click: {sel}", flush=True)
                    break
                except Exception:
                    pass

        page.wait_for_timeout(3000)
        _snap(page, "7. Después de clickear CAV")

        if not cav_clicked:
            _snap(page, "❌ No se encontró 'Certificado Vehículos de anotaciones Vigentes'")
            result = {"ok": False, "error": "Could not find/click CAV certificate option"}
            if debug:
                result["steps"] = steps
            return result

        # ── Step 5: Enter the plate number ──
        print(f"[cav_worker] Step 5: Entering plate {plate}...", flush=True)
        page.wait_for_timeout(2000)

        plate_input = None

        # Strategy 1: Find the PPU input by known ID pattern or placeholder
        # After checking the CAV checkbox, a plate input appears:
        #   <input id="idInputPPU_4_4_1" placeholder="Ej: LLNNNN, LLLLNN o LLLNNN">
        for selector in [
            'input[id^="idInputPPU_"]',
            'input[placeholder*="LLNNNN"]',
            'input[placeholder*="patente" i]',
            'input[name*="ppu" i]',
            'input[name*="patente" i]',
            'input[id*="ppu" i]',
            'input[id*="patente" i]',
        ]:
            try:
                el = page.locator(selector).first
                if el.count() > 0 and el.is_visible():
                    plate_input = el
                    print(f"[cav_worker] ✅ Found plate input: {selector}", flush=True)
                    break
            except Exception:
                continue

        # Strategy 2: JS search for visible text input that's not login/captcha
        if not plate_input:
            try:
                input_id = page.evaluate("""() => {
                    const inputs = document.querySelectorAll('input[type=text], input:not([type])');
                    for (const inp of inputs) {
                        if (inp.offsetParent === null) continue;
                        const n = (inp.name || '').toLowerCase();
                        const id = (inp.id || '').toLowerCase();
                        const ph = (inp.placeholder || '').toLowerCase();
                        if (n === 'run' || n === 'pass' || n.includes('captcha') || n.includes('codigo')) continue;
                        if (id.includes('run') || id.includes('captcha')) continue;
                        if (id.includes('ppu') || id.includes('patente') || ph.includes('llnnnn') || ph.includes('patente')) {
                            inp.scrollIntoView({block: 'center'});
                            return inp.id || '_plate_input';
                        }
                    }
                    return null;
                }""")
                if input_id:
                    plate_input = page.locator(f"#{input_id}").first
                    print(f"[cav_worker] ✅ JS found plate input: #{input_id}", flush=True)
            except Exception as e:
                print(f"[cav_worker] JS plate search failed: {e}", flush=True)

        if not plate_input:
            inputs_dump = page.evaluate("""() => Array.from(document.querySelectorAll('input')).filter(
                inp => inp.offsetParent !== null && inp.type !== 'hidden'
            ).map(inp => ({ type: inp.type, name: inp.name, id: inp.id, placeholder: inp.placeholder || '' }))""")
            print(f"[cav_worker] Visible inputs: {json.dumps(inputs_dump)}", flush=True)
            _snap(page, "❌ No se encontró campo para ingresar patente")
            result = {"ok": False, "error": "Could not find plate input field", "visible_inputs": inputs_dump}
            if debug:
                result["steps"] = steps
            return result

        plate_input.click()
        plate_input.fill("")
        plate_input.type(plate, delay=50)
        _snap(page, f"8. Patente escrita: {plate}")

        # ── Step 6: Click per-row "Agregar al Carro" button ──
        # The button ID contains '#' chars (e.g. btn_agregarCarro_1#4_4_1#1)
        # so standard CSS selectors won't work. Must use JS to find & click it.
        # Clicking triggers an iframe modal (divAgregarACarro) that loads
        # agregarACarro.srcei?run=''&idCertificado=...&ppu=...&filtro=1
        # The server processes the request inside the iframe and adds to cart.
        print("[cav_worker] Step 6: Clicking 'Agregar al Carro' (per-row button)...", flush=True)
        page.wait_for_timeout(1000)

        agregar_result = page.evaluate("""() => {
            const btns = document.querySelectorAll('button.btn_agregarCarro');
            for (const btn of btns) {
                if (btn.offsetParent === null) continue;
                const txt = (btn.innerText || '').trim();
                if (txt.includes('Agregar al Carro')) {
                    btn.scrollIntoView({block: 'center'});
                    btn.click();
                    return { clicked: true, id: btn.id, text: txt };
                }
            }
            return { clicked: false };
        }""")
        print(f"[cav_worker] Agregar result: {json.dumps(agregar_result)}", flush=True)

        if not agregar_result.get("clicked"):
            _snap(page, "❌ No se encontró botón 'Agregar al Carro'")
            result = {"ok": False, "error": "Could not find per-row 'Agregar al Carro' button"}
            if debug:
                result["steps"] = steps
            return result

        # Wait for the iframe (agregarACarro.srcei) to load and process
        print("[cav_worker] Waiting for iframe to process...", flush=True)
        page.wait_for_timeout(8000)
        _snap(page, "9. Después de click Agregar al Carro (iframe procesando)")

        # ── Step 7: Check iframe result and cart state ──
        print("[cav_worker] Step 7: Checking iframe result & cart...", flush=True)

        # Read the iframe response
        iframe_text = ""
        for f in page.frames:
            if 'agregarACarro' in f.url or (f.name == 'cu_idIframe4'):
                try:
                    iframe_text = f.evaluate(
                        "() => document.body ? document.body.innerText : ''"
                    )
                    print(f"[cav_worker] Iframe text: {iframe_text[:200]}", flush=True)
                except Exception as e:
                    print(f"[cav_worker] Iframe read error: {e}", flush=True)
                break

        # Check if rate-limited
        if "excedido" in iframe_text.lower():
            _snap(page, "⚠️ Excedido límite de certificados")
            result = {
                "ok": False,
                "error": "Ha excedido el número de certificados permitidos (rate limit)",
                "iframe_message": iframe_text.strip(),
            }
            if debug:
                result["steps"] = steps
            return result

        # Close the modal if it's still open (click the X or outside)
        page.evaluate("""() => {
            const closeBtn = document.querySelector('#divAgregarACarro .close-reveal-modal');
            if (closeBtn) closeBtn.click();
            // Also try jQuery reveal close
            if (typeof jQuery !== 'undefined') {
                try { jQuery('#divAgregarACarro').trigger('reveal:close'); } catch(e) {}
            }
        }""")
        page.wait_for_timeout(2000)

        # Check cart state
        cart_state = page.evaluate("""() => ({
            total: document.getElementById('carro_valor_total')
                   ? document.getElementById('carro_valor_total').innerText : '0',
            certCount: document.querySelectorAll('#carro_tablasListaCertificados tr').length,
            emptyVisible: document.getElementById('carro_textoVacio')
                          ? window.getComputedStyle(document.getElementById('carro_textoVacio')).display !== 'none'
                          : false,
            emailVisible: document.getElementById('carro_solicitanteInputEmail')
                          ? document.getElementById('carro_solicitanteInputEmail').offsetParent !== null
                          : false,
        })""")
        print(f"[cav_worker] Cart state: {json.dumps(cart_state)}", flush=True)

        if cart_state.get("certCount", 0) == 0 and cart_state.get("total", "0") == "0":
            _snap(page, "⚠️ Carro vacío después de Agregar")
            # The iframe might have processed but we need to check for errors
            result = {
                "ok": False,
                "error": "Cart is empty after clicking Agregar al Carro",
                "iframe_message": iframe_text.strip() if iframe_text else "No iframe response",
                "cart_state": cart_state,
            }
            if debug:
                result["steps"] = steps
            return result

        _snap(page, f"10. Certificado agregado al carro (total: ${cart_state.get('total', '?')})")

        # ── Step 8: Fill solicitor email ──
        EMAIL = email  # passed in as parameter (default: felipe@autodirecto.cl)
        print(f"[cav_worker] Step 8: Filling email {EMAIL}...", flush=True)

        # The email container may need to be visible first
        page.evaluate("""() => {
            const c = document.getElementById('carro_datosMailSolicitanteContainer');
            if (c) c.style.display = 'block';
        }""")
        page.wait_for_timeout(500)

        # Fill email + confirmation
        for field_id in ['carro_solicitanteInputEmail', 'carro_solicitanteInputEmailConfirm']:
            try:
                el = page.locator(f"#{field_id}")
                if el.count() > 0:
                    el.click()
                    el.fill("")
                    el.type(EMAIL, delay=30)
                    print(f"[cav_worker] ✅ Filled #{field_id}", flush=True)
            except Exception as e:
                # Fallback: JS fill
                page.evaluate(f"""() => {{
                    const el = document.getElementById('{field_id}');
                    if (el) {{ el.value = '{EMAIL}'; el.dispatchEvent(new Event('change', {{bubbles:true}})); }}
                }}""")
                print(f"[cav_worker] JS filled #{field_id}", flush=True)

        _snap(page, f"11. Email ingresado: {EMAIL}")

        # ── Step 9: Click "Continuar" ──
        print("[cav_worker] Step 9: Clicking Continuar...", flush=True)
        page.evaluate("""() => {
            const btn = document.getElementById('carro_btnContinuar');
            if (btn) btn.click();
        }""")
        page.wait_for_timeout(5000)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        _snap(page, "12. Después de Continuar")

        # ── Step 10: Read result page (payment or CAV data) ──
        print("[cav_worker] Step 10: Reading result page...", flush=True)
        result_html = page.content()
        _snap(page, "13. Página de resultado final")

        html_lower = result_html.lower()

        # Check if we're on the payment page
        is_payment = ("webpay" in html_lower or "tarjeta" in html_lower
                      or "pagar" in html_lower or "transbank" in html_lower
                      or "entregadocumentos" in page.url.lower())

        if is_payment:
            print("[cav_worker] Hit the payment page — CAV requires payment", flush=True)
            _snap(page, "💰 Página de pago — CAV es un certificado pagado")

            text = re.sub(r'<[^>]+>', ' ', result_html)
            text = re.sub(r'\s+', ' ', text).strip()

            price_match = re.search(r'Total\s*\$?\s*([\d.,]+)', text)
            price = price_match.group(1) if price_match else cart_state.get("total", "desconocido")

            result = {
                "ok": True,
                "status": "payment_required",
                "plate": plate,
                "message": f"El Certificado de Anotaciones Vigentes (CAV) para {plate} requiere pago de ${price} en registrocivil.cl.",
                "price": price,
                "certificate_name": "Certificado Vehículos de anotaciones Vigentes",
            }
            if debug:
                result["steps"] = steps
            return result

        # Check if we got actual CAV data (after payment completed)
        if _has_cav_data(result_html):
            print("[cav_worker] Found CAV data on page!", flush=True)
            _snap(page, "✅ Datos CAV encontrados")
            result = _parse_cav_page(result_html, plate)
            if debug:
                result["steps"] = steps
            return result

        # Still on cart page — return cart info
        _snap(page, "⚠️ Estado final del flujo")
        result = {
            "ok": True,
            "status": "payment_required",
            "plate": plate,
            "price": cart_state.get("total", "desconocido"),
            "message": f"CAV para {plate} agregado al carro. Requiere pago de ${cart_state.get('total', '?')} en registrocivil.cl.",
            "certificate_name": "Certificado Vehículos de anotaciones Vigentes",
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

    email = (data.get("email") or "felipe@autodirecto.cl").strip()

    print(f"\n{'='*60}", flush=True)
    print(f"[cav_worker] Fetching CAV for plate: {plate} / email: {email}", flush=True)
    print(f"{'='*60}", flush=True)

    start_time = time.time()
    try:
        result = _fetch_cav(plate, email=email)
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[cav_worker] ❌ CRASH: {e}\n{tb}", flush=True)
        result = {"ok": False, "error": f"Internal error: {str(e)}", "traceback": tb}
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

    email = (data.get("email") or "felipe@autodirecto.cl").strip()

    print(f"\n{'='*60}", flush=True)
    print(f"[cav_worker] 🔍 DEBUG — Fetching CAV for plate: {plate} / email: {email}", flush=True)
    print(f"{'='*60}", flush=True)

    start_time = time.time()
    try:
        result = _fetch_cav(plate, email=email, debug=True)
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[cav_worker] ❌ DEBUG CRASH: {e}\n{tb}", flush=True)
        result = {"ok": False, "error": f"Internal error: {str(e)}", "traceback": tb}
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
    email = (request.args.get("email") or "felipe@autodirecto.cl").strip()

    if secret != CAV_SECRET:
        def error_stream():
            yield f"data: {json.dumps({'error': 'Invalid secret', 'done': True})}\n\n"
        return Response(error_stream(), mimetype="text/event-stream")

    if not plate or len(plate) < 4:
        def error_stream():
            yield f"data: {json.dumps({'error': 'Invalid plate', 'done': True})}\n\n"
        return Response(error_stream(), mimetype="text/event-stream")

    print(f"\n{'='*60}", flush=True)
    print(f"[cav_worker] 📡 STREAM — Fetching CAV for plate: {plate} / email: {email}", flush=True)
    print(f"{'='*60}", flush=True)

    def generate():
        import queue, threading

        q = queue.Queue()
        start_time = time.time()

        def on_step(step_data):
            q.put(("step", step_data))

        def worker():
            try:
                result = _fetch_cav(plate, email=email, debug=False, on_step=on_step)
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
