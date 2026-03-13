"""
E2E test v4: Full flow with iCheck API + correct selectors.
Tests the entire flow: load → Vehículos → CAV → plate → Agregar
"""
import json, time
from playwright.sync_api import sync_playwright

URL = "https://www.registrocivil.cl/OficinaInternet/"
PLATE = "GKZR72"

with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
    )
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 900}, locale="es-CL",
    )
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['es-CL', 'es', 'en'] });
        window.chrome = { runtime: {} };
    """)
    page = context.new_page()

    # Step 1: Load
    print(f"═══ Step 1: Loading page ═══")
    page.goto(URL, timeout=60000, wait_until="domcontentloaded")
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except: pass
    time.sleep(5)
    print(f"URL: {page.url}")
    page.screenshot(path="e2e4_1_loaded.png")

    # Step 2: Check CAPTCHA
    if "código de la imagen" in page.content().lower():
        print("❌ CAPTCHA detected — aborting")
        browser.close()
        exit(1)
    print("✅ No CAPTCHA")

    # Step 3: Expand Vehículos
    print(f"\n═══ Step 3: Expand Vehículos ═══")
    page.locator("#title_5").scroll_into_view_if_needed()
    page.locator("#title_5").click()
    time.sleep(2)
    expanded = page.evaluate("() => { const d = document.getElementById('divLista_5'); return d && window.getComputedStyle(d).display !== 'none'; }")
    print(f"Expanded: {expanded}")
    if not expanded:
        print("❌ Failed")
        browser.close()
        exit(1)
    page.screenshot(path="e2e4_3_vehiculos.png")

    # Step 4: Check CAV checkbox via iCheck
    print(f"\n═══ Step 4: Check CAV (iCheck API) ═══")
    result = page.evaluate("""() => {
        const rows = document.querySelectorAll('tr');
        for (const row of rows) {
            const txt = (row.innerText || '').toLowerCase();
            if (txt.includes('anotaciones vigentes') && !txt.includes('multas')) {
                const cb = row.querySelector('input[type=checkbox]');
                if (!cb) continue;
                cb.scrollIntoView({block: 'center'});
                if (typeof jQuery !== 'undefined' && typeof jQuery.fn.iCheck !== 'undefined') {
                    jQuery(cb).iCheck('check');
                    return { method: 'iCheck', cbId: cb.id, checked: cb.checked };
                }
                return { error: 'no iCheck' };
            }
        }
        return { error: 'no CAV row' };
    }""")
    print(f"Result: {json.dumps(result)}")
    time.sleep(3)
    page.screenshot(path="e2e4_4_cav.png")

    # Step 5: Enter plate
    print(f"\n═══ Step 5: Enter plate {PLATE} ═══")
    plate_el = None
    for sel in ['input[id^="idInputPPU_"]', 'input[placeholder*="LLNNNN"]']:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible():
                plate_el = el
                print(f"Found: {sel}")
                break
        except:
            continue

    if not plate_el:
        print("❌ No plate input!")
        # Dump visible inputs
        vis = page.evaluate("""() => Array.from(document.querySelectorAll('input')).filter(
            i => i.offsetParent !== null && i.type !== 'hidden'
        ).map(i => ({ id: i.id, name: i.name, type: i.type, ph: i.placeholder }))""")
        print(json.dumps(vis, indent=2))
        browser.close()
        exit(1)

    plate_el.click()
    plate_el.fill("")
    plate_el.type(PLATE, delay=50)
    time.sleep(1)
    print(f"✅ Typed: {PLATE}")
    page.screenshot(path="e2e4_5_plate.png")

    # Step 6: Click Agregar/Continuar
    print(f"\n═══ Step 6: Click Agregar/Continuar ═══")
    
    # First: check for per-row "Agregar" button
    # Then: fall back to #carro_btnContinuar
    clicked = False
    
    # Try various buttons
    for sel in ['#carro_btnContinuar', '.btn_agregarCarro', 'button:has-text("Continuar")', 'button:has-text("Agregar")']:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible():
                el.click()
                clicked = True
                print(f"✅ Clicked: {sel}")
                break
        except:
            continue
    
    if not clicked:
        # JS fallback
        js_result = page.evaluate("""() => {
            const btns = document.querySelectorAll('button, input[type=submit], .btn_agregarCarro');
            for (const btn of btns) {
                if (btn.offsetParent === null) continue;
                const txt = (btn.innerText || btn.value || '').toLowerCase();
                if (/agregar|continuar/.test(txt)) {
                    btn.scrollIntoView({block: 'center'});
                    btn.click();
                    return { clicked: true, text: txt.substring(0,30), id: btn.id };
                }
            }
            return { clicked: false };
        }""")
        print(f"JS result: {json.dumps(js_result)}")
        clicked = js_result.get("clicked", False)

    time.sleep(5)
    try: page.wait_for_load_state("networkidle", timeout=10000)
    except: pass
    page.screenshot(path="e2e4_6_submitted.png")
    print("📸 e2e4_6_submitted.png")

    # Step 7: Read result
    print(f"\n═══ Step 7: Result ═══")
    print(f"URL: {page.url}")
    body_text = page.evaluate("() => document.body.innerText.substring(0, 1500)")
    print(f"Body:\n{body_text[:800]}")
    
    html = page.content().lower()
    if "propietario" in html or "inscripci" in html or "anotacion" in html:
        print("\n🎉 CAV DATA FOUND!")
    elif "carro" in html and ("total" in html or "vacío" in html):
        print("\n🛒 Cart page — checking cart contents...")
        cart = page.evaluate("() => document.getElementById('carro_divTable') ? document.getElementById('carro_divTable').innerText : 'not found'")
        print(f"Cart: {cart[:300]}")
    elif "pagar" in html or "webpay" in html:
        print("\n💰 Payment page!")
    elif "error" in html:
        print("\n⚠️ Error page")
    else:
        print("\n❓ Unknown state")
    
    page.screenshot(path="e2e4_7_result.png", full_page=True)
    print("📸 e2e4_7_result.png")

    browser.close()
    print("\n✅ Full E2E test complete!")
