"""
E2E test v13: The iframe went to loader.srcei after processing.
This means agregarACarro.srcei actually PROCESSED the request!
The cart might have been updated. Let's check.
Also check both iframe frames for content.
"""
import json, time
from playwright.sync_api import sync_playwright

URL = "https://www.registrocivil.cl/OficinaInternet/"
PLATE = "GKZR72"
EMAIL = "felipe@autodirecto.cl"

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

    # ── Load + expand + check + plate ──
    print("═══ Steps 1-4 ═══")
    page.goto(URL, timeout=60000, wait_until="domcontentloaded")
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except: pass
    time.sleep(5)
    
    page.locator("#title_5").scroll_into_view_if_needed()
    page.locator("#title_5").click()
    time.sleep(2)
    
    page.evaluate("""() => {
        const rows = document.querySelectorAll('tr');
        for (const row of rows) {
            const txt = (row.innerText || '').toLowerCase();
            if (txt.includes('anotaciones vigentes') && !txt.includes('multas')) {
                const cb = row.querySelector('input[type=checkbox]');
                if (cb && typeof jQuery !== 'undefined') jQuery(cb).iCheck('check');
            }
        }
    }""")
    time.sleep(1)
    
    plate_el = page.locator('input[id^="idInputPPU_"]').first
    plate_el.click()
    plate_el.fill("")
    plate_el.type(PLATE, delay=50)
    time.sleep(1)
    print("✅ Steps 1-4 done")

    # Check cart state BEFORE clicking Agregar
    print("\n═══ Cart state BEFORE Agregar ═══")
    cart_before = page.evaluate("""() => ({
        total: document.getElementById('carro_valor_total') ? document.getElementById('carro_valor_total').innerText : 'not found',
        empty: document.getElementById('carro_textoVacio') ? document.getElementById('carro_textoVacio').innerText : 'not found',
        hasCerts: document.getElementById('carro_textoHayCertificados') ? document.getElementById('carro_textoHayCertificados').style.display : 'not found',
        certRows: document.getElementById('carro_tablasListaCertificados') ? document.getElementById('carro_tablasListaCertificados').innerHTML.substring(0, 500) : 'not found',
    })""")
    print(json.dumps(cart_before, indent=2))

    # ── Click Agregar al Carro ──
    print("\n═══ Step 5: Click Agregar al Carro ═══")
    
    # Monitor what happens to the iframe
    frame_log = []
    def on_frame_navigated(frame):
        frame_log.append({'url': frame.url, 'name': frame.name})
    page.on("framenavigated", on_frame_navigated)
    
    btn_id = page.evaluate("""() => {
        const btns = document.querySelectorAll('button.btn_agregarCarro');
        for (const btn of btns) {
            if (btn.offsetParent === null) continue;
            const txt = (btn.innerText || '').trim();
            if (txt.includes('Agregar al Carro')) {
                btn.click();
                return btn.id;
            }
        }
        return 'none';
    }""")
    print(f"Clicked: {btn_id}")
    
    # Wait and monitor
    for i in range(10):
        time.sleep(1)
        print(f"  t+{i+1}s: frames navigated: {len(frame_log)}")
        for fl in frame_log:
            if fl not in frame_log[:len(frame_log)-1]:  # only new ones
                print(f"    → {fl['url'][:100]}")
    
    print(f"\nAll frame navigations:")
    for fl in frame_log:
        print(f"  {fl['name'] or '(main)'}: {fl['url'][:120]}")

    # ── Check cart state AFTER ──
    print("\n═══ Cart state AFTER Agregar ═══")
    cart_after = page.evaluate("""() => ({
        total: document.getElementById('carro_valor_total') ? document.getElementById('carro_valor_total').innerText : 'not found',
        emptyDisplay: document.getElementById('carro_textoVacio') ? document.getElementById('carro_textoVacio').style.display : 'not found',
        emptyText: document.getElementById('carro_textoVacio') ? document.getElementById('carro_textoVacio').innerText : 'not found',
        hasCertsDisplay: document.getElementById('carro_textoHayCertificados') ? document.getElementById('carro_textoHayCertificados').style.display : 'not found',
        certRowsCount: document.querySelectorAll('#carro_tablasListaCertificados tr').length,
        certRowsHTML: document.getElementById('carro_tablasListaCertificados') ? document.getElementById('carro_tablasListaCertificados').innerHTML.substring(0, 1000) : 'not found',
        emailVisible: document.getElementById('carro_solicitanteInputEmail') ? document.getElementById('carro_solicitanteInputEmail').offsetParent !== null : false,
        emailContainerDisplay: document.getElementById('carro_datosMailSolicitanteContainer') ? document.getElementById('carro_datosMailSolicitanteContainer').style.display : 'not found',
    })""")
    print(json.dumps(cart_after, indent=2))

    # Check the modal state
    print("\n═══ Modal state ═══")
    modal_state = page.evaluate("""() => ({
        divAgregarACarro: {
            display: document.getElementById('divAgregarACarro') ? window.getComputedStyle(document.getElementById('divAgregarACarro')).display : 'not found',
            visibility: document.getElementById('divAgregarACarro') ? window.getComputedStyle(document.getElementById('divAgregarACarro')).visibility : 'not found',
        },
        divMensajeErrorMaxCert: {
            display: document.getElementById('divMensajeErrorMaxCertificados') ? window.getComputedStyle(document.getElementById('divMensajeErrorMaxCertificados')).display : 'not found',
        },
    })""")
    print(json.dumps(modal_state, indent=2))

    # Read the two loader.srcei frames
    print("\n═══ Iframe contents ═══")
    for f in page.frames:
        if f.url != page.url:
            print(f"\n  Frame '{f.name}': {f.url}")
            try:
                text = f.evaluate("() => document.body ? document.body.innerText : 'no body'")
                print(f"    Text: {text[:300]}")
            except Exception as e:
                print(f"    Error: {e}")

    page.screenshot(path="e2e13_after_agregar.png")
    print("\n📸 e2e13_after_agregar.png")
    
    browser.close()
    print("\n✅ Done!")
