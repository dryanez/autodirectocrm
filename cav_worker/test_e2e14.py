"""
E2E test v14: Try with a different plate to see if rate limit is plate-specific.
Also try to close the modal and retry.
The iframe returns "Ha excedido" for GKZR72 - maybe too many requests for this plate.
Let's try BBBB11 (a test plate) or something else.
"""
import json, time
from playwright.sync_api import sync_playwright

URL = "https://www.registrocivil.cl/OficinaInternet/"
# Try a different plate
PLATE = "JXSP58"  # different test plate

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

    # ── Load → Expand → Check → Plate → Agregar ──
    print(f"═══ Full flow with plate: {PLATE} ═══")
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
    print("✅ Setup done, clicking Agregar...")

    # Click Agregar
    page.evaluate("""() => {
        const btns = document.querySelectorAll('button.btn_agregarCarro');
        for (const btn of btns) {
            if (btn.offsetParent === null) continue;
            const txt = (btn.innerText || '').trim();
            if (txt.includes('Agregar al Carro')) {
                btn.click();
                return;
            }
        }
    }""")
    
    # Wait for iframe to load
    print("Waiting for iframe...")
    time.sleep(8)
    
    # Check iframe content
    iframe_result = None
    for f in page.frames:
        if 'agregarACarro' in f.url:
            try:
                iframe_result = f.evaluate("""() => ({
                    url: location.href,
                    text: document.body ? document.body.innerText.substring(0, 2000) : '',
                    html: document.body ? document.body.innerHTML.substring(0, 3000) : '',
                    inputs: Array.from(document.querySelectorAll('input, select, textarea, button')).map(el => ({
                        tag: el.tagName, type: el.type, id: el.id, name: el.name,
                        text: (el.innerText || el.value || '').substring(0, 50),
                        visible: el.offsetParent !== null,
                    })),
                })""")
            except Exception as e:
                print(f"Error reading iframe: {e}")
    
    if iframe_result:
        print(f"\nIframe URL: {iframe_result['url']}")
        print(f"Iframe text: {iframe_result['text'][:500]}")
        print(f"Iframe inputs: {json.dumps(iframe_result['inputs'], indent=2)}")
        if 'excedido' in iframe_result['text'].lower():
            print("\n⚠️ RATE LIMITED! Server says 'Ha excedido el número de certificados permitidos.'")
        elif iframe_result['text'].strip() == '':
            print("\n⚠️ Empty iframe — might still be loading")
        else:
            print(f"\n✅ DIFFERENT RESPONSE! Full text:\n{iframe_result['text']}")
            print(f"\nFull HTML:\n{iframe_result['html'][:2000]}")
    else:
        # Check if iframe redirected to loader.srcei (meaning it processed)
        for f in page.frames:
            if f.url != page.url:
                print(f"Frame '{f.name}': {f.url}")
                try:
                    text = f.evaluate("() => document.body ? document.body.innerText : ''")
                    print(f"  Text: {text[:300]}")
                except:
                    pass

    # Check cart
    cart = page.evaluate("""() => ({
        total: document.getElementById('carro_valor_total') ? document.getElementById('carro_valor_total').innerText : '',
        certCount: document.querySelectorAll('#carro_tablasListaCertificados tr').length,
        certHTML: document.getElementById('carro_tablasListaCertificados') ? document.getElementById('carro_tablasListaCertificados').innerHTML.substring(0, 500) : '',
    })""")
    print(f"\nCart: total={cart['total']}, certs={cart['certCount']}")
    if cart['certCount'] > 0:
        print(f"Cart HTML: {cart['certHTML'][:400]}")
    
    page.screenshot(path="e2e14_different_plate.png")
    print("📸 e2e14_different_plate.png")
    
    browser.close()
    print("\n✅ Done!")
