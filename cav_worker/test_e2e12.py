"""
E2E test v12: Complete flow - properly handle the iframe modal.
The agregarACarro.srcei iframe is server-side. We need to:
1. Wait for iframe to load
2. Read its content  
3. Interact with any buttons/forms inside
4. After iframe interaction, cart should be populated
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

    # ── Step 1: Load ──
    print("═══ Step 1: Load page ═══")
    page.goto(URL, timeout=60000, wait_until="domcontentloaded")
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except: pass
    time.sleep(5)

    # ── Step 2: Expand Vehículos ──
    print("═══ Step 2: Expand Vehículos ═══")
    page.locator("#title_5").scroll_into_view_if_needed()
    page.locator("#title_5").click()
    time.sleep(2)

    # ── Step 3: Check CAV checkbox ──
    print("═══ Step 3: Check CAV checkbox ═══")
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

    # ── Step 4: Enter plate ──
    print("═══ Step 4: Enter plate ═══")
    plate_el = page.locator('input[id^="idInputPPU_"]').first
    plate_el.click()
    plate_el.fill("")
    plate_el.type(PLATE, delay=50)
    time.sleep(1)

    # ── Step 5: Click per-row "Agregar al Carro" via JS ──
    print("═══ Step 5: Click Agregar al Carro ═══")
    
    # Listen for frame navigations
    page.evaluate("""() => {
        const btns = document.querySelectorAll('button.btn_agregarCarro');
        for (const btn of btns) {
            if (btn.offsetParent === null) continue;
            const txt = (btn.innerText || '').trim();
            if (txt.includes('Agregar al Carro')) {
                btn.click();
                return btn.id;
            }
        }
        return 'no button found';
    }""")
    
    # Wait for the iframe to load
    print("Waiting for iframe to load...")
    time.sleep(5)
    
    # ── Step 6: Find and interact with iframe ──
    print("\n═══ Step 6: Inspect iframe ═══")
    
    iframe_frame = None
    for f in page.frames:
        if 'agregarACarro' in f.url:
            iframe_frame = f
            break
    
    if not iframe_frame:
        print("❌ No iframe found!")
        for f in page.frames:
            print(f"  Frame: url={f.url}")
        browser.close()
        exit(1)
    
    print(f"✅ Found iframe: {iframe_frame.url}")
    
    # Wait for iframe content to be ready
    try:
        iframe_frame.wait_for_load_state("domcontentloaded", timeout=10000)
    except:
        pass
    time.sleep(2)
    
    # Read iframe content
    try:
        iframe_content = iframe_frame.evaluate("""() => {
            return {
                url: location.href,
                title: document.title,
                readyState: document.readyState,
                bodyText: document.body ? document.body.innerText.substring(0, 3000) : 'no body',
                bodyHTML: document.body ? document.body.innerHTML.substring(0, 5000) : 'no body',
                forms: Array.from(document.querySelectorAll('form')).map(f => ({
                    id: f.id, action: f.action, method: f.method,
                    inputs: Array.from(f.querySelectorAll('input,select,button')).map(el => ({
                        tag: el.tagName, type: el.type, id: el.id, name: el.name,
                        value: (el.value || '').substring(0, 100),
                        text: (el.innerText || '').substring(0, 50),
                        visible: el.offsetParent !== null,
                    }))
                })),
                allButtons: Array.from(document.querySelectorAll('button, input[type=submit], a.btn, .btn')).map(el => ({
                    tag: el.tagName, id: el.id,
                    text: (el.innerText || el.value || '').trim().substring(0, 50),
                    visible: el.offsetParent !== null,
                })),
                allInputs: Array.from(document.querySelectorAll('input, select, textarea')).map(el => ({
                    tag: el.tagName, type: el.type, id: el.id, name: el.name,
                    value: (el.value || '').substring(0, 100),
                    placeholder: el.placeholder || '',
                    visible: el.offsetParent !== null,
                })),
                allLinks: Array.from(document.querySelectorAll('a')).map(a => ({
                    href: a.href, text: (a.innerText || '').trim().substring(0, 50),
                })),
            };
        }""")
        
        print(f"\nURL: {iframe_content['url']}")
        print(f"Ready: {iframe_content['readyState']}")
        print(f"\nBody text:\n{iframe_content['bodyText'][:1000]}")
        print(f"\nForms: {json.dumps(iframe_content['forms'], indent=2)}")
        print(f"\nButtons: {json.dumps(iframe_content['allButtons'], indent=2)}")
        print(f"\nInputs: {json.dumps(iframe_content['allInputs'], indent=2)}")
        print(f"\nLinks: {json.dumps(iframe_content['allLinks'][:10], indent=2)}")
        print(f"\nBody HTML (first 2000):\n{iframe_content['bodyHTML'][:2000]}")
        
    except Exception as e:
        print(f"Error reading iframe: {e}")
    
    page.screenshot(path="e2e12_iframe.png")
    print("📸 e2e12_iframe.png")
    
    browser.close()
    print("\n✅ Done!")
