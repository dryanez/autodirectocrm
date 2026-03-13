"""
E2E test v7: Handle the "Agregar al Carro" modal iframe.
After clicking Agregar, a modal appears with iframe src=agregarACarro.srcei
We need to interact with the iframe content.
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

    # ── Load → Expand Vehículos → Check CAV → Enter plate ──
    print("═══ Steps 1-5: Load → Vehículos → CAV → Plate ═══")
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
    time.sleep(2)
    
    plate_el = page.locator('input[id^="idInputPPU_"]').first
    plate_el.click()
    plate_el.fill("")
    plate_el.type(PLATE, delay=50)
    time.sleep(1)
    print("✅ Steps 1-5 complete")

    # ── Step 6: Click "Agregar al Carro" ──
    print("\n═══ Step 6: Click 'Agregar al Carro' ═══")
    
    # JS click (we know this triggers the modal)
    page.evaluate("""() => {
        const btns = document.querySelectorAll('button.btn_agregarCarro');
        for (const btn of btns) {
            if (btn.offsetParent === null) continue;
            const txt = (btn.innerText || '').trim();
            if (txt.includes('Agregar')) {
                btn.click();
                return;
            }
        }
    }""")
    
    time.sleep(3)
    page.screenshot(path="e2e7_modal.png")
    print("📸 e2e7_modal.png")
    
    # ── Step 7: Inspect the modal iframe ──
    print("\n═══ Step 7: Inspect modal iframe ═══")
    
    # Check the modal state
    modal_state = page.evaluate("""() => {
        const modal = document.getElementById('divAgregarACarro');
        const iframe = document.getElementById('cu_idIframe4');
        return {
            modalExists: !!modal,
            modalVisible: modal ? modal.offsetParent !== null : false,
            modalDisplay: modal ? window.getComputedStyle(modal).display : 'not found',
            modalVisibility: modal ? window.getComputedStyle(modal).visibility : 'not found',
            modalZIndex: modal ? window.getComputedStyle(modal).zIndex : 'not found',
            iframeExists: !!iframe,
            iframeSrc: iframe ? iframe.src : '',
            iframeWidth: iframe ? iframe.offsetWidth : 0,
            iframeHeight: iframe ? iframe.offsetHeight : 0,
        };
    }""")
    print(f"Modal state: {json.dumps(modal_state, indent=2)}")
    
    # Get the iframe's content
    iframe_el = page.frame(name="") or page.frame(url="*agregarACarro*")
    if not iframe_el:
        # Try finding frame by ID
        frames = page.frames
        print(f"All frames ({len(frames)}):")
        for f in frames:
            print(f"  name='{f.name}', url='{f.url}'")
            if 'agregarACarro' in f.url or 'agregar' in f.url.lower():
                iframe_el = f
                break
    
    if iframe_el:
        print(f"\n✅ Found iframe: url={iframe_el.url}")
        
        # Wait for iframe content to load
        time.sleep(3)
        
        # Get iframe content
        try:
            iframe_content = iframe_el.evaluate("""() => ({
                url: location.href,
                title: document.title,
                bodyText: document.body.innerText.substring(0, 1500),
                bodyHTML: document.body.innerHTML.substring(0, 2000),
                inputs: Array.from(document.querySelectorAll('input, select, textarea')).map(el => ({
                    tag: el.tagName, type: el.type, id: el.id, name: el.name,
                    placeholder: el.placeholder || '', value: el.value,
                    visible: el.offsetParent !== null,
                })),
                buttons: Array.from(document.querySelectorAll('button, input[type=submit], a.btn, .btn')).map(el => ({
                    tag: el.tagName, id: el.id, text: (el.innerText || el.value || '').trim().substring(0,50),
                    cls: el.className.substring(0, 50),
                    visible: el.offsetParent !== null,
                    href: el.href || '',
                })),
                links: Array.from(document.querySelectorAll('a')).map(a => ({
                    href: a.href, text: (a.innerText || '').trim().substring(0, 50),
                })),
            })""")
            print(f"\nIframe content:")
            print(f"  URL: {iframe_content['url']}")
            print(f"  Body text:\n{iframe_content['bodyText'][:800]}")
            print(f"\n  Inputs: {json.dumps(iframe_content['inputs'], indent=4)}")
            print(f"\n  Buttons: {json.dumps(iframe_content['buttons'], indent=4)}")
            print(f"\n  Links: {json.dumps(iframe_content['links'][:5], indent=4)}")
        except Exception as e:
            print(f"Error reading iframe: {e}")
            # Try screenshot of full page to see the modal
            page.screenshot(path="e2e7_modal_detail.png", full_page=True)
            print("📸 e2e7_modal_detail.png")
    else:
        print("❌ Could not find iframe!")
        # Dump all frames
        for f in page.frames:
            print(f"  Frame: name='{f.name}' url='{f.url}'")
    
    browser.close()
    print("\n✅ E2E v7 complete!")
