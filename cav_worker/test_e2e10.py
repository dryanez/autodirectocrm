"""
E2E test v10: Fetch the OI.js and ajaxQPostCert.js to understand the click handler.
Also intercept network requests when clicking "Agregar al Carro".
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

    print("═══ Loading page ═══")
    page.goto(URL, timeout=60000, wait_until="domcontentloaded")
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except: pass
    time.sleep(5)

    # ── Get the source of OI.js ──
    print("\n═══ Search for btn_agregarCarro handler in all loaded JS ═══")
    handler_src = page.evaluate(r"""() => {
        // Look in the jQuery event namespace for click on .btn_agregarCarro
        if (typeof jQuery === 'undefined') return 'no jQuery';
        
        // Check if there's a click handler on the button element itself
        const btn = document.querySelector('.btn_agregarCarro');
        if (!btn) return 'no button found';
        
        const directEvents = jQuery._data(btn, 'events');
        if (directEvents) {
            const handlers = {};
            for (const [type, arr] of Object.entries(directEvents)) {
                handlers[type] = arr.map(h => h.handler.toString().substring(0, 500));
            }
            return { directOnButton: handlers };
        }
        
        // No direct events on button, check parents for delegation
        let el = btn.parentElement;
        const delegated = [];
        while (el) {
            const events = jQuery._data(el, 'events');
            if (events && events.click) {
                for (const h of events.click) {
                    if (h.selector && h.selector.includes('btn_agregarCarro')) {
                        delegated.push({
                            parentTag: el.tagName,
                            parentId: el.id,
                            selector: h.selector,
                            handler: h.handler.toString().substring(0, 800),
                        });
                    }
                }
            }
            el = el.parentElement;
        }
        
        return { delegatedHandlers: delegated, note: 'searched up DOM tree' };
    }""")
    print(json.dumps(handler_src, indent=2))

    # ── Try a different approach: override the click to log ──
    print("\n═══ Expanding and setting up ═══")
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
    
    # ── Monitor network requests when clicking Agregar ──
    print("\n═══ Clicking Agregar al Carro with network monitor ═══")
    
    requests_log = []
    def on_request(request):
        if 'registrocivil' in request.url:
            requests_log.append({
                'method': request.method,
                'url': request.url,
                'postData': (request.post_data or '')[:500]
            })
    
    page.on("request", on_request)
    
    # Click the button
    page.evaluate("""() => {
        const btns = document.querySelectorAll('button.btn_agregarCarro');
        for (const btn of btns) {
            if (btn.offsetParent === null) continue;
            const txt = (btn.innerText || '').trim();
            if (txt.includes('Agregar')) {
                btn.click();
                return btn.id;
            }
        }
    }""")
    
    time.sleep(5)
    
    print(f"\nNetwork requests after click ({len(requests_log)}):")
    for r in requests_log:
        print(f"  {r['method']} {r['url'][:120]}")
        if r['postData']:
            print(f"    POST: {r['postData'][:200]}")
    
    # ── Check iframe content ──
    print("\n═══ Check iframe content ═══")
    for f in page.frames:
        if 'agregarACarro' in f.url:
            print(f"Found iframe: {f.url}")
            try:
                content = f.evaluate("""() => ({
                    text: document.body ? document.body.innerText : 'no body',
                    html: document.body ? document.body.innerHTML.substring(0, 3000) : 'no body',
                })""")
                print(f"Body text: {content['text'][:500]}")
                print(f"\nBody HTML:\n{content['html'][:2000]}")
            except Exception as e:
                print(f"Error: {e}")

    # ── Also check: what's the max cert limit? ──
    print("\n═══ Check divMensajeErrorMaxCertificados ═══")
    max_cert_info = page.evaluate("""() => {
        const div = document.getElementById('divMensajeErrorMaxCertificados');
        if (!div) return 'not found';
        return {
            display: window.getComputedStyle(div).display,
            text: div.innerText,
            html: div.innerHTML.substring(0, 500),
        };
    }""")
    print(json.dumps(max_cert_info, indent=2))

    page.screenshot(path="e2e10_final.png")
    browser.close()
    print("\n✅ Done!")
