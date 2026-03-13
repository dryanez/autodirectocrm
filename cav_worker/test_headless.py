"""
Test: Does the accordion work in HEADLESS mode?
This is the key difference between local (headed) and Railway (headless).
Run this to see if headless Chromium has the same click issue.
"""
import json
from playwright.sync_api import sync_playwright

URL = "https://www.registrocivil.cl/OficinaInternet/"

with sync_playwright() as p:
    # ── Test in HEADLESS mode (like Railway) ──
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--single-process",
        ],
    )
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

    print(f"Opening {URL} in HEADLESS mode...")
    try:
        resp = page.goto(URL, timeout=60000, wait_until="domcontentloaded")
        print(f"Response status: {resp.status if resp else 'None'}")
    except Exception as e:
        print(f"Navigation error: {e}")
        # Try taking a screenshot anyway
        try:
            page.screenshot(path="headless_error.png")
            print("📸 Error screenshot saved")
        except:
            pass
        browser.close()
        exit(1)
    
    import time
    time.sleep(3)  # Use sleep instead of wait_for_timeout

    # Check if we have a CAPTCHA
    has_captcha = "código de la imagen" in page.content().lower()
    print(f"Has CAPTCHA gate: {has_captcha}")

    if has_captcha:
        print("\n⚠️  CAPTCHA detected. This test needs to get past it first.")
        print("    In production, 2captcha solves it. For this test, we'll just")
        print("    check if the page structure is the same in headless mode.\n")
        
        # Take a screenshot for visual comparison
        page.screenshot(path="headless_captcha.png")
        print("📸 Screenshot saved: headless_captcha.png")
        
        # Check what elements exist on the CAPTCHA page
        page_info = page.evaluate("""() => {
            const allIds = Array.from(document.querySelectorAll('[id]')).map(e => e.id);
            return {
                url: location.href,
                title: document.title,
                idCount: allIds.length,
                ids: allIds.slice(0, 40),
                bodyText: document.body.innerText.substring(0, 300),
                hasTitle5: !!document.getElementById('title_5'),
                hasDivLista5: !!document.getElementById('divLista_5'),
                hasCertTable: !!document.getElementById('certificadosTable'),
                hasJQuery: typeof jQuery !== 'undefined',
            };
        }""")
        print(f"\nPage info (CAPTCHA page):")
        print(json.dumps(page_info, indent=2))
        
        # IMPORTANT: Even on the CAPTCHA page, the certificate list may be
        # partially loaded in the background. Check if title_5 exists.
        if page_info.get("hasTitle5"):
            print("\n🔍 title_5 EXISTS even on CAPTCHA page!")
            print("   Trying to click it...")
            
            # Try clicking
            el = page.locator("#title_5")
            if el.count() > 0:
                el.scroll_into_view_if_needed()
                el.click()
                page.wait_for_timeout(2000)
                
                div_state = page.evaluate("""() => {
                    const d = document.getElementById('divLista_5');
                    if (!d) return 'NOT FOUND';
                    return {
                        display: window.getComputedStyle(d).display,
                        childCount: d.children.length,
                        innerHTML: d.innerHTML.substring(0, 300),
                    };
                }""")
                print(f"   divLista_5 after headless click: {json.dumps(div_state, indent=2)}")
                page.screenshot(path="headless_after_click.png")
                print("📸 Screenshot saved: headless_after_click.png")
        else:
            print("\n❌ title_5 does NOT exist on CAPTCHA page")
            print("   The certificate list is only loaded AFTER solving CAPTCHA")
    else:
        print("✅ No CAPTCHA — directly inside the site!")
        
        # Now test the accordion click
        page_state = page.evaluate("""() => {
            const title5 = document.getElementById('title_5');
            const divLista5 = document.getElementById('divLista_5');
            return {
                hasTitle5: !!title5,
                title5Onclick: title5 ? (title5.getAttribute('onclick') || '') : '',
                hasDivLista5: !!divLista5,
                divLista5Display: divLista5 ? window.getComputedStyle(divLista5).display : null,
                divLista5Children: divLista5 ? divLista5.children.length : 0,
                jqueryLoaded: typeof jQuery !== 'undefined',
            };
        }""")
        print(f"\nBEFORE click: {json.dumps(page_state, indent=2)}")
        
        # Try Playwright click
        el = page.locator("#title_5")
        if el.count() > 0:
            el.scroll_into_view_if_needed()
            el.click()
            page.wait_for_timeout(2000)
            
            after_state = page.evaluate("""() => {
                const d = document.getElementById('divLista_5');
                if (!d) return { expanded: false, reason: 'not found' };
                return {
                    expanded: window.getComputedStyle(d).display !== 'none',
                    display: window.getComputedStyle(d).display,
                    childCount: d.children.length,
                    hasCertTable: !!document.getElementById('certificadosTable'),
                    certRows: document.getElementById('certificadosTable') 
                        ? document.getElementById('certificadosTable').querySelectorAll('tr').length : 0,
                };
            }""")
            print(f"\nAFTER headless click: {json.dumps(after_state, indent=2)}")
            page.screenshot(path="headless_after_accordion.png")
            print("📸 Screenshot saved: headless_after_accordion.png")
            
            if after_state.get("expanded"):
                print("\n🎉 HEADLESS CLICK WORKS! The accordion expands in headless mode.")
                
                # Check cert table content
                cert_content = page.evaluate("""() => {
                    const table = document.getElementById('certificadosTable');
                    if (!table) return 'no certificadosTable';
                    const rows = Array.from(table.querySelectorAll('tr'));
                    return rows.map(r => r.innerText.trim().substring(0, 100));
                }""")
                print(f"\nCertificate rows: {json.dumps(cert_content, indent=2)}")
            else:
                print("\n❌ HEADLESS CLICK FAILED! Accordion did NOT expand.")
                print("   This confirms the Railway issue.")
                
                # Try all the methods from the production code
                methods = [
                    ("Force click", lambda: page.locator("#title_5").click(force=True)),
                    ("JS dispatchEvent", lambda: page.evaluate("""() => {
                        const el = document.getElementById('title_5');
                        ['pointerdown','mousedown','pointerup','mouseup','click'].forEach(t => {
                            el.dispatchEvent(new PointerEvent(t, {bubbles:true, cancelable:true, view:window, button:0, buttons:1, pointerType:'mouse'}));
                        });
                    }""")),
                    ("jQuery trigger", lambda: page.evaluate("() => typeof jQuery !== 'undefined' && jQuery('#title_5').trigger('click')")),
                ]
                
                for name, fn in methods:
                    # Reset
                    page.evaluate("() => { const d = document.getElementById('divLista_5'); if(d) d.style.display='none'; }")
                    page.wait_for_timeout(500)
                    
                    try:
                        fn()
                        page.wait_for_timeout(2000)
                        state = page.evaluate("""() => {
                            const d = document.getElementById('divLista_5');
                            return d ? window.getComputedStyle(d).display : 'NOT FOUND';
                        }""")
                        print(f"  {name}: divLista_5 display = {state}")
                    except Exception as e:
                        print(f"  {name}: ERROR — {e}")

    browser.close()
    print("\n✅ Test complete.")
