"""
E2E test v2: Use Playwright click (not JS) for the checkbox.
The checkbox JS click may not trigger the site's event handler properly.
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

    print(f"═══ Loading page ═══")
    page.goto(URL, timeout=60000, wait_until="domcontentloaded")
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except: pass
    time.sleep(5)
    print(f"URL: {page.url}")

    # Step 3: Click Vehículos
    print(f"\n═══ Step 3: Click Vehículos ═══")
    page.locator("#title_5").scroll_into_view_if_needed()
    page.locator("#title_5").click()
    time.sleep(2)
    
    state = page.evaluate("""() => {
        const d = document.getElementById('divLista_5');
        return { expanded: d && window.getComputedStyle(d).display !== 'none' };
    }""")
    print(f"Expanded: {state}")
    
    if not state.get("expanded"):
        print("❌ Failed to expand")
        browser.close()
        exit(1)

    # Step 4: Click CAV checkbox using PLAYWRIGHT (not JS!)
    print(f"\n═══ Step 4: Click CAV checkbox (Playwright) ═══")
    
    # The checkbox ID is checkCert_4_4_1_false  
    # Let's try multiple ways
    
    # First, find the checkbox
    cb_info = page.evaluate("""() => {
        const rows = document.querySelectorAll('tr');
        for (const row of rows) {
            const txt = (row.innerText || '').toLowerCase();
            if (txt.includes('anotaciones vigentes') && !txt.includes('multas')) {
                const cb = row.querySelector('input[type=checkbox]');
                if (cb) return { id: cb.id, name: cb.name, checked: cb.checked };
            }
        }
        return null;
    }""")
    print(f"Checkbox info: {cb_info}")
    
    if cb_info and cb_info.get("id"):
        # Use Playwright click (this dispatches proper events)
        cb_locator = page.locator(f"#{cb_info['id']}")
        cb_locator.scroll_into_view_if_needed()
        
        # Check if checkbox is visible
        is_vis = cb_locator.is_visible()
        print(f"Checkbox visible: {is_vis}")
        
        if not is_vis:
            # Force click
            cb_locator.click(force=True)
            print("Force-clicked checkbox")
        else:
            cb_locator.click()
            print("Clicked checkbox")
        
        time.sleep(3)
        
        # Check if the plate input appeared
        after_state = page.evaluate("""() => {
            const hint = document.getElementById('idTextoEjemplPatente');
            const hasHint = !!hint;
            const hintVisible = hint ? hint.offsetParent !== null : false;
            
            // Look for any new text inputs that appeared
            const textInputs = Array.from(document.querySelectorAll('input[type=text], input:not([type])')).filter(
                inp => inp.offsetParent !== null
            ).map(inp => ({
                id: inp.id, name: inp.name, placeholder: inp.placeholder || '',
                maxLength: inp.maxLength,
                nearbyText: (() => {
                    let el = inp;
                    for (let i=0; i<3; i++) { el = el.parentElement; if (!el) break; }
                    return el ? el.innerText.trim().substring(0,80) : '';
                })()
            }));
            
            return { hasHint, hintVisible, textInputs };
        }""")
        print(f"After checkbox click: {json.dumps(after_state, indent=2)}")
        page.screenshot(path="e2e2_after_checkbox.png")
        print("📸 e2e2_after_checkbox.png")
        
        # If no hint/plate input appeared, try clicking the TD text instead
        if not after_state.get("hintVisible"):
            print("\n⚠️ Hint not visible. Trying to click the certificate TD text...")
            
            # Maybe we need to click the entire row, not just the checkbox
            td = page.locator("td:has-text('anotaciones Vigentes')").first
            td.scroll_into_view_if_needed()
            td.click()
            time.sleep(3)
            
            after2 = page.evaluate("""() => {
                const hint = document.getElementById('idTextoEjemplPatente');
                return {
                    hintVisible: hint ? hint.offsetParent !== null : false,
                    hintText: hint ? hint.innerText.trim() : 'NOT FOUND',
                    textInputs: Array.from(document.querySelectorAll('input[type=text]')).filter(
                        inp => inp.offsetParent !== null
                    ).map(inp => ({ id: inp.id, name: inp.name, placeholder: inp.placeholder || '' }))
                };
            }""")
            print(f"After TD click: {json.dumps(after2, indent=2)}")
            page.screenshot(path="e2e2_after_td_click.png")
            print("📸 e2e2_after_td_click.png")
        
        # Let's also check: does clicking the checkbox change any visible form?
        # Maybe the plate input appears inside the cert row itself
        row_content = page.evaluate("""() => {
            const rows = document.querySelectorAll('tr');
            for (const row of rows) {
                const txt = (row.innerText || '').toLowerCase();
                if (txt.includes('anotaciones vigentes')) {
                    return {
                        innerHTML: row.innerHTML.substring(0, 500),
                        allInputs: Array.from(row.querySelectorAll('input')).map(inp => ({
                            type: inp.type, id: inp.id, name: inp.name,
                            visible: inp.offsetParent !== null,
                            placeholder: inp.placeholder || ''
                        }))
                    };
                }
            }
            return null;
        }""")
        print(f"\nCAV row content: {json.dumps(row_content, indent=2)}")
        
        # Check entire page for any plate-related input
        all_plate_elements = page.evaluate("""() => {
            const all = document.querySelectorAll('input, select, textarea');
            return Array.from(all).filter(el => {
                const n = (el.name || '').toLowerCase();
                const id = (el.id || '').toLowerCase();
                const ph = (el.placeholder || '').toLowerCase();
                return n.includes('patente') || n.includes('ppu') || n.includes('placa') ||
                       id.includes('patente') || id.includes('ppu') || id.includes('placa') ||
                       ph.includes('llnnnn') || ph.includes('patente');
            }).map(el => ({
                tag: el.tagName, type: el.type, id: el.id, name: el.name,
                placeholder: el.placeholder || '', visible: el.offsetParent !== null,
                display: window.getComputedStyle(el).display,
            }));
        }""")
        print(f"\nPlate-related elements: {json.dumps(all_plate_elements, indent=2)}")
    
    browser.close()
    print("\n✅ Done")
