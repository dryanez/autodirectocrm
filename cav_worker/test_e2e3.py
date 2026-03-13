"""
E2E test v3: Handle iCheck plugin (iCheck-helper intercepts pointer events)
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

    # Step 3: Vehículos
    print(f"\n═══ Step 3: Click Vehículos ═══")
    page.locator("#title_5").scroll_into_view_if_needed()
    page.locator("#title_5").click()
    time.sleep(2)
    print("✅ Expanded")

    # Step 4: CAV checkbox — handle iCheck plugin
    print(f"\n═══ Step 4: Click CAV (iCheck) ═══")
    
    # The site uses iCheck jQuery plugin. The real checkbox is hidden behind
    # <ins class="iCheck-helper"> which intercepts pointer events.
    # Solution 1: Use jQuery/iCheck API to check the checkbox
    # Solution 2: Click the <ins> element instead
    # Solution 3: Use force=True to bypass the interception
    
    # Let's try force=True first (simplest)
    cb_info = page.evaluate("""() => {
        const rows = document.querySelectorAll('tr');
        for (const row of rows) {
            const txt = (row.innerText || '').toLowerCase();
            if (txt.includes('anotaciones vigentes') && !txt.includes('multas')) {
                const cb = row.querySelector('input[type=checkbox]');
                if (cb) {
                    // Check the iCheck structure
                    const parent = cb.parentElement;
                    const iCheckHelper = parent ? parent.querySelector('.iCheck-helper, ins') : null;
                    return {
                        cbId: cb.id,
                        cbName: cb.name,
                        cbChecked: cb.checked,
                        parentTag: parent ? parent.tagName : null,
                        parentClass: parent ? parent.className : null,
                        hasICheckHelper: !!iCheckHelper,
                        iCheckHelperTag: iCheckHelper ? iCheckHelper.tagName : null,
                    };
                }
            }
        }
        return null;
    }""")
    print(f"Checkbox info: {json.dumps(cb_info, indent=2)}")
    
    # Method 1: Use jQuery iCheck API
    icheck_result = page.evaluate("""() => {
        if (typeof jQuery === 'undefined') return { error: 'no jQuery' };
        try {
            // iCheck uses: $('input').iCheck('check')
            const cb = jQuery('#checkCert_4_4_1_false');
            if (cb.length === 0) return { error: 'checkbox not found via jQuery' };
            
            // Check if iCheck is initialized
            if (typeof cb.iCheck === 'function') {
                cb.iCheck('check');
                return { method: 'iCheck API', checked: cb.prop('checked') };
            }
            
            // Fallback: trigger click on the iCheck parent (div.icheckbox)
            const parent = cb.closest('.icheckbox, .icheckbox_minimal, .icheck, div[class*="icheckbox"]');
            if (parent.length > 0) {
                parent.trigger('click');
                return { method: 'parent click', checked: cb.prop('checked'), parentClass: parent.attr('class') };
            }
            
            // Fallback: trigger click via jQuery  
            cb.trigger('click');
            return { method: 'jQuery trigger', checked: cb.prop('checked') };
        } catch(e) {
            return { error: e.message };
        }
    }""")
    print(f"iCheck result: {json.dumps(icheck_result, indent=2)}")
    
    time.sleep(3)
    page.screenshot(path="e2e3_after_icheck.png")
    print("📸 e2e3_after_icheck.png")
    
    # Check if the plate input appeared
    after_state = page.evaluate("""() => {
        const hint = document.getElementById('idTextoEjemplPatente');
        const textInputs = Array.from(document.querySelectorAll('input[type=text], input:not([type])')).filter(
            inp => inp.offsetParent !== null
        ).map(inp => ({
            id: inp.id, name: inp.name, placeholder: inp.placeholder || ''
        }));
        
        // Check for patente-related inputs
        const patenteInputs = Array.from(document.querySelectorAll('input')).filter(el => {
            const n = (el.name || '').toLowerCase();
            const id = (el.id || '').toLowerCase();
            const ph = (el.placeholder || '').toLowerCase();
            return n.includes('patente') || n.includes('ppu') || n.includes('placa') ||
                   id.includes('patente') || id.includes('ppu') || id.includes('placa') ||
                   ph.includes('llnnnn') || ph.includes('patente');
        }).map(el => ({
            id: el.id, name: el.name, placeholder: el.placeholder || '',
            visible: el.offsetParent !== null, display: window.getComputedStyle(el).display,
        }));
        
        return {
            hintExists: !!hint,
            hintVisible: hint ? hint.offsetParent !== null : false,
            hintText: hint ? hint.innerText.trim() : 'NOT FOUND',
            visibleTextInputCount: textInputs.length,
            textInputs,
            patenteInputs,
        };
    }""")
    print(f"\nAfter state: {json.dumps(after_state, indent=2)}")
    
    # If still no plate input, try Method 2: Playwright force click
    if not after_state.get("hintVisible") and len(after_state.get("patenteInputs", [])) == 0:
        print("\n⚠️ No plate input appeared. Trying Playwright force click...")
        try:
            page.locator("#checkCert_4_4_1_false").click(force=True)
            time.sleep(3)
            after2 = page.evaluate("""() => {
                const hint = document.getElementById('idTextoEjemplPatente');
                return { hintVisible: hint ? hint.offsetParent !== null : false };
            }""")
            print(f"After force click: {after2}")
        except Exception as e:
            print(f"Force click error: {e}")
    
    # If STILL no plate input, try Method 3: click the iCheck-helper element
    if not after_state.get("hintVisible"):
        print("\n⚠️ Trying to click the iCheck-helper (ins element)...")
        try:
            # Find the ins element next to the checkbox
            ins_clicked = page.evaluate("""() => {
                const cb = document.getElementById('checkCert_4_4_1_false');
                if (!cb) return { error: 'no checkbox' };
                const parent = cb.parentElement;
                const ins = parent ? parent.querySelector('ins.iCheck-helper') : null;
                if (ins) {
                    ins.click();
                    return { method: 'ins.click()', checked: cb.checked };
                }
                // Try clicking the parent div
                if (parent) {
                    parent.click();
                    return { method: 'parent.click()', checked: cb.checked };
                }
                return { error: 'no ins or parent found' };
            }""")
            print(f"ins click result: {json.dumps(ins_clicked, indent=2)}")
            time.sleep(3)
            
            after3 = page.evaluate("""() => {
                const hint = document.getElementById('idTextoEjemplPatente');
                return {
                    hintVisible: hint ? hint.offsetParent !== null : false,
                    hintText: hint ? hint.innerText : 'NOT FOUND',
                    bodyChanged: document.body.innerText.includes('LLNNNN'),
                };
            }""")
            print(f"After ins click: {json.dumps(after3, indent=2)}")
            page.screenshot(path="e2e3_after_ins.png")
            print("📸 e2e3_after_ins.png")
        except Exception as e:
            print(f"Error: {e}")
    
    # Final: dump visible page state
    print(f"\n═══ Final page state ═══")
    print(f"URL: {page.url}")
    visible_text = page.evaluate("() => document.body.innerText.substring(0, 600)")
    print(f"Visible text:\n{visible_text}")
    
    browser.close()
    print("\n✅ Done")
