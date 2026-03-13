"""
E2E test v8: 
- The iframe said "Ha excedido el número de certificados permitidos."
- URL has run='' (empty). The RUN might be a RUT number needed.
- Let's check the actual form more carefully before clicking.
- Let's also check if there's a RUN field we missed.
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

    print("═══ Step 1: Load page ═══")
    page.goto(URL, timeout=60000, wait_until="domcontentloaded")
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except: pass
    time.sleep(5)

    print("\n═══ Step 2: Check what the main page frame structure is ═══")
    # The main URL loaded carro.srcei - let's see where we actually are
    frames = page.frames
    print(f"Frames on load ({len(frames)}):")
    for f in frames:
        print(f"  name='{f.name}', url='{f.url}'")
    
    print("\n═══ Step 3: Search for RUN/RUT input field ═══")
    all_inputs = page.evaluate("""() => {
        return Array.from(document.querySelectorAll('input, select, textarea')).map(el => ({
            tag: el.tagName, type: el.type, id: el.id, name: el.name,
            placeholder: el.placeholder || '',
            value: el.value,
            visible: el.offsetParent !== null,
            parentId: el.parentElement ? el.parentElement.id : '',
            classes: el.className.substring(0, 60),
        })).filter(el => {
            const s = (el.id + el.name + el.placeholder + el.classes).toLowerCase();
            return s.includes('run') || s.includes('rut') || s.includes('ruc') || s.includes('solicit')
                || s.includes('email') || s.includes('nombre') || s.includes('hidden')
                || el.type === 'hidden';
        });
    }""")
    print(f"RUN/RUT/hidden inputs: {json.dumps(all_inputs, indent=2)}")

    # ── Expand Vehículos ──
    print("\n═══ Step 4: Expand Vehículos ═══")
    page.locator("#title_5").scroll_into_view_if_needed()
    page.locator("#title_5").click()
    time.sleep(2)

    # ── Now look at the CAV section in detail ──
    print("\n═══ Step 5: Look at CAV section inputs + labels ═══")
    cav_section = page.evaluate("""() => {
        const div = document.getElementById('divLista_5');
        if (!div) return { error: 'no divLista_5' };
        
        // Get ALL inputs in the Vehiculos section
        const inputs = Array.from(div.querySelectorAll('input, select')).map(el => ({
            tag: el.tagName, type: el.type, id: el.id, name: el.name,
            placeholder: el.placeholder || '', value: el.value,
            visible: el.offsetParent !== null,
        }));
        
        // Get text content to understand the form
        const text = div.innerText.substring(0, 2000);
        
        // Get the CAV row specifically
        const rows = div.querySelectorAll('tr');
        let cavRow = null;
        for (const row of rows) {
            if (row.innerText.toLowerCase().includes('anotaciones vigentes') && 
                !row.innerText.toLowerCase().includes('multas')) {
                cavRow = {
                    text: row.innerText.substring(0, 200),
                    html: row.innerHTML.substring(0, 500),
                    inputs: Array.from(row.querySelectorAll('input')).map(el => ({
                        type: el.type, id: el.id, name: el.name, value: el.value,
                    })),
                };
            }
        }
        
        return { inputs, text: text.substring(0, 1000), cavRow };
    }""")
    print(f"Section text:\n{cav_section.get('text', '')[:500]}")
    print(f"\nAll inputs: {json.dumps(cav_section.get('inputs', []), indent=2)}")
    print(f"\nCAV Row: {json.dumps(cav_section.get('cavRow'), indent=2)}")
    
    # ── Check checkbox and enter plate ──
    print("\n═══ Step 6: Check checkbox + enter plate ═══")
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

    # ── Before clicking Agregar, examine what JS function it calls ──
    print("\n═══ Step 7: Examine Agregar button JS handler ═══")
    btn_info = page.evaluate("""() => {
        const btns = document.querySelectorAll('button.btn_agregarCarro');
        const result = [];
        for (const btn of btns) {
            if (btn.offsetParent === null) continue;
            result.push({
                id: btn.id,
                text: (btn.innerText || '').trim(),
                onclick: btn.getAttribute('onclick') || 'none',
                classes: btn.className,
                parent: btn.parentElement ? btn.parentElement.id : '',
                html: btn.outerHTML.substring(0, 300),
            });
        }
        
        // Check for any onclick or event listeners
        // Also check if there's a function called agregarACarro or similar
        const funcs = [];
        for (const key of Object.keys(window)) {
            if (typeof window[key] === 'function' && 
                key.toLowerCase().includes('agregar')) {
                funcs.push(key);
            }
        }
        
        return { buttons: result, agregarFunctions: funcs };
    }""")
    print(f"Buttons: {json.dumps(btn_info.get('buttons', []), indent=2)}")
    print(f"Agregar functions: {btn_info.get('agregarFunctions', [])}")
    
    # ── Try to find the JS source that handles the Agregar click ──
    print("\n═══ Step 8: Find the JS handler for btn_agregarCarro ═══")
    js_handler = page.evaluate("""() => {
        // Check if jQuery has event handlers bound
        const btns = document.querySelectorAll('button.btn_agregarCarro');
        const result = {};
        for (const btn of btns) {
            if (btn.offsetParent === null) continue;
            // Try to get jQuery events
            if (typeof jQuery !== 'undefined') {
                const events = jQuery._data(btn, 'events') || {};
                result.jqueryEvents = Object.keys(events);
            }
            // Check for onclick attribute
            result.onclick = btn.getAttribute('onclick');
            result.id = btn.id;
        }
        
        // Also look at the iframe URL pattern - maybe we can see the real URL format
        // The key question: what should 'run' parameter be?
        
        // Search for the agregarACarro function in any script
        const scripts = document.querySelectorAll('script');
        let agregarScript = '';
        for (const s of scripts) {
            const text = s.textContent || '';
            if (text.includes('agregarACarro') || text.includes('btn_agregarCarro')) {
                const idx = text.indexOf('agregarACarro');
                const idx2 = text.indexOf('btn_agregarCarro');
                const pos = Math.min(
                    idx >= 0 ? idx : 99999,
                    idx2 >= 0 ? idx2 : 99999
                );
                agregarScript += text.substring(Math.max(0, pos - 100), pos + 500) + '\n---\n';
            }
        }
        result.scriptSnippets = agregarScript.substring(0, 3000);
        
        return result;
    }""")
    print(f"jQuery events: {js_handler.get('jqueryEvents', [])}")
    print(f"onclick: {js_handler.get('onclick')}")
    print(f"\nScript snippets:\n{js_handler.get('scriptSnippets', 'none')[:2000]}")
    
    browser.close()
    print("\n✅ E2E v8 complete!")
