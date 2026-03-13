"""
Test 2: After solving CAPTCHA manually, dump the EXACT HTML structure
of the Vehículos accordion section so we know exactly what to click.
"""
import time
from playwright.sync_api import sync_playwright

URL = "https://www.registrocivil.cl/OficinaInternet/"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, slow_mo=300)
    context = browser.new_context(viewport={"width": 1280, "height": 900})
    page = context.new_page()

    print(f"Opening {URL} ...")
    page.goto(URL, timeout=60000, wait_until="domcontentloaded")
    page.wait_for_timeout(2000)

    print("\n" + "="*60)
    print("👆 SOLVE THE CAPTCHA IN THE BROWSER WINDOW")
    print("   Then press ENTER here to continue...")
    print("="*60)
    input()
    page.wait_for_timeout(2000)

    # Dump the HTML around "Vehículos" text
    print("\n" + "="*60)
    print("SEARCHING FOR 'Vehículos' IN THE DOM...")
    print("="*60)

    result = page.evaluate("""() => {
        const results = [];
        const walker = document.createTreeWalker(
            document.body,
            NodeFilter.SHOW_TEXT,
            null,
            false
        );
        while (walker.nextNode()) {
            const node = walker.currentNode;
            if (node.textContent.trim().toLowerCase().includes('vehículo') ||
                node.textContent.trim().toLowerCase().includes('vehiculo')) {
                const el = node.parentElement;
                // Get the element and 3 levels of parents
                const chain = [];
                let current = el;
                for (let i = 0; i < 4; i++) {
                    if (!current) break;
                    chain.push({
                        tag: current.tagName,
                        id: current.id || '',
                        cls: current.className ? current.className.toString().substring(0,80) : '',
                        text: (current.innerText || '').substring(0, 100),
                        onclick: current.onclick ? 'HAS ONCLICK' : '',
                        href: current.href || '',
                        role: current.getAttribute('role') || '',
                        ariaExpanded: current.getAttribute('aria-expanded') || '',
                        dataToggle: current.getAttribute('data-toggle') || '',
                        style: (current.getAttribute('style') || '').substring(0, 60),
                    });
                    current = current.parentElement;
                }
                results.push({
                    directText: node.textContent.trim(),
                    outerHTML: el.outerHTML.substring(0, 300),
                    chain: chain
                });
            }
        }
        return results;
    }""")

    for i, r in enumerate(result):
        print(f"\n--- Match {i+1}: directText='{r['directText']}' ---")
        print(f"  outerHTML: {r['outerHTML']}")
        for j, c in enumerate(r['chain']):
            indent = "  " * (j + 1)
            print(f"{indent}Level {j}: <{c['tag']} id='{c['id']}' class='{c['cls']}'> role='{c['role']}' aria-expanded='{c['ariaExpanded']}' data-toggle='{c['dataToggle']}' onclick='{c['onclick']}' href='{c['href']}' style='{c['style']}'")

    # Also dump the specific section HTML
    print("\n" + "="*60)
    print("LOOKING FOR ACCORDION STRUCTURE...")
    print("="*60)
    
    accordion_html = page.evaluate("""() => {
        // Look for elements with accordion-related attributes
        const accordions = document.querySelectorAll('[data-toggle], [role=tab], [role=button], .accordion, .panel-heading, .collapse, .collapsible, [aria-expanded]');
        const results = [];
        for (const el of accordions) {
            const txt = (el.innerText || '').trim();
            if (txt.length < 200) {
                results.push({
                    tag: el.tagName,
                    id: el.id,
                    cls: (el.className || '').toString().substring(0,60),
                    text: txt.substring(0,80),
                    role: el.getAttribute('role'),
                    ariaExpanded: el.getAttribute('aria-expanded'),
                    dataToggle: el.getAttribute('data-toggle'),
                    onclick: el.getAttribute('onclick') || (el.onclick ? 'fn' : ''),
                    outerHTML: el.outerHTML.substring(0, 200)
                });
            }
        }
        return results;
    }""")

    if accordion_html:
        for a in accordion_html:
            print(f"\n  <{a['tag']} id='{a['id']}' class='{a['cls']}'> text='{a['text']}'")
            print(f"    role={a['role']} aria-expanded={a['ariaExpanded']} data-toggle={a['dataToggle']} onclick={a['onclick']}")
            print(f"    HTML: {a['outerHTML']}")
    else:
        print("  No accordion elements found with standard attributes!")
        # Try finding by the ">" arrow pattern
        arrow_result = page.evaluate("""() => {
            const all = document.querySelectorAll('*');
            const results = [];
            for (const el of all) {
                const txt = (el.innerText || '').trim();
                if (txt.startsWith('>') && txt.length < 30 && el.children.length < 3) {
                    results.push({
                        tag: el.tagName, id: el.id, 
                        cls: (el.className||'').toString().substring(0,60),
                        text: txt,
                        outerHTML: el.outerHTML.substring(0, 200)
                    });
                }
            }
            return results.slice(0, 20);
        }""")
        print("\n  Elements starting with '>':")
        for a in arrow_result:
            print(f"    <{a['tag']} id='{a['id']}' class='{a['cls']}'> '{a['text']}' -> {a['outerHTML']}")

    # Now let's try clicking Vehículos manually and see what happens
    print("\n" + "="*60)
    print("👆 Now MANUALLY click 'Vehículos' in the browser to expand it")
    print("   Then press ENTER here...")
    print("="*60)
    input()
    page.wait_for_timeout(2000)

    # Dump what's now visible in the Vehículos section
    print("\n" + "="*60)
    print("AFTER MANUALLY EXPANDING VEHÍCULOS:")
    print("="*60)

    veh_items = page.evaluate("""() => {
        const results = [];
        const all = document.querySelectorAll('*');
        let inVehiculos = false;
        for (const el of all) {
            const txt = (el.innerText || '').trim().toLowerCase();
            if (txt.includes('vehículo') && el.children.length < 3 && txt.length < 30) {
                inVehiculos = true;
            }
            if (inVehiculos && txt.includes('anotacion')) {
                const tr = el.closest('tr');
                if (tr) {
                    results.push({
                        rowHTML: tr.outerHTML.substring(0, 500),
                        inputs: Array.from(tr.querySelectorAll('input')).map(i => ({
                            type: i.type, name: i.name, id: i.id, checked: i.checked,
                            outerHTML: i.outerHTML.substring(0, 200)
                        }))
                    });
                }
                break;
            }
        }
        // Also get the input that appeared
        const patenteHint = document.getElementById('idTextoEjemplPatente');
        let nearbyInput = null;
        if (patenteHint) {
            let el = patenteHint;
            for (let i = 0; i < 8; i++) {
                el = el.parentElement;
                if (!el) break;
                const inp = el.querySelector('input[type=text], input:not([type])');
                if (inp) {
                    nearbyInput = {
                        tag: inp.tagName, type: inp.type, name: inp.name, id: inp.id,
                        placeholder: inp.placeholder, outerHTML: inp.outerHTML.substring(0, 200)
                    };
                    break;
                }
            }
        }
        return { vehiculosRow: results, patenteInput: nearbyInput };
    }""")

    print(f"\nVehículos Row: {veh_items.get('vehiculosRow', 'NOT FOUND')}")
    print(f"\nPatente Input: {veh_items.get('patenteInput', 'NOT FOUND')}")

    print("\nDone! Copy all output above.")
    browser.close()
