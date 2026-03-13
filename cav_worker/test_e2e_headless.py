"""
Full end-to-end test in HEADLESS mode with anti-bot detection.
Simulates what Railway will do.
"""
import json, time
from playwright.sync_api import sync_playwright

URL = "https://www.registrocivil.cl/OficinaInternet/"
PLATE = "GKZR72"  # Test plate

with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox", 
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-blink-features=AutomationControlled",
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
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['es-CL', 'es', 'en'] });
        window.chrome = { runtime: {} };
    """)
    page = context.new_page()

    print(f"═══ Opening {URL} ═══")
    resp = page.goto(URL, timeout=60000, wait_until="domcontentloaded")
    print(f"Status: {resp.status}")
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except:
        pass
    time.sleep(5)

    # Check CAPTCHA
    content = page.content().lower()
    has_captcha = "código de la imagen" in content
    print(f"Has CAPTCHA: {has_captcha}")
    if has_captcha:
        print("❌ Got CAPTCHA — can't proceed without 2captcha solver. Aborting.")
        page.screenshot(path="e2e_captcha.png")
        browser.close()
        exit(1)

    print(f"URL: {page.url}")
    page.screenshot(path="e2e_1_loaded.png")
    print("📸 e2e_1_loaded.png")

    # ── Step 3: Click Vehículos ──
    print(f"\n═══ Step 3: Expanding Vehículos ═══")
    title5 = page.locator("#title_5")
    if title5.count() > 0:
        title5.scroll_into_view_if_needed()
        title5.click()
        time.sleep(2)
        
        state = page.evaluate("""() => {
            const d = document.getElementById('divLista_5');
            if (!d) return { expanded: false, reason: 'not found' };
            const certTable = document.getElementById('certificadosTable');
            return {
                expanded: window.getComputedStyle(d).display !== 'none',
                display: window.getComputedStyle(d).display,
                childCount: d.children.length,
                hasCertTable: !!certTable,
                certRows: certTable ? certTable.querySelectorAll('tr').length : 0,
                contentPreview: d.innerText.trim().substring(0, 300),
            };
        }""")
        print(f"Result: {json.dumps(state, indent=2)}")
        page.screenshot(path="e2e_2_vehiculos.png")
        print("📸 e2e_2_vehiculos.png")
        
        if not state.get("expanded"):
            print("❌ Click didn't expand!")
            browser.close()
            exit(1)
    else:
        print("❌ #title_5 not found!")
        browser.close()
        exit(1)

    # ── Step 4: Click CAV checkbox ──
    print(f"\n═══ Step 4: Clicking CAV certificate ═══")
    cav_result = page.evaluate("""() => {
        const rows = document.querySelectorAll('tr');
        for (const row of rows) {
            const txt = (row.innerText || '').toLowerCase();
            if (txt.includes('anotaciones vigentes') && !txt.includes('multas')) {
                const cb = row.querySelector('input[type=checkbox], input[type=radio]');
                if (cb) {
                    cb.scrollIntoView({block: 'center'});
                    cb.click();
                    return {
                        found: true, clicked: true,
                        cbId: cb.id, cbName: cb.name,
                        checked: cb.checked,
                        rowText: txt.substring(0, 100)
                    };
                }
                return { found: true, clicked: false, noCheckbox: true, rowText: txt.substring(0, 100) };
            }
        }
        return { found: false };
    }""")
    print(f"Result: {json.dumps(cav_result, indent=2)}")
    
    if not cav_result.get("found"):
        # Dump all rows for debugging
        rows = page.evaluate("""() => Array.from(document.querySelectorAll('tr')).map(
            r => r.innerText.trim().substring(0, 80)
        ).filter(t => t.length > 5)""")
        print(f"All rows ({len(rows)}):")
        for r in rows[:30]:
            print(f"  | {r}")
    
    time.sleep(3)
    page.screenshot(path="e2e_3_cav_clicked.png")
    print("📸 e2e_3_cav_clicked.png")

    # ── Step 5: Find and fill plate input ──
    print(f"\n═══ Step 5: Entering plate {PLATE} ═══")
    
    # Check what inputs are now visible
    inputs = page.evaluate("""() => Array.from(document.querySelectorAll('input')).filter(
        inp => inp.offsetParent !== null && inp.type !== 'hidden'
    ).map(inp => ({
        type: inp.type, name: inp.name, id: inp.id,
        placeholder: inp.placeholder || '',
        maxLength: inp.maxLength,
        nearbyText: (() => {
            let el = inp;
            for (let i=0; i<3; i++) { el = el.parentElement; if (!el) break; }
            return el ? el.innerText.trim().substring(0,100) : '';
        })()
    }))""")
    print(f"Visible inputs: {json.dumps(inputs, indent=2)}")
    
    # Find the plate input (not captcha, not hidden)
    plate_input_id = page.evaluate("""() => {
        const hint = document.getElementById('idTextoEjemplPatente');
        if (hint) {
            // Walk up to find nearby text input
            let el = hint;
            for (let i = 0; i < 8; i++) {
                el = el.parentElement;
                if (!el) break;
                const inputs = el.querySelectorAll('input[type=text], input:not([type])');
                for (const inp of inputs) {
                    if (inp.offsetParent === null) continue;
                    const n = (inp.name || '').toLowerCase();
                    if (n.includes('captcha') || n.includes('codigo')) continue;
                    inp.id = inp.id || '_plate_input';
                    return inp.id;
                }
            }
        }
        // Broader: any visible text input
        const all = document.querySelectorAll('input[type=text], input:not([type])');
        for (const inp of all) {
            if (inp.offsetParent === null) continue;
            const n = (inp.name || '').toLowerCase();
            if (n.includes('captcha') || n.includes('codigo')) continue;
            inp.id = inp.id || '_plate_input';
            return inp.id;
        }
        return null;
    }""")
    print(f"Plate input ID: {plate_input_id}")
    
    if plate_input_id:
        el = page.locator(f"#{plate_input_id}").first
        el.click()
        el.fill("")
        el.type(PLATE, delay=50)
        time.sleep(1)
        print(f"✅ Typed plate: {PLATE}")
        page.screenshot(path="e2e_4_plate.png")
        print("📸 e2e_4_plate.png")
        
        # ── Step 6: Click Agregar/Continuar ──
        print(f"\n═══ Step 6: Click Agregar/Continuar ═══")
        
        # Dump buttons
        buttons = page.evaluate("""() => Array.from(
            document.querySelectorAll('button, input[type=submit], input[type=button], .btn_agregarCarro')
        ).filter(b => b.offsetParent !== null).map(b => ({
            tag: b.tagName, id: b.id, cls: b.className,
            text: (b.innerText || b.value || '').trim().substring(0, 50),
            visible: b.offsetParent !== null
        }))""")
        print(f"Buttons: {json.dumps(buttons, indent=2)}")
        
        # Click the first relevant button
        clicked = False
        for sel in ['#carro_btnContinuar', '.btn_agregarCarro', 'button:has-text("Agregar")', 'button:has-text("Continuar")']:
            try:
                el = page.locator(sel).first
                if el.count() > 0 and el.is_visible():
                    el.click()
                    clicked = True
                    print(f"✅ Clicked: {sel}")
                    break
            except:
                continue
        
        if not clicked:
            # JS fallback
            js_click = page.evaluate("""() => {
                const btns = document.querySelectorAll('button, input[type=submit], .btn_agregarCarro');
                for (const btn of btns) {
                    if (btn.offsetParent === null) continue;
                    const txt = (btn.innerText || btn.value || '').toLowerCase();
                    if (/agregar|continuar/.test(txt)) {
                        btn.click();
                        return { clicked: true, text: txt };
                    }
                }
                return { clicked: false };
            }""")
            print(f"JS click result: {json.dumps(js_click)}")
            clicked = js_click.get("clicked", False)
        
        time.sleep(5)
        page.screenshot(path="e2e_5_submitted.png")
        print("📸 e2e_5_submitted.png")
        
        # ── Step 7: Read result ──
        print(f"\n═══ Step 7: Reading result ═══")
        print(f"URL: {page.url}")
        body_text = page.evaluate("() => document.body.innerText.substring(0, 1000)")
        print(f"Body text:\n{body_text}")
        
        page.screenshot(path="e2e_6_result.png", full_page=True)
        print("📸 e2e_6_result.png")
    else:
        print("❌ No plate input found!")

    browser.close()
    print("\n✅ Full E2E test complete.")
