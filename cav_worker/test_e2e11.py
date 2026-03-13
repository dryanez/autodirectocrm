"""
E2E test v11: 
Find the per-row "Agregar al Carro" click handler.
The button id has hash chars: btn_agregarCarro_1#4_4_1#1
The iframe opens with run='' (empty). We need to find what sets the RUN.
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

    # ── Find the ACTUAL per-row Agregar handler ──
    print("\n═══ Finding per-row Agregar click handler ═══")
    
    # First expand vehiculos and check CAV
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

    # Now get the per-row button handler
    row_btn_handler = page.evaluate(r"""() => {
        // The per-row Agregar button has ID like btn_agregarCarro_1#4_4_1#1
        // Find it by querying all buttons with class btn_agregarCarro that are visible
        const btns = document.querySelectorAll('button.btn_agregarCarro');
        const results = [];
        for (const btn of btns) {
            if (!btn.offsetParent) continue;
            const text = (btn.innerText || '').trim();
            if (!text.includes('Agregar al Carro')) continue;
            
            // Get jQuery events on this specific button
            const events = jQuery._data(btn, 'events') || {};
            const clickHandlers = (events.click || []).map(h => ({
                handler: h.handler.toString().substring(0, 1500),
                namespace: h.namespace || '',
            }));
            
            results.push({
                id: btn.id,
                text: text,
                jqueryClickHandlers: clickHandlers,
            });
        }
        return results;
    }""")
    print(json.dumps(row_btn_handler, indent=2))
    
    # ── Fetch OI.js source to find the Agregar handler ──
    print("\n═══ Fetching OI.js source (searching for Agregar code) ═══")
    oi_js = page.evaluate(r"""async () => {
        try {
            const resp = await fetch('/OficinaInternet/web/js/OI.js?srcei=LA92UI');
            const text = await resp.text();
            
            // Find the part that deals with btn_agregarCarro
            const snippets = [];
            const patterns = ['btn_agregarCarro', 'agregarACarro', 'cu_idIframe', 'divAgregarACarro', 'filtro='];
            for (const pat of patterns) {
                let idx = 0;
                while (true) {
                    idx = text.indexOf(pat, idx);
                    if (idx < 0) break;
                    const start = Math.max(0, idx - 300);
                    const end = Math.min(text.length, idx + 400);
                    snippets.push({
                        pattern: pat,
                        position: idx,
                        snippet: text.substring(start, end)
                    });
                    idx += pat.length;
                }
            }
            return { totalLen: text.length, snippets };
        } catch(e) {
            return { error: e.message };
        }
    }""")
    
    print(f"OI.js total length: {oi_js.get('totalLen', 'unknown')}")
    for s in oi_js.get('snippets', []):
        print(f"\n--- Pattern: {s['pattern']} @ {s['position']} ---")
        print(s['snippet'][:700])
    
    browser.close()
    print("\n✅ Done!")
