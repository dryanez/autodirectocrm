"""
E2E test v9: Find the JS handler for btn_agregarCarro clicks.
The error "Ha excedido el número de certificados permitidos" suggests rate limiting.
Also notice `run=''` in the iframe URL — the RUN (RUT) is empty.
Let's find how the click handler constructs the iframe URL and what RUN should be.
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

    # ── Find script files loaded by the page ──
    print("\n═══ External scripts loaded ═══")
    scripts = page.evaluate("""() => {
        return Array.from(document.querySelectorAll('script[src]'))
            .map(s => s.src)
            .filter(s => s.includes('registrocivil') || s.includes('oi_') || s.includes('carro'));
    }""")
    for s in scripts:
        print(f"  {s}")

    # ── Search inline scripts for btn_agregarCarro handler ──
    print("\n═══ Inline script search: btn_agregarCarro ═══")
    handler_code = page.evaluate(r"""() => {
        const scripts = document.querySelectorAll('script:not([src])');
        let results = [];
        for (const s of scripts) {
            const text = s.textContent || '';
            // Search for the button handler
            const patterns = ['btn_agregarCarro', 'agregarACarro', 'cu_idIframe4', 'divAgregarACarro'];
            for (const pat of patterns) {
                const idx = text.indexOf(pat);
                if (idx >= 0) {
                    const start = Math.max(0, idx - 200);
                    const end = Math.min(text.length, idx + 500);
                    results.push({
                        pattern: pat,
                        snippet: text.substring(start, end)
                    });
                }
            }
        }
        return results;
    }""")
    for r in handler_code:
        print(f"\n--- Pattern: {r['pattern']} ---")
        print(r['snippet'][:600])

    # ── Also check if there's a jQuery event bound ──
    print("\n═══ jQuery click event on .btn_agregarCarro ═══")
    jquery_events = page.evaluate("""() => {
        if (typeof jQuery === 'undefined') return 'no jQuery';
        const events = jQuery._data(document, 'events') || {};
        const clickHandlers = (events.click || []).map(h => ({
            selector: h.selector || 'none',
            handler: (h.handler || '').toString().substring(0, 300),
        }));
        return { docEvents: clickHandlers };
    }""")
    print(json.dumps(jquery_events, indent=2))

    # ── Try to get the delegated click handler ──
    print("\n═══ Event delegation search ═══")
    delegation = page.evaluate("""() => {
        if (typeof jQuery === 'undefined') return 'no jQuery';
        
        // Check events on body, document, various containers
        const targets = [document, document.body, document.getElementById('mainContainer')];
        const results = {};
        
        for (let i = 0; i < targets.length; i++) {
            const t = targets[i];
            if (!t) continue;
            const events = jQuery._data(t, 'events') || {};
            for (const [type, handlers] of Object.entries(events)) {
                for (const h of handlers) {
                    const handlerStr = h.handler.toString();
                    if (handlerStr.includes('agregarCarro') || 
                        handlerStr.includes('iframe') ||
                        handlerStr.includes('Agregar') ||
                        (h.selector && h.selector.includes('btn_agregarCarro'))) {
                        results[`target${i}_${type}_${h.selector || 'direct'}`] = handlerStr.substring(0, 800);
                    }
                }
            }
        }
        return results;
    }""")
    print(json.dumps(delegation, indent=2))

    browser.close()
    print("\n✅ Done!")
