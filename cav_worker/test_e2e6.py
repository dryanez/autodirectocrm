"""
E2E test v6: Fix "Agregar al Carro" button click.
The per-certificate button has ID like btn_agregarCarro_1#4_4_1#1
which contains # characters — need special handling.
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

    # ── Load ──
    print("═══ Loading ═══")
    page.goto(URL, timeout=60000, wait_until="domcontentloaded")
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except: pass
    time.sleep(5)
    print(f"✅ URL: {page.url}")

    # ── Expand Vehículos ──
    print("\n═══ Expand Vehículos ═══")
    page.locator("#title_5").scroll_into_view_if_needed()
    page.locator("#title_5").click()
    time.sleep(2)
    print("✅ Expanded")

    # ── Check CAV via iCheck ──
    print("\n═══ Check CAV (iCheck) ═══")
    page.evaluate("""() => {
        const rows = document.querySelectorAll('tr');
        for (const row of rows) {
            const txt = (row.innerText || '').toLowerCase();
            if (txt.includes('anotaciones vigentes') && !txt.includes('multas')) {
                const cb = row.querySelector('input[type=checkbox]');
                if (cb && typeof jQuery !== 'undefined' && typeof jQuery.fn.iCheck !== 'undefined') {
                    jQuery(cb).iCheck('check');
                }
            }
        }
    }""")
    time.sleep(2)
    print("✅ CAV checked")

    # ── Enter plate ──
    print(f"\n═══ Enter plate {PLATE} ═══")
    plate_el = page.locator('input[id^="idInputPPU_"]').first
    plate_el.click()
    plate_el.fill("")
    plate_el.type(PLATE, delay=50)
    time.sleep(1)
    print(f"✅ Typed: {PLATE}")
    page.screenshot(path="e2e6_plate.png")

    # ── Click "Agregar al Carro" — the PER-CERTIFICATE button ──
    # Button ID: btn_agregarCarro_1#4_4_1#1  (has # in ID — can't use CSS selector)
    print("\n═══ Click 'Agregar al Carro' ═══")
    
    agregar_result = page.evaluate("""() => {
        // Find button by text that says "Agregar al Carro" and is visible
        const btns = document.querySelectorAll('button.btn_agregarCarro');
        for (const btn of btns) {
            if (btn.offsetParent === null) continue;
            const txt = (btn.innerText || '').trim();
            if (txt.includes('Agregar')) {
                btn.scrollIntoView({block: 'center'});
                btn.click();
                return { clicked: true, id: btn.id, text: txt };
            }
        }
        // Fallback: find any visible button with "Agregar" text
        const allBtns = document.querySelectorAll('button');
        for (const btn of allBtns) {
            if (btn.offsetParent === null) continue;
            const txt = (btn.innerText || '').trim();
            if (txt.includes('Agregar')) {
                btn.scrollIntoView({block: 'center'});
                btn.click();
                return { clicked: true, id: btn.id, text: txt, fallback: true };
            }
        }
        return { clicked: false };
    }""")
    print(f"Result: {json.dumps(agregar_result)}")
    
    time.sleep(5)
    try: page.wait_for_load_state("networkidle", timeout=10000)
    except: pass
    
    # Check if item was added to cart
    cart_state = page.evaluate("""() => {
        const totalEl = document.getElementById('carro_valor_total');
        const vacioEl = document.getElementById('carro_textoVacio');
        const hayCertEl = document.getElementById('carro_textoHayCertificados');
        const cartTable = document.getElementById('carro_tablasListaCertificados');
        return {
            total: totalEl ? totalEl.innerText.trim() : 'not found',
            vacio: vacioEl ? window.getComputedStyle(vacioEl).display : 'not found',
            hayCert: hayCertEl ? window.getComputedStyle(hayCertEl).display : 'not found', 
            cartRows: cartTable ? cartTable.querySelectorAll('tr').length : 0,
            cartText: cartTable ? cartTable.innerText.trim().substring(0, 300) : '',
        };
    }""")
    print(f"Cart state: {json.dumps(cart_state, indent=2)}")
    page.screenshot(path="e2e6_cart.png")
    print("📸 e2e6_cart.png")
    
    if cart_state.get('total', '0') == '0' or cart_state.get('total', '') == '$ 0':
        print("⚠️ Cart still empty! Agregar didn't work.")
        print("Let me try Playwright click on the Agregar button...")
        
        # Try Playwright locator with text
        try:
            btn = page.locator("button:has-text('Agregar al Carro')").first
            if btn.count() > 0 and btn.is_visible():
                btn.click()
                print("✅ Playwright click on 'Agregar al Carro'")
                time.sleep(5)
            else:
                print("Button not visible via Playwright")
                # Try force click
                btn.click(force=True)
                print("✅ Force clicked")
                time.sleep(5)
        except Exception as e:
            print(f"Playwright click failed: {e}")
        
        # Re-check cart
        cart_state = page.evaluate("""() => {
            const totalEl = document.getElementById('carro_valor_total');
            return { total: totalEl ? totalEl.innerText.trim() : 'not found' };
        }""")
        print(f"Cart after retry: {json.dumps(cart_state)}")
    
    page.screenshot(path="e2e6_cart_after.png")
    
    # ── Now check if email/solicitante section is visible ──
    print("\n═══ Check Datos del Solicitante visibility ═══")
    
    sol_state = page.evaluate("""() => {
        const container = document.getElementById('carro_SolicitanteContainer');
        const emailInput = document.getElementById('carro_solicitanteInputEmail');
        const emailConfirm = document.getElementById('carro_solicitanteInputEmailConfirm');
        const btnContinuar = document.getElementById('carro_btnContinuar');
        return {
            containerExists: !!container,
            containerVisible: container ? container.offsetParent !== null : false,
            containerDisplay: container ? window.getComputedStyle(container).display : 'not found',
            emailExists: !!emailInput,
            emailVisible: emailInput ? emailInput.offsetParent !== null : false,
            emailConfirmExists: !!emailConfirm,
            emailConfirmVisible: emailConfirm ? emailConfirm.offsetParent !== null : false,
            btnContinuarExists: !!btnContinuar,
            btnContinuarVisible: btnContinuar ? btnContinuar.offsetParent !== null : false,
            btnContinuarText: btnContinuar ? btnContinuar.innerText.trim() : '',
        };
    }""")
    print(f"Solicitante state: {json.dumps(sol_state, indent=2)}")
    
    if not sol_state.get('emailVisible'):
        print("Email NOT visible — cart might be empty or we need to scroll")
        # Try scrolling the page all the way down
        page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1)
        # Re-check
        email_vis = page.evaluate("() => { const e = document.getElementById('carro_solicitanteInputEmail'); return e ? e.offsetParent !== null : false; }")
        print(f"Email visible after scroll: {email_vis}")
    
    # ── Fill email ──
    if sol_state.get('emailVisible') or sol_state.get('emailExists'):
        print(f"\n═══ Fill email: {EMAIL} ═══")
        
        # Use JS to fill (since Playwright can't see hidden elements)
        fill_result = page.evaluate(f"""() => {{
            const email = document.getElementById('carro_solicitanteInputEmail');
            const confirm = document.getElementById('carro_solicitanteInputEmailConfirm');
            const results = {{}};
            
            if (email) {{
                email.value = '{EMAIL}';
                email.dispatchEvent(new Event('input', {{bubbles: true}}));
                email.dispatchEvent(new Event('change', {{bubbles: true}}));
                email.dispatchEvent(new Event('blur', {{bubbles: true}}));
                results.email = {{ filled: true, value: email.value }};
            }}
            
            if (confirm) {{
                confirm.value = '{EMAIL}';
                confirm.dispatchEvent(new Event('input', {{bubbles: true}}));
                confirm.dispatchEvent(new Event('change', {{bubbles: true}}));
                confirm.dispatchEvent(new Event('blur', {{bubbles: true}}));
                results.confirm = {{ filled: true, value: confirm.value }};
            }}
            
            return results;
        }}""")
        print(f"Fill result: {json.dumps(fill_result)}")
        
        # Also fill hidden form fields for the second form
        page.evaluate(f"""() => {{
            const fields = {{
                'carro_email': '{EMAIL}',
                'carro_emailConfirm': '{EMAIL}',
                'carro_email2': '{EMAIL}',
                'carro_emailConfirm2': '{EMAIL}',
            }};
            for (const [name, val] of Object.entries(fields)) {{
                const el = document.querySelector('[name="' + name + '"], #' + name);
                if (el) {{ el.value = val; }}
            }}
        }}""")
        print("✅ Also filled hidden form email fields")
    
    time.sleep(1)
    page.screenshot(path="e2e6_email.png")
    print("📸 e2e6_email.png")
    
    # ── Click Continuar ──
    print("\n═══ Click Continuar (submit cart) ═══")
    
    # The main form submits to carro.srcei
    # The second form (idContinuarEntregaDocumentos) submits to entregadocumentos.srcei
    continuar_result = page.evaluate("""() => {
        // First try the main Continuar button
        const btn = document.getElementById('carro_btnContinuar');
        if (btn) {
            btn.scrollIntoView({block: 'center'});
            btn.click();
            return { clicked: true, id: 'carro_btnContinuar', text: btn.innerText.trim() };
        }
        
        // Try submitting the form directly
        const form = document.getElementById('idContinuarEntregaDocumentos');
        if (form) {
            form.submit();
            return { clicked: true, method: 'form.submit', formId: 'idContinuarEntregaDocumentos' };
        }
        
        return { clicked: false };
    }""")
    print(f"Continuar result: {json.dumps(continuar_result)}")
    
    time.sleep(8)
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except: pass
    
    page.screenshot(path="e2e6_after_continuar.png", full_page=True)
    print("📸 e2e6_after_continuar.png")
    
    # ── Analyze result page ──
    print(f"\n═══ Result page ═══")
    print(f"URL: {page.url}")
    
    result_info = page.evaluate("""() => ({
        url: location.href,
        title: document.title,
        bodyPreview: document.body.innerText.substring(0, 2000),
        hasWebpay: document.body.innerText.toLowerCase().includes('webpay'),
        hasPagar: document.body.innerText.toLowerCase().includes('pagar'),
        hasError: document.body.innerText.toLowerCase().includes('error'),
        hasTarjeta: document.body.innerText.toLowerCase().includes('tarjeta'),
        iframes: Array.from(document.querySelectorAll('iframe')).map(f => ({
            src: f.src, id: f.id
        })),
        forms: Array.from(document.querySelectorAll('form')).map(f => ({
            action: f.action, method: f.method, id: f.id
        })),
    })""")
    
    print(f"Title: {result_info['title']}")
    print(f"WebPay: {result_info['hasWebpay']}")
    print(f"Pagar: {result_info['hasPagar']}")
    print(f"Error: {result_info['hasError']}")
    print(f"Tarjeta: {result_info['hasTarjeta']}")
    print(f"\nBody:\n{result_info['bodyPreview'][:1000]}")
    print(f"\nIframes: {json.dumps(result_info['iframes'], indent=2)}")
    print(f"Forms: {json.dumps(result_info['forms'], indent=2)}")
    
    browser.close()
    print("\n✅ E2E v6 complete!")
