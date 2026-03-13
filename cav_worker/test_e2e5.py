"""
E2E test v5: Full flow through to payment page.
After adding CAV+plate to cart, fill in Datos del Solicitante
(email: felipe@autodirecto.cl) and click Continuar to see the payment step.
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

    # ── Step 1: Load ──
    print(f"═══ Step 1: Loading page ═══")
    page.goto(URL, timeout=60000, wait_until="domcontentloaded")
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except: pass
    time.sleep(5)
    print(f"URL: {page.url}")
    
    if "código de la imagen" in page.content().lower():
        print("❌ CAPTCHA — aborting")
        page.screenshot(path="e2e5_captcha.png")
        browser.close()
        exit(1)
    print("✅ No CAPTCHA")

    # ── Step 3: Expand Vehículos ──
    print(f"\n═══ Step 3: Expand Vehículos ═══")
    page.locator("#title_5").scroll_into_view_if_needed()
    page.locator("#title_5").click()
    time.sleep(2)
    print("✅ Expanded")

    # ── Step 4: Check CAV via iCheck ──
    print(f"\n═══ Step 4: Check CAV ═══")
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
    time.sleep(3)
    print("✅ CAV checked")

    # ── Step 5: Enter plate ──
    print(f"\n═══ Step 5: Enter plate {PLATE} ═══")
    plate_el = page.locator('input[id^="idInputPPU_"]').first
    plate_el.click()
    plate_el.fill("")
    plate_el.type(PLATE, delay=50)
    time.sleep(1)
    print(f"✅ Typed: {PLATE}")

    # ── Step 6: Click Agregar/Continuar ──
    print(f"\n═══ Step 6: Click Continuar (add to cart) ═══")
    for sel in ['#carro_btnContinuar', '.btn_agregarCarro', 'button:has-text("Continuar")']:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible():
                el.click()
                print(f"✅ Clicked: {sel}")
                break
        except:
            continue
    
    time.sleep(5)
    try: page.wait_for_load_state("networkidle", timeout=10000)
    except: pass
    page.screenshot(path="e2e5_6_after_agregar.png")
    print(f"📸 e2e5_6_after_agregar.png")

    # ── Step 7: Check what page we're on ──
    print(f"\n═══ Step 7: Analyze page state ═══")
    print(f"URL: {page.url}")
    
    page_analysis = page.evaluate("""() => {
        const body = document.body.innerText;
        return {
            bodyPreview: body.substring(0, 1500),
            hasCart: body.includes('Carro de Certificados'),
            hasSolicitante: body.includes('Datos del Solicitante') || body.includes('Solicitante'),
            hasEmail: !!document.querySelector('input[type=email], input[id*="mail"], input[name*="mail"]'),
            hasTotal: /Total\s*\$/.test(body),
            hasContinuar: !!document.querySelector('button:not([disabled])'),
            hasPagar: body.toLowerCase().includes('pagar'),
            hasWebpay: body.toLowerCase().includes('webpay'),
            // Find ALL visible forms/inputs
            visibleInputs: Array.from(document.querySelectorAll('input, select, textarea')).filter(
                el => el.offsetParent !== null && el.type !== 'hidden'
            ).map(el => ({
                tag: el.tagName, type: el.type, id: el.id, name: el.name,
                placeholder: el.placeholder || '', value: el.value || '',
                required: el.required, maxLength: el.maxLength,
            })),
            visibleButtons: Array.from(document.querySelectorAll('button, input[type=submit]')).filter(
                el => el.offsetParent !== null
            ).map(el => ({
                tag: el.tagName, id: el.id, text: (el.innerText || el.value || '').trim().substring(0,50),
                disabled: el.disabled, cls: el.className.substring(0,50),
            })),
        };
    }""")
    
    print(f"\nBody preview:\n{page_analysis['bodyPreview'][:800]}")
    print(f"\nHas cart: {page_analysis['hasCart']}")
    print(f"Has solicitante: {page_analysis['hasSolicitante']}")
    print(f"Has email input: {page_analysis['hasEmail']}")
    print(f"Has total: {page_analysis['hasTotal']}")
    print(f"Has pagar: {page_analysis['hasPagar']}")
    print(f"\nVisible inputs:")
    for inp in page_analysis['visibleInputs']:
        print(f"  {inp}")
    print(f"\nVisible buttons:")
    for btn in page_analysis['visibleButtons']:
        print(f"  {btn}")

    # ── Step 8: Fill in Datos del Solicitante ──
    print(f"\n═══ Step 8: Fill email {EMAIL} ═══")
    
    # Find email input
    email_filled = False
    email_selectors = [
        '#carro_solicitanteInputEmail',
        'input[id*="mail" i]',
        'input[name*="mail" i]', 
        'input[type="email"]',
        'input[placeholder*="mail" i]',
        'input[placeholder*="correo" i]',
    ]
    
    for sel in email_selectors:
        try:
            el = page.locator(sel).first
            if el.count() > 0:
                el.scroll_into_view_if_needed()
                el.click()
                el.fill(EMAIL)
                email_filled = True
                print(f"✅ Email filled via {sel}")
                break
        except Exception as e:
            print(f"  {sel}: {e}")
    
    if not email_filled:
        # JS fallback
        js_result = page.evaluate(f"""() => {{
            const inputs = document.querySelectorAll('input');
            for (const inp of inputs) {{
                const id = (inp.id || '').toLowerCase();
                const name = (inp.name || '').toLowerCase();
                const ph = (inp.placeholder || '').toLowerCase();
                if (id.includes('mail') || name.includes('mail') || ph.includes('correo') || ph.includes('mail')) {{
                    inp.scrollIntoView({{block: 'center'}});
                    inp.value = '{EMAIL}';
                    inp.dispatchEvent(new Event('input', {{bubbles: true}}));
                    inp.dispatchEvent(new Event('change', {{bubbles: true}}));
                    return {{ filled: true, id: inp.id, name: inp.name }};
                }}
            }}
            return {{ filled: false }};
        }}""")
        if js_result.get("filled"):
            email_filled = True
            print(f"✅ Email filled via JS: {js_result}")
        else:
            print(f"❌ Could not find email input")
    
    # Also check if there's a "confirm email" field
    try:
        confirm = page.locator('#carro_solicitanteInputEmailConfirm, input[id*="confirm" i][id*="mail" i]').first
        if confirm.count() > 0:
            confirm.scroll_into_view_if_needed()
            confirm.click()
            confirm.fill(EMAIL)
            print(f"✅ Confirm email filled too")
    except:
        pass
    
    time.sleep(1)
    page.screenshot(path="e2e5_8_email.png")
    print(f"📸 e2e5_8_email.png")

    # ── Step 9: Click "Continuar" to proceed to payment ──
    print(f"\n═══ Step 9: Click Continuar (to payment) ═══")
    
    # The Continuar button might be #carro_btnContinuar or similar
    continuar_clicked = False
    for sel in ['#carro_btnContinuar', '.btn_agregarCarro', 'button:has-text("Continuar")', 
                'button:has-text("Pagar")', 'input[value*="Continuar" i]']:
        try:
            el = page.locator(sel).first
            if el.count() > 0 and el.is_visible():
                el.scroll_into_view_if_needed()
                el.click()
                continuar_clicked = True
                print(f"✅ Clicked: {sel}")
                break
        except:
            continue
    
    if not continuar_clicked:
        # JS fallback
        js_result = page.evaluate("""() => {
            const btns = document.querySelectorAll('button, input[type=submit]');
            for (const btn of btns) {
                if (btn.offsetParent === null || btn.disabled) continue;
                const txt = (btn.innerText || btn.value || '').toLowerCase();
                if (/continuar|pagar|siguiente|enviar/.test(txt)) {
                    btn.scrollIntoView({block: 'center'});
                    btn.click();
                    return { clicked: true, text: txt.substring(0,30), id: btn.id };
                }
            }
            return { clicked: false };
        }""")
        print(f"JS click: {json.dumps(js_result)}")
        continuar_clicked = js_result.get("clicked", False)
    
    if not continuar_clicked:
        print("❌ Could not find Continuar/Pagar button")
    
    time.sleep(8)
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except: pass
    
    page.screenshot(path="e2e5_9_after_continuar.png", full_page=True)
    print(f"📸 e2e5_9_after_continuar.png")

    # ── Step 10: Analyze payment page ──
    print(f"\n═══ Step 10: Payment page analysis ═══")
    print(f"URL: {page.url}")
    
    payment_info = page.evaluate("""() => {
        const body = document.body.innerText;
        return {
            bodyPreview: body.substring(0, 2000),
            url: location.href,
            title: document.title,
            hasWebpay: body.toLowerCase().includes('webpay'),
            hasPagar: body.toLowerCase().includes('pagar'),
            hasTarjeta: body.toLowerCase().includes('tarjeta'),
            hasTransbank: body.toLowerCase().includes('transbank'),
            hasTotal: /Total\s*\$/.test(body),
            hasError: body.toLowerCase().includes('error'),
            // Check for iframes (WebPay often uses iframe)
            iframes: Array.from(document.querySelectorAll('iframe')).map(f => ({
                src: f.src, id: f.id, name: f.name,
                width: f.width, height: f.height,
            })),
            // All links
            links: Array.from(document.querySelectorAll('a')).filter(
                a => a.offsetParent !== null
            ).slice(0, 20).map(a => ({
                href: a.href, text: (a.innerText || '').trim().substring(0,50),
            })),
            // Forms
            forms: Array.from(document.querySelectorAll('form')).map(f => ({
                action: f.action, method: f.method, id: f.id,
                inputs: Array.from(f.querySelectorAll('input')).map(i => ({
                    type: i.type, name: i.name, id: i.id, value: i.value.substring(0,50),
                })),
            })),
            visibleInputs: Array.from(document.querySelectorAll('input, select')).filter(
                el => el.offsetParent !== null && el.type !== 'hidden'
            ).map(el => ({
                tag: el.tagName, type: el.type, id: el.id, name: el.name,
                value: el.value.substring(0, 50),
            })),
            visibleButtons: Array.from(document.querySelectorAll('button, input[type=submit]')).filter(
                el => el.offsetParent !== null
            ).map(el => ({
                id: el.id, text: (el.innerText || el.value || '').trim().substring(0,50),
                disabled: el.disabled,
            })),
        };
    }""")
    
    print(f"\nTitle: {payment_info['title']}")
    print(f"\nBody preview:\n{payment_info['bodyPreview'][:1200]}")
    print(f"\nWebPay: {payment_info['hasWebpay']}")
    print(f"Pagar: {payment_info['hasPagar']}")
    print(f"Tarjeta: {payment_info['hasTarjeta']}")
    print(f"Transbank: {payment_info['hasTransbank']}")
    print(f"Error: {payment_info['hasError']}")
    print(f"\nIframes: {json.dumps(payment_info['iframes'], indent=2)}")
    print(f"\nForms: {json.dumps(payment_info['forms'], indent=2)}")
    print(f"\nVisible inputs: {json.dumps(payment_info['visibleInputs'], indent=2)}")
    print(f"\nVisible buttons: {json.dumps(payment_info['visibleButtons'], indent=2)}")
    print(f"\nLinks: {json.dumps(payment_info['links'][:10], indent=2)}")

    browser.close()
    print("\n✅ E2E v5 complete!")
