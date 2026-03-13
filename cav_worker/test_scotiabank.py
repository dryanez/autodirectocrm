"""
Focused test: Full flow → TGR → Scotiabank → Empresas → login.

Usage:
  python test_scotiabank.py PLATE              # default, no proxy
  python test_scotiabank.py PLATE proxy_url    # with proxy to avoid rate-limit

Examples:
  python test_scotiabank.py WG9755
  python test_scotiabank.py WG9755 socks5://user:pass@proxy.example.com:1080
  python test_scotiabank.py WG9755 http://user:pass@proxy.example.com:8080
"""
import json, time, sys, os
from playwright.sync_api import sync_playwright

URL = "https://www.registrocivil.cl/OficinaInternet/"
PLATE = sys.argv[1] if len(sys.argv) > 1 else "WG9755"
PROXY = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("PROXY_URL", "")
EMAIL = "felipe@autodirecto.cl"

# Scotiabank Empresas credentials
RUT_EMPRESA = "783557177"
CLAVE = "Comoestas01"

if PROXY:
    print(f"🔒 Using proxy: {PROXY}")
else:
    print("⚠️  No proxy — if rate limited, try: python test_scotiabank.py PLATE socks5://host:port")

with sync_playwright() as p:
    launch_opts = {
        "headless": False,
        "args": ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
    }
    if PROXY:
        launch_opts["proxy"] = {"server": PROXY}

    browser = p.chromium.launch(**launch_opts)
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

    # Quick IP check
    try:
        page.goto("https://ifconfig.me/ip", timeout=15000)
        my_ip = page.evaluate("() => document.body.innerText.trim()")
        print(f"🌐 Our IP: {my_ip}")
    except:
        print("Could not check IP")

    # ═══════════════════════════════════════════════════════════
    # PHASE 1: Get to payment page (CAV → cart → continuar)
    # ═══════════════════════════════════════════════════════════
    print(f"═══ Phase 1: Get to payment page (plate={PLATE}) ═══")
    page.goto(URL, timeout=120000, wait_until="domcontentloaded")
    try: page.wait_for_load_state("networkidle", timeout=30000)
    except: pass
    time.sleep(8)

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
    plate_el.click(); plate_el.fill(""); plate_el.type(PLATE, delay=50)
    time.sleep(1)

    page.evaluate("""() => {
        const btns = document.querySelectorAll('button.btn_agregarCarro');
        for (const btn of btns) {
            if (btn.offsetParent === null) continue;
            if ((btn.innerText||'').includes('Agregar al Carro')) { btn.click(); return; }
        }
    }""")
    print("Waiting for iframe to process...")
    time.sleep(8)

    cart = page.evaluate("""() => ({
        certCount: document.querySelectorAll('#carro_tablasListaCertificados tr').length,
        total: document.getElementById('carro_valor_total') ? document.getElementById('carro_valor_total').innerText : '0',
    })""")
    print(f"Cart: {cart}")

    if cart['certCount'] == 0:
        print("❌ Cart empty — rate limited or error")
        page.screenshot(path="scotia_step1_empty.png")
        browser.close()
        exit(1)

    # Fill email
    page.evaluate("""() => {
        const c = document.getElementById('carro_datosMailSolicitanteContainer');
        if (c) c.style.display = 'block';
    }""")
    time.sleep(0.5)
    for fid in ['carro_solicitanteInputEmail', 'carro_solicitanteInputEmailConfirm']:
        page.evaluate(f"""() => {{
            const el = document.getElementById('{fid}');
            if (el) {{ el.value = '{EMAIL}'; el.dispatchEvent(new Event('input', {{bubbles:true}})); el.dispatchEvent(new Event('change', {{bubbles:true}})); }}
        }}""")
    time.sleep(0.5)

    # Click Continuar
    page.evaluate("() => { const btn = document.getElementById('carro_btnContinuar'); if(btn) btn.click(); }")
    print("Clicked Continuar... waiting for payment page")
    time.sleep(5)
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except: pass

    # Check if rate limited
    body_text = page.evaluate("() => document.body.innerText.substring(0,500)")
    if 'excedido' in body_text.lower():
        print("❌ RATE LIMITED: Ha excedido el número de certificados permitidos")
        print("Try a different plate: python test_scotiabank.py XXXX99")
        page.screenshot(path="scotia_rate_limited.png")
        browser.close()
        exit(1)

    # ═══════════════════════════════════════════════════════════
    # PHASE 2: Payment method page — select TGR → Continuar
    # ═══════════════════════════════════════════════════════════
    print(f"\n═══ Phase 2: Payment method page ═══")
    print(f"URL: {page.url}")

    # Select TGR radio (id="1", value="TGR")
    page.evaluate("""() => {
        const r = document.getElementById('1') || document.querySelector('input[value="TGR"]');
        if (r) { r.checked = true; r.click(); r.dispatchEvent(new Event('change', {bubbles:true})); }
    }""")
    print("TGR selected")
    time.sleep(1)

    # Click Continuar
    page.evaluate("""() => {
        const btn = document.getElementById('continuar_btn_button');
        if (btn) btn.click();
    }""")
    print("Clicked Continuar → waiting for TGR bank page (slow external domain)...")
    time.sleep(10)
    try: page.wait_for_load_state("domcontentloaded", timeout=60000)
    except: pass
    time.sleep(10)
    try: page.wait_for_load_state("networkidle", timeout=30000)
    except: pass
    time.sleep(5)

    print(f"Bank page URL: {page.url}")
    try: page.screenshot(path="scotia_step2_banks.png", timeout=15000, full_page=False)
    except: pass
    print("📸 scotia_step2_banks.png")

    # ═══════════════════════════════════════════════════════════
    # PHASE 3: Click Scotiabank (with retry — page loads slowly)
    # ═══════════════════════════════════════════════════════════
    print(f"\n═══ Phase 3: Click Scotiabank ═══")
    scotia_result = None
    for attempt in range(8):
        scotia_result = page.evaluate("""() => {
            const all = document.querySelectorAll('a, button, input, img, div, td, li, label, span');
            for (const el of all) {
                if (el.offsetParent === null) continue;
                const txt = (el.innerText || el.textContent || el.alt || el.title || el.value || '').toLowerCase();
                const src = (el.src || '').toLowerCase();
                if (txt.includes('scotiabank') || txt.includes('scotia') || src.includes('scotiabank') || src.includes('scotia')) {
                    const target = (el.tagName === 'IMG' && el.parentElement && (el.parentElement.tagName === 'A' || el.parentElement.tagName === 'DIV')) ? el.parentElement : el;
                    target.scrollIntoView({block:'center'});
                    target.click();
                    return { clicked: true, tag: target.tagName, id: target.id, text: (target.innerText||target.alt||'').trim().substring(0,60) };
                }
            }
            return { clicked: false };
        }""")
        if scotia_result.get('clicked'):
            print(f"  ✅ Scotiabank clicked on attempt {attempt+1}: {scotia_result}")
            break
        print(f"  Attempt {attempt+1}: not found yet, waiting 5s...")
        time.sleep(5)

    if not scotia_result or not scotia_result.get('clicked'):
        print("❌ Scotiabank NOT found after 8 attempts")
        page.screenshot(path="scotia_not_found.png", full_page=False)
        input("Navigate manually and press Enter...")

    time.sleep(3)

    # ═══════════════════════════════════════════════════════════
    # PHASE 3b: Click "Pagar" button (on the page showing amount)
    # ═══════════════════════════════════════════════════════════
    print(f"\n═══ Phase 3b: Click Pagar ═══")
    print(f"URL: {page.url}")
    
    # Dump page to see what we have
    pagar_info = page.evaluate("""() => ({
        text: document.body.innerText.substring(0, 500),
        buttons: Array.from(document.querySelectorAll('a, button, input[type=submit], input[type=button]'))
            .filter(el => el.offsetParent !== null)
            .map(el => ({ tag: el.tagName, id: el.id, text: (el.innerText||el.value||'').trim().substring(0,50) }))
    })""")
    print(f"Text: {pagar_info['text'][:300]}")
    print(f"Buttons: {json.dumps(pagar_info['buttons'][:10], indent=2)}")
    
    pagar_result = page.evaluate("""() => {
        const all = document.querySelectorAll('a, button, input[type=submit], input[type=button]');
        for (const el of all) {
            if (el.offsetParent === null) continue;
            const txt = (el.innerText || el.value || '').trim().toLowerCase();
            if (txt === 'pagar' || txt.includes('pagar')) {
                el.scrollIntoView({block:'center'});
                el.click();
                return { clicked: true, tag: el.tagName, id: el.id, text: txt.substring(0,50) };
            }
        }
        return { clicked: false };
    }""")
    print(f"Pagar result: {pagar_result}")
    time.sleep(5)
    try: page.wait_for_load_state("domcontentloaded", timeout=30000)
    except: pass
    time.sleep(5)

    print(f"After Pagar URL: {page.url}")
    try: page.screenshot(path="scotia_step3_login.png", timeout=15000, full_page=False)
    except: pass
    print("📸 scotia_step3_login.png")

    # ═══════════════════════════════════════════════════════════
    # PHASE 4: Scotiabank personal login page → click "Haga Click Aquí"
    #   for "Para empresas con Contrato Scotiaweb"
    # ═══════════════════════════════════════════════════════════
    print(f"\n═══ Phase 4: Click 'Haga Click Aquí' (Para Empresas) ═══")

    # Dump page structure first — the page might use frames
    page_info = page.evaluate("""() => ({
        url: location.href,
        text: document.body.innerText.substring(0, 1500),
        links: Array.from(document.querySelectorAll('a')).map(a => ({
            text: (a.innerText||'').trim().substring(0,80),
            href: (a.href||'').substring(0,100),
            visible: a.offsetParent !== null,
        })).filter(l => l.visible && l.text),
        frames: Array.from(document.querySelectorAll('frame, iframe')).map(f => ({
            tag: f.tagName, id: f.id, name: f.name, src: (f.src||'').substring(0,150),
        })),
    })""")
    print(f"URL: {page_info['url']}")
    print(f"Text: {page_info['text'][:500]}")
    print(f"Links: {json.dumps(page_info['links'][:15], indent=2)}")
    print(f"Frames: {json.dumps(page_info['frames'], indent=2)}")

    # Try on main page first
    empresas_clicked = False
    empresas_result = page.evaluate("""() => {
        const links = document.querySelectorAll('a');
        for (const a of links) {
            const txt = (a.innerText || a.textContent || '').trim();
            const txtLow = txt.toLowerCase();
            if (txtLow.includes('haga click') || txtLow.includes('click aquí') || txtLow.includes('click aqui')) {
                a.scrollIntoView({block:'center'});
                a.click();
                return { clicked: true, text: txt.substring(0,80), href: (a.href||'').substring(0,100) };
            }
        }
        return { clicked: false };
    }""")
    print(f"Main page 'Haga Click Aquí': {empresas_result}")
    if empresas_result.get('clicked'):
        empresas_clicked = True

    # If not found, search ALL frames (Scotiabank pages often use framesets)
    if not empresas_clicked:
        print("Not found on main page — searching frames...")
        all_frames = page.frames
        print(f"Total frames: {len(all_frames)}")
        for i, frame in enumerate(all_frames):
            if frame == page.main_frame:
                continue
            try:
                fname = frame.name or f"frame_{i}"
                furl = frame.url[:100] if frame.url else "no-url"
                print(f"  Checking frame '{fname}': {furl}")
                
                # Dump frame content
                fr_text = frame.evaluate("() => document.body ? document.body.innerText.substring(0,300) : 'no body'")
                print(f"    Text: {fr_text[:200]}")
                
                fr_links = frame.evaluate("""() => {
                    return Array.from(document.querySelectorAll('a')).map(a => ({
                        text: (a.innerText||'').trim().substring(0,80),
                        href: (a.href||'').substring(0,100),
                    }));
                }""")
                print(f"    Links: {json.dumps(fr_links[:5], indent=2)}")
                
                fr_result = frame.evaluate("""() => {
                    const links = document.querySelectorAll('a');
                    for (const a of links) {
                        const txt = (a.innerText || a.textContent || '').trim();
                        const txtLow = txt.toLowerCase();
                        if (txtLow.includes('haga click') || txtLow.includes('click aquí') || txtLow.includes('click aqui') || txtLow.includes('empresa')) {
                            a.scrollIntoView({block:'center'});
                            a.click();
                            return { clicked: true, text: txt.substring(0,80), href: (a.href||'').substring(0,100) };
                        }
                    }
                    return { clicked: false };
                }""")
                if fr_result.get('clicked'):
                    print(f"    ✅ Found and clicked in frame '{fname}': {fr_result}")
                    empresas_result = fr_result
                    empresas_clicked = True
                    break
            except Exception as e:
                print(f"    Frame error: {e}")

    if not empresas_clicked:
        print("❌ 'Haga Click Aquí' NOT found anywhere")
        print("Pausing — check the browser and navigate manually")
        input("Press Enter after navigating to Empresas login...")

    print(f"Empresas result: {empresas_result}")
    time.sleep(5)
    try: page.wait_for_load_state("domcontentloaded", timeout=20000)
    except: pass
    time.sleep(3)

    print(f"After Empresas URL: {page.url}")
    try: page.screenshot(path="scotia_step4_empresas_login.png", timeout=15000, full_page=False)
    except: pass
    print("📸 scotia_step4_empresas_login.png")

    # ═══════════════════════════════════════════════════════════
    # PHASE 5: Fill RUT Empresa + Clave → click Ingresar
    # ═══════════════════════════════════════════════════════════
    print(f"\n═══ Phase 5: Login with RUT={RUT_EMPRESA} ═══")

    # The empresas page might also use frames. Dump structure.
    emp_info = page.evaluate("""() => ({
        url: location.href,
        text: document.body.innerText.substring(0, 500),
        inputs: Array.from(document.querySelectorAll('input')).filter(i => i.offsetParent !== null).map(i => ({
            type: i.type, id: i.id, name: i.name, placeholder: i.placeholder,
        })),
        frames: Array.from(document.querySelectorAll('frame, iframe')).map(f => ({
            tag: f.tagName, id: f.id, name: f.name, src: (f.src||'').substring(0,150),
        })),
    })""")
    print(f"URL: {emp_info['url']}")
    print(f"Text: {emp_info['text'][:300]}")
    print(f"Inputs: {json.dumps(emp_info['inputs'][:10], indent=2)}")
    print(f"Frames: {json.dumps(emp_info['frames'], indent=2)}")

    # Try filling on main page first, then frames
    def fill_rut_clave(target):
        """Fill RUT + Clave on a page or frame"""
        return target.evaluate(f"""() => {{
            const inputs = Array.from(document.querySelectorAll('input'));
            let rutField = null;
            let claveField = null;
            
            for (const inp of inputs) {{
                if (inp.offsetParent === null && inp.type !== 'hidden') continue;
                const nm = (inp.name || '').toLowerCase();
                const id = (inp.id || '').toLowerCase();
                const tp = (inp.type || '').toLowerCase();
                
                if (nm.includes('rut') || id.includes('rut')) rutField = inp;
                if (tp === 'password' || nm.includes('clave') || nm.includes('pass') || id.includes('clave') || id.includes('pass')) claveField = inp;
            }}
            
            if (!rutField) rutField = inputs.find(i => (i.offsetParent !== null) && (i.type === 'text' || !i.type));
            if (!claveField) claveField = inputs.find(i => (i.offsetParent !== null) && i.type === 'password');
            
            const result = {{ rutFound: !!rutField, claveFound: !!claveField }};
            
            if (rutField) {{
                rutField.focus();
                rutField.value = '{RUT_EMPRESA}';
                rutField.dispatchEvent(new Event('input', {{bubbles: true}}));
                rutField.dispatchEvent(new Event('change', {{bubbles: true}}));
                result.rutId = rutField.id;
                result.rutName = rutField.name;
            }}
            if (claveField) {{
                claveField.focus();
                claveField.value = '{CLAVE}';
                claveField.dispatchEvent(new Event('input', {{bubbles: true}}));
                claveField.dispatchEvent(new Event('change', {{bubbles: true}}));
                result.claveId = claveField.id;
                result.claveName = claveField.name;
            }}
            
            return result;
        }}""")

    def click_ingresar(target):
        """Click Ingresar button"""
        return target.evaluate("""() => {
            const all = document.querySelectorAll('button, input[type=submit], input[type=button], a, input[type=image]');
            for (const el of all) {
                if (el.offsetParent === null) continue;
                const txt = (el.innerText || el.value || el.alt || '').trim().toLowerCase();
                if (txt.includes('ingresar') || txt.includes('login') || txt.includes('entrar') || txt.includes('enviar')) {
                    el.scrollIntoView({block:'center'});
                    el.click();
                    return { clicked: true, tag: el.tagName, id: el.id, text: txt.substring(0,50) };
                }
            }
            return { clicked: false };
        }""")

    # Try main page
    login_result = fill_rut_clave(page)
    print(f"Main page login fields: {login_result}")
    
    if not login_result.get('rutFound'):
        print("RUT field not found on main page — checking frames...")
        for i, frame in enumerate(page.frames):
            if frame == page.main_frame:
                continue
            try:
                fr_login = fill_rut_clave(frame)
                print(f"  Frame {i} ({frame.name}): {fr_login}")
                if fr_login.get('rutFound'):
                    login_result = fr_login
                    # Click ingresar in same frame
                    time.sleep(1)
                    try: page.screenshot(path="scotia_step5_filled.png", timeout=10000, full_page=False)
                    except: pass
                    ing_result = click_ingresar(frame)
                    print(f"  Ingresar in frame: {ing_result}")
                    break
            except Exception as e:
                print(f"  Frame {i} error: {e}")
    else:
        time.sleep(1)
        try: page.screenshot(path="scotia_step5_filled.png", timeout=10000, full_page=False)
        except: pass
        print("📸 scotia_step5_filled.png")
        
        ing_result = click_ingresar(page)
        print(f"Ingresar click: {ing_result}")

    time.sleep(8)
    try: page.wait_for_load_state("domcontentloaded", timeout=30000)
    except: pass
    time.sleep(3)

    print(f"\nAfter login URL: {page.url}")
    try: page.screenshot(path="scotia_step6_after_login.png", timeout=15000, full_page=False)
    except: pass
    print("📸 scotia_step6_after_login.png")

    # ═══════════════════════════════════════════════════════════
    # PHASE 6: Dump what we see after login
    # ═══════════════════════════════════════════════════════════
    print(f"\n═══ Phase 6: After Scotiabank login ═══")
    
    # Check main page + all frames
    after_login = page.evaluate("""() => ({
        url: location.href,
        title: document.title,
        text: document.body.innerText.substring(0, 2000),
        inputs: Array.from(document.querySelectorAll('input, select, textarea')).filter(el => el.offsetParent !== null).map(el => ({
            tag: el.tagName, type: el.type, id: el.id, name: el.name, value: el.value.substring(0,30),
        })),
        buttons: Array.from(document.querySelectorAll('button, input[type=submit], input[type=button]')).filter(el => el.offsetParent !== null).map(el => ({
            id: el.id, text: (el.innerText||el.value||'').trim().substring(0,50),
        })),
        frames: Array.from(document.querySelectorAll('frame, iframe')).map(f => ({
            id: f.id, name: f.name, src: (f.src||'').substring(0,150),
        })),
    })""")
    print(f"URL: {after_login['url']}")
    print(f"Title: {after_login['title']}")
    print(f"Text:\n{after_login['text'][:1200]}")
    print(f"\nInputs: {json.dumps(after_login['inputs'][:15], indent=2)}")
    print(f"\nButtons: {json.dumps(after_login['buttons'][:10], indent=2)}")
    if after_login.get('frames'):
        print(f"\nFrames: {json.dumps(after_login['frames'], indent=2)}")
        for i, frame in enumerate(page.frames):
            if frame == page.main_frame:
                continue
            try:
                ft = frame.evaluate("() => document.body ? document.body.innerText.substring(0,500) : ''")
                if ft.strip():
                    print(f"\n  Frame {i} ({frame.name}) text: {ft[:300]}")
            except: pass

    print("\n✅ Done! Check screenshots.")
    input("Press Enter to close browser...")
    browser.close()
