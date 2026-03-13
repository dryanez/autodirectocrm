"""
Payment flow exploration:
Step 1: Load page → CAV → add to cart → fill email → Continuar
Step 2: Payment selection → select TGR → Continuar
Step 3: Bank selection → find Scotiabank → click
Step 4: Scotiabank Empresas → click
Step 5: See what's next and dump the page
"""
import json, time
from playwright.sync_api import sync_playwright

URL = "https://www.registrocivil.cl/OficinaInternet/"
PLATE = "WG9755"
EMAIL = "felipe@autodirecto.cl"

# Scotiabank Empresas credentials
RUT_EMPRESA = "783557177"
RUT_PERSONA = "188424430"
CLAVE = "Comoestas01"

with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=False,  # HEADED so we can see what happens
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

    # ── Steps 1-5: Load → expand → check → plate → agregar ──
    print("═══ Steps 1-5: Setup ═══")
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

    # Click Agregar al Carro
    page.evaluate("""() => {
        const btns = document.querySelectorAll('button.btn_agregarCarro');
        for (const btn of btns) {
            if (btn.offsetParent === null) continue;
            if ((btn.innerText||'').includes('Agregar al Carro')) { btn.click(); return; }
        }
    }""")
    print("Waiting for iframe to process...")
    time.sleep(8)

    # Check cart
    cart = page.evaluate("""() => ({
        certCount: document.querySelectorAll('#carro_tablasListaCertificados tr').length,
        total: document.getElementById('carro_valor_total') ? document.getElementById('carro_valor_total').innerText : '0',
    })""")
    print(f"Cart: {cart}")

    if cart['certCount'] == 0:
        print("❌ Cart empty — rate limited or error. Check screenshot.")
        page.screenshot(path="pay_step1_empty.png")
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

    # Click Continuar (cart → payment method)
    page.evaluate("() => { const btn = document.getElementById('carro_btnContinuar'); if(btn) btn.click(); }")
    print("Clicked Continuar (cart)... waiting for payment page")
    time.sleep(5)
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except: pass

    page.screenshot(path="pay_step2_payment_method.png")
    print(f"📸 pay_step2_payment_method.png | URL: {page.url}")

    # ── Step 2: Payment method page — inspect ──
    print("\n═══ Step 2: Payment method page ═══")
    pay_content = page.evaluate("""() => ({
        url: location.href,
        title: document.title,
        text: document.body.innerText.substring(0, 1000),
        radios: Array.from(document.querySelectorAll('input[type=radio]')).map(r => ({
            id: r.id, name: r.name, value: r.value,
            checked: r.checked, visible: r.offsetParent !== null,
            label: r.closest('label') ? r.closest('label').innerText.trim().substring(0,60) :
                   (r.parentElement ? r.parentElement.innerText.trim().substring(0,60) : ''),
        })),
        buttons: Array.from(document.querySelectorAll('button, input[type=submit], a.btn, .btn')).map(el => ({
            id: el.id, text: (el.innerText||el.value||'').trim().substring(0,50),
            visible: el.offsetParent !== null,
        })).filter(b => b.visible),
    })""")
    print(f"URL: {pay_content['url']}")
    print(f"Text:\n{pay_content['text'][:500]}")
    print(f"Radios: {json.dumps(pay_content['radios'], indent=2)}")
    print(f"Buttons: {json.dumps(pay_content['buttons'], indent=2)}")

    # ── Select TGR radio ──
    print("\n═══ Selecting TGR ═══")
    tgr_selected = page.evaluate("""() => {
        const radios = document.querySelectorAll('input[type=radio]');
        for (const r of radios) {
            const ctx = (r.closest('label') || r.parentElement || r);
            const txt = ctx.innerText || ctx.textContent || '';
            if (txt.toLowerCase().includes('tgr') || txt.toLowerCase().includes('tesorerí') || txt.toLowerCase().includes('tesorer')) {
                r.checked = true;
                r.dispatchEvent(new Event('change', {bubbles: true}));
                r.dispatchEvent(new Event('click', {bubbles: true}));
                r.click();
                return { selected: true, value: r.value, id: r.id, label: txt.trim().substring(0,80) };
            }
        }
        // fallback: first radio
        const first = document.querySelector('input[type=radio]');
        if (first) {
            first.checked = true;
            first.click();
            return { selected: true, value: first.value, id: first.id, label: 'first radio (fallback)' };
        }
        return { selected: false };
    }""")
    print(f"TGR selection: {tgr_selected}")
    time.sleep(1)
    page.screenshot(path="pay_step2b_tgr_selected.png")
    print("📸 pay_step2b_tgr_selected.png")

    # ── Click Continuar on payment method page ──
    print("\n═══ Clicking Continuar (payment method) ═══")
    continuar_clicked = page.evaluate("""() => {
        // Look for Continuar button
        const btns = document.querySelectorAll('button, input[type=submit], input[type=button], a');
        for (const btn of btns) {
            if (btn.offsetParent === null) continue;
            const txt = (btn.innerText || btn.value || '').trim().toLowerCase();
            if (txt.includes('continuar') || txt.includes('siguiente') || txt.includes('pagar')) {
                btn.click();
                return { clicked: true, text: (btn.innerText||btn.value||'').trim().substring(0,50), id: btn.id };
            }
        }
        return { clicked: false };
    }""")
    print(f"Continuar clicked: {continuar_clicked}")
    print("Waiting for bank selection page (TGR external page — takes a while)...")
    time.sleep(10)
    try: page.wait_for_load_state("domcontentloaded", timeout=60000)
    except: pass
    # Extra wait — TGR page loads bank logos/images slowly
    time.sleep(10)
    try: page.wait_for_load_state("networkidle", timeout=30000)
    except: pass
    time.sleep(5)

    print(f"Bank page URL: {page.url}")
    try:
        page.screenshot(path="pay_step3_bank_select.png", timeout=15000, full_page=False)
    except Exception as e:
        print(f"Screenshot failed: {e}")
    print(f"📸 pay_step3_bank_select.png")

    # ── Step 3: Bank selection page ──
    print("\n═══ Step 3: Bank selection page ═══")
    bank_content = page.evaluate("""() => ({
        url: location.href,
        title: document.title,
        text: document.body.innerText.substring(0, 2000),
        html: document.body.innerHTML.substring(0, 5000),
        links: Array.from(document.querySelectorAll('a, button, input[type=submit]')).map(el => ({
            id: el.id, text: (el.innerText||el.value||'').trim().substring(0,60),
            href: el.href || '', visible: el.offsetParent !== null,
        })).filter(l => l.visible && l.text),
        images: Array.from(document.querySelectorAll('img')).map(img => ({
            src: img.src.substring(0,120), alt: img.alt, title: img.title,
            visible: img.offsetParent !== null,
            parentTag: img.parentElement ? img.parentElement.tagName : '',
            parentHref: img.parentElement && img.parentElement.href ? img.parentElement.href : '',
        })).filter(i => i.visible),
    })""")
    print(f"URL: {bank_content['url']}")
    print(f"Text:\n{bank_content['text'][:800]}")
    print(f"\nLinks/Buttons: {json.dumps(bank_content['links'][:20], indent=2)}")
    print(f"\nImages: {json.dumps(bank_content['images'][:20], indent=2)}")

    # ── Select Scotiabank (3rd bank in first row) ──
    # Flow from screenshots:
    # 1. Bank grid → click Scotiabank logo/card
    # 2. Page shows "Pagar" button on left → click Pagar
    # 3. Scotiabank login page → click "Para empresas con Contrato Scotiaweb Haga Click Aquí"
    print("\n═══ Looking for Scotiabank (with retry) ═══")

    # Retry loop — bank images may not be loaded yet
    scotia_result = None
    for attempt in range(5):
        scotia_result = page.evaluate("""() => {
            // Search all clickable elements and images for 'scotiabank' / 'scotia'
            const all = document.querySelectorAll('a, button, input, img, div, td, li, label, span');
            for (const el of all) {
                if (el.offsetParent === null) continue;
                const txt = (el.innerText || el.textContent || el.alt || el.title || el.value || '').toLowerCase();
                const src = (el.src || '').toLowerCase();
                if (txt.includes('scotiabank') || txt.includes('scotia') || src.includes('scotiabank') || src.includes('scotia')) {
                    // If it's an img, click the parent <a> or the img itself
                    const target = (el.tagName === 'IMG' && el.parentElement && el.parentElement.tagName === 'A') ? el.parentElement : el;
                    target.scrollIntoView({block:'center'});
                    target.click();
                    return { clicked: true, tag: target.tagName, id: target.id, class: target.className, text: (target.innerText||target.alt||'').trim().substring(0,60), src: (el.src||'').substring(0,100) };
                }
            }
            return { clicked: false, note: 'scotiabank not found' };
        }""")
        print(f"  Attempt {attempt+1}: {scotia_result}")
        if scotia_result.get('clicked'):
            break
        print(f"  Not found yet — waiting 5s...")
        time.sleep(5)

    print(f"Scotiabank click result: {scotia_result}")

    if not scotia_result.get('clicked'):
        print("⚠️ Scotiabank not found — dumping full page HTML")
        with open("pay_step3_bank_html.txt", "w") as f:
            f.write(bank_content['html'])
        print("Saved pay_step3_bank_html.txt")
        input("Press Enter to continue manually...")
    else:
        time.sleep(4)
        try: page.wait_for_load_state("domcontentloaded", timeout=20000)
        except: pass
        time.sleep(2)

        try: page.screenshot(path="pay_step4_after_scotia.png", timeout=15000, full_page=False)
        except Exception as e: print(f"Screenshot err: {e}")
        print(f"📸 pay_step4_after_scotia.png | URL: {page.url}")

        # ── Step 4: After clicking Scotiabank — expect a "Pagar" button ──
        print("\n═══ Step 4: After Scotiabank click — looking for Pagar button ═══")
        step4 = page.evaluate("""() => ({
            url: location.href,
            text: document.body.innerText.substring(0, 1000),
            links: Array.from(document.querySelectorAll('a, button, input[type=submit], input[type=button]')).map(el => ({
                id: el.id, text: (el.innerText||el.value||'').trim().substring(0,60),
                href: el.href || '', visible: el.offsetParent !== null,
            })).filter(l => l.visible),
        })""")
        print(f"URL: {step4['url']}")
        print(f"Text:\n{step4['text'][:500]}")
        print(f"\nButtons: {json.dumps(step4['links'][:15], indent=2)}")

        # ── Click "Pagar" button ──
        print("\n═══ Clicking Pagar ═══")
        pagar_result = page.evaluate("""() => {
            const all = document.querySelectorAll('a, button, input[type=submit], input[type=button]');
            for (const el of all) {
                if (el.offsetParent === null) continue;
                const txt = (el.innerText || el.value || '').trim().toLowerCase();
                if (txt.includes('pagar') || txt === 'pagar') {
                    el.scrollIntoView({block:'center'});
                    el.click();
                    return { clicked: true, tag: el.tagName, id: el.id, text: txt.substring(0,50) };
                }
            }
            return { clicked: false, note: 'Pagar button not found' };
        }""")
        print(f"Pagar click: {pagar_result}")
        time.sleep(5)
        try: page.wait_for_load_state("domcontentloaded", timeout=20000)
        except: pass
        time.sleep(3)

        try: page.screenshot(path="pay_step5_scotiabank_login.png", timeout=15000, full_page=False)
        except Exception as e: print(f"Screenshot err: {e}")
        print(f"📸 pay_step5_scotiabank_login.png | URL: {page.url}")

        # ── Step 5: Scotiabank login page — click "Para empresas" link ──
        print("\n═══ Step 5: Scotiabank login page ═══")
        step5 = page.evaluate("""() => ({
            url: location.href,
            title: document.title,
            text: document.body.innerText.substring(0, 1000),
            links: Array.from(document.querySelectorAll('a, button, input[type=submit]')).map(el => ({
                id: el.id, text: (el.innerText||el.value||'').trim().substring(0,80),
                href: el.href || '', visible: el.offsetParent !== null,
            })).filter(l => l.visible),
        })""")
        print(f"URL: {step5['url']}")
        print(f"Text:\n{step5['text'][:600]}")
        print(f"\nLinks: {json.dumps(step5['links'][:20], indent=2)}")

        # ── Click "Para empresas con Contrato Scotiaweb Haga Click Aquí" ──
        # The personal login page has RUT + Clave, and at the bottom a link:
        # "Para empresas con Contrato Scotiaweb Haga Click Aquí"
        # We MUST click specifically the <a> with "Haga Click Aquí" — NOT a parent div
        # The page might also have frames
        print("\n═══ Step 5b: Checking for frames on Scotiabank page ═══")
        frames_info = page.evaluate("""() => ({
            frames: Array.from(document.querySelectorAll('frame, iframe')).map(f => ({
                tag: f.tagName, id: f.id, name: f.name, src: f.src.substring(0,150),
            })),
            framesets: document.querySelectorAll('frameset').length,
        })""")
        print(f"Frames: {json.dumps(frames_info, indent=2)}")

        print("\n═══ Clicking 'Haga Click Aquí' (Para Empresas) ═══")
        # First try on main page, then in each frame
        empresas_result = page.evaluate("""() => {
            // ONLY look at <a> tags to find the specific "Haga Click Aquí" link
            const links = document.querySelectorAll('a');
            for (const a of links) {
                const txt = (a.innerText || a.textContent || '').trim().toLowerCase();
                if (txt.includes('haga click') || txt.includes('click aquí') || txt.includes('click aqui')) {
                    a.scrollIntoView({block:'center'});
                    a.click();
                    return { clicked: true, tag: 'A', id: a.id, href: a.href, text: txt.substring(0,80) };
                }
            }
            // Fallback: look for any link mentioning "empresa" but only <a> tags
            for (const a of links) {
                const txt = (a.innerText || a.textContent || '').trim().toLowerCase();
                if (txt.includes('empresa')) {
                    a.scrollIntoView({block:'center'});
                    a.click();
                    return { clicked: true, tag: 'A', id: a.id, href: a.href, text: txt.substring(0,80) };
                }
            }
            return { clicked: false, note: 'Haga Click Aqui link not found on main page' };
        }""")
        print(f"Empresas click (main page): {empresas_result}")

        # If not found on main page, try inside frames
        if not empresas_result.get('clicked'):
            print("Not found on main page — checking frames...")
            all_frames = page.frames
            print(f"Total frames: {len(all_frames)}")
            for i, frame in enumerate(all_frames):
                if frame == page.main_frame:
                    continue
                try:
                    frame_url = frame.url
                    print(f"  Frame {i}: {frame_url[:100]}")
                    fr_result = frame.evaluate("""() => {
                        const links = document.querySelectorAll('a');
                        for (const a of links) {
                            const txt = (a.innerText || a.textContent || '').trim().toLowerCase();
                            if (txt.includes('haga click') || txt.includes('click aquí') || txt.includes('click aqui') || txt.includes('empresa')) {
                                a.scrollIntoView({block:'center'});
                                a.click();
                                return { clicked: true, tag: 'A', id: a.id, href: a.href, text: txt.substring(0,80) };
                            }
                        }
                        return { clicked: false };
                    }""")
                    print(f"    Frame result: {fr_result}")
                    if fr_result.get('clicked'):
                        empresas_result = fr_result
                        break
                except Exception as e:
                    print(f"    Frame {i} error: {e}")

        print(f"Final empresas result: {empresas_result}")
        time.sleep(5)
        try: page.wait_for_load_state("domcontentloaded", timeout=20000)
        except: pass
        time.sleep(3)

        try: page.screenshot(path="pay_step6_after_empresas.png", timeout=15000, full_page=False)
        except Exception as e: print(f"Screenshot err: {e}")
        print(f"📸 pay_step6_after_empresas.png | URL: {page.url}")

        # ── Step 6: Scotiabank Empresas login — fill RUT + Clave ──
        print("\n═══ Step 6: After Scotiabank Empresas — Login page ═══")
        final = page.evaluate("""() => ({
            url: location.href,
            title: document.title,
            text: document.body.innerText.substring(0, 3000),
            inputs: Array.from(document.querySelectorAll('input, select, textarea')).filter(el => el.offsetParent !== null).map(el => ({
                tag: el.tagName, type: el.type, id: el.id, name: el.name,
                placeholder: el.placeholder||'', value: el.value.substring(0,50),
            })),
            buttons: Array.from(document.querySelectorAll('button, input[type=submit], input[type=button]')).filter(el => el.offsetParent !== null).map(el => ({
                id: el.id, text: (el.innerText||el.value||'').trim().substring(0,50),
                tag: el.tagName, type: el.type,
            })),
            iframes: Array.from(document.querySelectorAll('iframe')).map(f => ({
                id: f.id, name: f.name, src: f.src.substring(0,100),
            })),
        })""")
        print(f"URL: {final['url']}")
        print(f"Title: {final['title']}")
        print(f"Text:\n{final['text'][:1000]}")
        print(f"\nInputs: {json.dumps(final['inputs'], indent=2)}")
        print(f"\nButtons: {json.dumps(final['buttons'], indent=2)}")
        print(f"\nIframes: {json.dumps(final.get('iframes', []), indent=2)}")

        # ── Fill RUT Empresa + Clave ──
        print(f"\n═══ Filling RUT Empresa ({RUT_EMPRESA}) + Clave ═══")
        
        # The Scotiabank Empresas page might have the form in a frame or direct
        # Try to find and fill RUT/Clave fields
        login_result = page.evaluate(f"""() => {{
            // Look for RUT input — could be by name, id, or label
            const inputs = Array.from(document.querySelectorAll('input'));
            let rutField = null;
            let claveField = null;
            
            for (const inp of inputs) {{
                if (inp.offsetParent === null) continue;
                const nm = (inp.name || '').toLowerCase();
                const id = (inp.id || '').toLowerCase();
                const ph = (inp.placeholder || '').toLowerCase();
                const tp = (inp.type || '').toLowerCase();
                
                // Find RUT field
                if (nm.includes('rut') || id.includes('rut') || ph.includes('rut')) {{
                    rutField = inp;
                }}
                // Find Clave/password field
                if (tp === 'password' || nm.includes('clave') || nm.includes('pass') || id.includes('clave') || id.includes('pass')) {{
                    claveField = inp;
                }}
            }}
            
            // Fallback: first text input = RUT, first password = Clave
            if (!rutField) {{
                rutField = inputs.find(i => i.offsetParent !== null && (i.type === 'text' || !i.type));
            }}
            if (!claveField) {{
                claveField = inputs.find(i => i.offsetParent !== null && i.type === 'password');
            }}
            
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
        print(f"Login fields: {login_result}")
        time.sleep(1)

        try: page.screenshot(path="pay_step7_rut_filled.png", timeout=15000, full_page=False)
        except Exception as e: print(f"Screenshot err: {e}")
        print("📸 pay_step7_rut_filled.png")

        # ── Click "Ingresar" button ──
        print("\n═══ Clicking Ingresar ═══")
        ingresar_result = page.evaluate("""() => {
            const all = document.querySelectorAll('button, input[type=submit], input[type=button], a');
            for (const el of all) {
                if (el.offsetParent === null) continue;
                const txt = (el.innerText || el.value || '').trim().toLowerCase();
                if (txt.includes('ingresar') || txt.includes('login') || txt.includes('entrar') || txt.includes('enviar')) {
                    el.scrollIntoView({block:'center'});
                    el.click();
                    return { clicked: true, tag: el.tagName, id: el.id, text: txt.substring(0,50) };
                }
            }
            return { clicked: false, note: 'Ingresar button not found' };
        }""")
        print(f"Ingresar click: {ingresar_result}")
        time.sleep(8)
        try: page.wait_for_load_state("domcontentloaded", timeout=30000)
        except: pass
        time.sleep(3)

        try: page.screenshot(path="pay_step8_after_login.png", timeout=15000, full_page=False)
        except Exception as e: print(f"Screenshot err: {e}")
        print(f"📸 pay_step8_after_login.png | URL: {page.url}")

        # ── Step 7: What comes after login? ──
        print("\n═══ Step 7: After Scotiabank login ═══")
        after_login = page.evaluate("""() => ({
            url: location.href,
            title: document.title,
            text: document.body.innerText.substring(0, 3000),
            inputs: Array.from(document.querySelectorAll('input, select, textarea')).filter(el => el.offsetParent !== null).map(el => ({
                tag: el.tagName, type: el.type, id: el.id, name: el.name,
                placeholder: el.placeholder||'', value: el.value.substring(0,50),
            })),
            buttons: Array.from(document.querySelectorAll('button, input[type=submit], input[type=button]')).filter(el => el.offsetParent !== null).map(el => ({
                id: el.id, text: (el.innerText||el.value||'').trim().substring(0,50),
            })),
            iframes: Array.from(document.querySelectorAll('iframe')).map(f => ({
                id: f.id, name: f.name, src: f.src.substring(0,100),
            })),
        })""")
        print(f"URL: {after_login['url']}")
        print(f"Title: {after_login['title']}")
        print(f"Text:\n{after_login['text'][:1500]}")
        print(f"\nInputs: {json.dumps(after_login['inputs'], indent=2)}")
        print(f"\nButtons: {json.dumps(after_login['buttons'], indent=2)}")
        if after_login.get('iframes'):
            print(f"\nIframes: {json.dumps(after_login['iframes'], indent=2)}")

    print("\n✅ Done! Check screenshots.")
    input("Press Enter to close browser...")
    browser.close()
