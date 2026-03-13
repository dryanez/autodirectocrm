"""
Standalone test: navigate registrocivil.cl and dump all elements at each step.
Run with: .venv/bin/python test_nav.py
This does NOT solve the CAPTCHA automatically - it opens the browser visibly
so you can solve it manually, then it dumps what's on screen.
"""
import time
import sys
from playwright.sync_api import sync_playwright

URL = "https://www.registrocivil.cl/OficinaInternet/"

def dump_all(page, label):
    print(f"\n{'='*60}")
    print(f"DUMP: {label}")
    print(f"{'='*60}")
    print(f"URL: {page.url}")
    print(f"Title: {page.title()}")
    try:
        links = page.evaluate("""() => {
            const results = [];
            document.querySelectorAll('*').forEach(el => {
                const txt = (el.innerText || '').trim();
                if (txt && txt.length > 0 && txt.length < 120 && el.children.length === 0) {
                    const rect = el.getBoundingClientRect();
                    results.push({
                        tag: el.tagName,
                        txt: txt,
                        id: el.id || '',
                        cls: (el.className || '').toString().substring(0,40),
                        href: el.href || '',
                        visible: rect.width > 0 && rect.height > 0
                    });
                }
            });
            return results;
        }""")
        print(f"\nALL LEAF TEXT ELEMENTS ({len(links)} total):")
        for el in links:
            vis = "✅" if el['visible'] else "⬜"
            print(f"  {vis} <{el['tag']}> '{el['txt']}' id='{el['id']}' class='{el['cls']}' href='{el['href']}'")
    except Exception as e:
        print(f"Could not dump elements: {e}")
        # Fallback: just print body text
        try:
            txt = page.inner_text("body")
            for line in txt.splitlines():
                if line.strip():
                    print(f"  | {line.strip()}")
        except Exception:
            pass

with sync_playwright() as p:
    # Launch VISIBLE browser so you can manually solve CAPTCHA
    browser = p.chromium.launch(headless=False, slow_mo=500)
    context = browser.new_context(viewport={"width": 1280, "height": 900})
    page = context.new_page()

    print(f"Opening {URL} ...")
    page.goto(URL, timeout=60000, wait_until="domcontentloaded")
    page.wait_for_timeout(2000)

    dump_all(page, "INITIAL PAGE LOAD")

    print("\n" + "="*60)
    print("👆 SOLVE THE CAPTCHA IN THE BROWSER WINDOW")
    print("   Then press ENTER here to continue...")
    print("="*60)
    input()

    page.wait_for_timeout(2000)
    dump_all(page, "AFTER CAPTCHA SOLVED")

    print("\n👆 Now click 'Vehículos' in the browser, then press ENTER...")
    input()

    page.wait_for_timeout(2000)
    dump_all(page, "AFTER CLICKING VEHICULOS")

    print("\n👆 Now click 'Certificado de Anotaciones Vigentes', then press ENTER...")
    input()

    page.wait_for_timeout(2000)
    dump_all(page, "AFTER CLICKING CERTIFICADO")

    print("\nDone! Check the output above for exact element text/ids.")
    browser.close()
