"""Quick check: what's on the page after loading?"""
import json, time
from playwright.sync_api import sync_playwright

URL = "https://www.registrocivil.cl/OficinaInternet/"

with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
    )
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
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

    print(f"Opening {URL}...")
    resp = page.goto(URL, timeout=60000, wait_until="domcontentloaded")
    print(f"Status: {resp.status}")
    
    # Wait for full load
    time.sleep(5)
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except:
        pass

    print(f"Final URL: {page.url}")
    print(f"Title: {page.title()}")
    
    # Dump element IDs
    info = page.evaluate("""() => {
        const allIds = Array.from(document.querySelectorAll('[id]')).map(e => ({
            id: e.id, tag: e.tagName, cls: e.className.toString().substring(0,50)
        }));
        const titleGrupos = Array.from(document.querySelectorAll('.titleGrupos')).map(e => ({
            id: e.id, text: e.innerText.trim().substring(0,50)
        }));
        return {
            url: location.href,
            elementCount: document.querySelectorAll('*').length,
            idCount: allIds.length,
            ids: allIds.slice(0, 60),
            titleGrupos: titleGrupos,
            bodyText: document.body.innerText.substring(0, 800),
        };
    }""")
    
    print(f"\nElement count: {info['elementCount']}")
    print(f"ID count: {info['idCount']}")
    print(f"\ntitleGrupos: {json.dumps(info['titleGrupos'], indent=2)}")
    print(f"\nFirst 60 IDs:")
    for el in info['ids']:
        print(f"  #{el['id']} ({el['tag']}.{el['cls'][:30]})")
    print(f"\nBody text preview:\n{info['bodyText']}")
    
    page.screenshot(path="page_check.png")
    print("\n📸 page_check.png")
    
    browser.close()
