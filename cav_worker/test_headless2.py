"""
Test: Does registrocivil.cl close the page in headless mode?
Test with anti-bot detection evasion.
"""
import json, time
from playwright.sync_api import sync_playwright

URL = "https://www.registrocivil.cl/OficinaInternet/"

with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox", 
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-blink-features=AutomationControlled",  # KEY: hide automation
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

    # Anti-bot: override navigator.webdriver before any page loads
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        // Override Permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
        );
        // Override plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        Object.defineProperty(navigator, 'languages', {
            get: () => ['es-CL', 'es', 'en']
        });
        // Chrome runtime
        window.chrome = { runtime: {} };
    """)

    page = context.new_page()
    
    # Listen for page close/crash events
    page.on("close", lambda: print("⚠️ PAGE WAS CLOSED!"))
    page.on("crash", lambda: print("💥 PAGE CRASHED!"))

    print(f"Opening {URL} in HEADLESS mode with anti-detection...")
    try:
        resp = page.goto(URL, timeout=60000, wait_until="domcontentloaded")
        print(f"Response status: {resp.status if resp else 'None'}")
    except Exception as e:
        print(f"Navigation error: {e}")
        browser.close()
        exit(1)
    
    time.sleep(5)

    # Try to interact
    try:
        url = page.url
        print(f"Current URL: {url}")
        
        title = page.title()
        print(f"Page title: {title}")
        
        webdriver = page.evaluate("() => navigator.webdriver")
        print(f"navigator.webdriver: {webdriver}")
        
        content = page.content()
        has_captcha = "código de la imagen" in content.lower()
        print(f"Has CAPTCHA: {has_captcha}")
        print(f"Content length: {len(content)}")
        
        # Check for bot detection
        if "robot" in content.lower() or "blocked" in content.lower() or "access denied" in content.lower():
            print("⚠️ BOT DETECTED!")
            print(content[:500])
        
        page.screenshot(path="headless_antibot.png")
        print("📸 Screenshot saved: headless_antibot.png")
        
        # Check page structure
        page_info = page.evaluate("""() => ({
            url: location.href,
            title: document.title,
            bodyLength: document.body.innerHTML.length,
            bodyText: document.body.innerText.substring(0, 500),
            hasTitle5: !!document.getElementById('title_5'),
            hasDivLista5: !!document.getElementById('divLista_5'),
            hasJQuery: typeof jQuery !== 'undefined',
            elementCount: document.querySelectorAll('*').length,
        })""")
        print(f"\nPage info:\n{json.dumps(page_info, indent=2)}")
        
        if page_info.get("hasTitle5"):
            print("\n✅ title_5 exists! Trying click...")
            page.locator("#title_5").scroll_into_view_if_needed()
            page.locator("#title_5").click()
            time.sleep(2)
            
            div_state = page.evaluate("""() => {
                const d = document.getElementById('divLista_5');
                if (!d) return 'NOT FOUND';
                return {
                    display: window.getComputedStyle(d).display,
                    childCount: d.children.length,
                };
            }""")
            print(f"divLista_5 after click: {json.dumps(div_state, indent=2)}")
            page.screenshot(path="headless_after_click.png")
            print("📸 Screenshot saved: headless_after_click.png")
        
    except Exception as e:
        print(f"Interaction error: {e}")
        import traceback
        traceback.print_exc()
    
    browser.close()
    print("\n✅ Test complete.")
