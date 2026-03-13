"""
Test 3: After solving CAPTCHA, try EVERY way to click Vehículos
and see which one actually expands the accordion.
"""
from playwright.sync_api import sync_playwright

URL = "https://www.registrocivil.cl/OficinaInternet/"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, slow_mo=300)
    context = browser.new_context(viewport={"width": 1280, "height": 900})
    page = context.new_page()

    print(f"Opening {URL} ...")
    page.goto(URL, timeout=60000, wait_until="domcontentloaded")
    page.wait_for_timeout(2000)

    print("\n👆 SOLVE THE CAPTCHA, then press ENTER...")
    input()
    page.wait_for_timeout(2000)

    # First, check the current state of the arrow
    arrow_state = page.evaluate("""() => {
        const el = document.getElementById('arrowDown_5');
        if (!el) return 'arrowDown_5 NOT FOUND';
        return {
            style: el.getAttribute('style'),
            transform: window.getComputedStyle(el).transform,
            class: el.className
        };
    }""")
    print(f"\nArrow state BEFORE click: {arrow_state}")

    # Also check what event listeners are on title_5
    title5_info = page.evaluate("""() => {
        const el = document.getElementById('title_5');
        if (!el) return 'NOT FOUND';
        return {
            tag: el.tagName,
            id: el.id,
            cls: el.className,
            outerHTML: el.outerHTML.substring(0, 300),
            children: Array.from(el.children).map(c => c.outerHTML.substring(0, 150)),
            style: el.getAttribute('style') || '',
            parentId: el.parentElement ? el.parentElement.id : '',
            parentCls: el.parentElement ? el.parentElement.className : ''
        };
    }""")
    print(f"\ntitle_5 info: {title5_info}")

    # Check what's in the parent container (container_4 from our earlier test)
    container_info = page.evaluate("""() => {
        const el = document.getElementById('container_4');
        if (!el) return 'NOT FOUND';
        return {
            childCount: el.children.length,
            children: Array.from(el.children).map(c => ({
                tag: c.tagName, id: c.id, cls: c.className,
                display: window.getComputedStyle(c).display,
                height: window.getComputedStyle(c).height,
                overflow: window.getComputedStyle(c).overflow,
                outerHTML: c.outerHTML.substring(0, 200)
            }))
        };
    }""")
    print(f"\ncontainer_4 children: {container_info}")

    # ──────────── TRY DIFFERENT CLICK METHODS ────────────
    methods = [
        ("Playwright click #title_5", lambda: page.locator("#title_5").click()),
        ("Playwright click force #title_5", lambda: page.locator("#title_5").click(force=True)),
        ("Playwright click #arrowDown_5", lambda: page.locator("#arrowDown_5").click(force=True)),
        ("Playwright dblclick #title_5", lambda: page.locator("#title_5").dblclick(force=True)),
        ("JS dispatchEvent mousedown+mouseup+click", lambda: page.evaluate("""() => {
            const el = document.getElementById('title_5');
            el.dispatchEvent(new MouseEvent('mousedown', {bubbles: true}));
            el.dispatchEvent(new MouseEvent('mouseup', {bubbles: true}));
            el.dispatchEvent(new MouseEvent('click', {bubbles: true}));
            return 'dispatched';
        }""")),
        ("JS jQuery trigger click", lambda: page.evaluate("""() => {
            const el = document.getElementById('title_5');
            if (typeof jQuery !== 'undefined') {
                jQuery(el).trigger('click');
                return 'jquery click triggered';
            }
            if (typeof $ !== 'undefined') {
                $(el).trigger('click');
                return '$ click triggered';
            }
            return 'no jquery found';
        }""")),
        ("Playwright click on text=Vehículos", lambda: page.locator("text=Vehículos").first.click()),
    ]

    for name, fn in methods:
        # Reset the page first
        page.goto(page.url, timeout=60000, wait_until="domcontentloaded")
        page.wait_for_timeout(3000)

        # Check initial arrow state
        before = page.evaluate("""() => {
            const a = document.getElementById('arrowDown_5');
            return a ? a.getAttribute('style') : 'not found';
        }""")

        print(f"\n{'='*50}")
        print(f"TRYING: {name}")
        print(f"  Arrow BEFORE: {before}")

        try:
            result = fn()
            print(f"  Result: {result}")
        except Exception as e:
            print(f"  ERROR: {e}")

        page.wait_for_timeout(2000)

        # Check arrow state after
        after = page.evaluate("""() => {
            const a = document.getElementById('arrowDown_5');
            return a ? a.getAttribute('style') : 'not found';
        }""")
        print(f"  Arrow AFTER: {after}")

        # Check if sub-items are now visible
        visible = page.evaluate("""() => {
            const tds = document.querySelectorAll('td');
            for (const td of tds) {
                const txt = (td.innerText || '').toLowerCase();
                if (txt.includes('anotaciones vigentes')) {
                    return {found: true, visible: td.offsetParent !== null, display: window.getComputedStyle(td).display};
                }
            }
            return {found: false};
        }""")
        print(f"  CAV TD visible: {visible}")

        if after != before:
            print(f"  🎉 ARROW CHANGED! This method works!")
        if visible.get('visible'):
            print(f"  🎉🎉 CAV TD IS VISIBLE! This is the right method!")

    print("\nDone!")
    browser.close()
