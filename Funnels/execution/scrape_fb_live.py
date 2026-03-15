"""
scrape_fb_live.py — Facebook Marketplace scraper for Funnels dashboard.
============================================================================
Based on the PROVEN Marketing/Funnels/execution/scrape_fb_marketplace.py
that was working perfectly.  Uses sync Playwright + stealth, simple
page.fill() / page.click() — no over-engineering.

Outputs Apify-compatible JSON that the Funnels dashboard ingests directly.

Usage:
  python scrape_fb_live.py [--output /path/to/output.json] [--scrolls 30]
"""

import json
import os
import random
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime

# --- Load .env if present ---
_env_file = Path(__file__).resolve().parent.parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# --- Config ---
DEFAULT_TARGET_URL = (
    "https://www.facebook.com/marketplace/106647439372422/search/"
    "?minPrice=4000000&query=Vehicles&exact=false&radius=20"
)
MAX_SCROLLS          = 2000
TARGET_LEADS         = 500
MIN_PRICE_CLP        = 4_000_000
DEFAULT_SCROLL_STEPS = MAX_SCROLLS
SCROLL_PX            = 1200
SCROLL_DELAY_MIN     = 2.0
SCROLL_DELAY_MAX     = 4.0

# Credentials - dr.felipeyanez@gmail.com account (cookies are from this account)
FB_EMAIL    = os.environ.get("FB_EMAIL", "dr.felipeyanez@gmail.com")
FB_PASSWORD = os.environ.get("FB_PASSWORD", "Chile202601@")

COOKIES_FILE = Path(__file__).resolve().parent.parent / "fb_cookies.json"

# --- V Region communes ---
V_REGION_COMMUNES = {
    "viña del mar", "vina del mar", "concón", "concon",
    "valparaíso", "valparaiso", "quilpué", "quilpue",
    "villa alemana", "quintero", "limache", "olmué", "olmue",
    "casablanca", "quillota", "la cruz", "puchuncaví", "puchuncavi",
    "calera", "nogales", "hijuelas", "algarrobo",
    "el quisco", "el tabo", "san antonio", "cartagena", "santo domingo",
}

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

# --- Global store ---
vehicles = {}
qualifying_count = 0
graphql_count = 0


def human_delay(lo=2.0, hi=4.0):
    time.sleep(random.uniform(lo, hi))


def _is_v_region(text):
    if not text:
        return False
    loc = text.lower().strip()
    return any(commune in loc for commune in V_REGION_COMMUNES)


def _parse_price_clp(amount_str, formatted):
    if amount_str and amount_str != "0":
        try:
            return int(amount_str)
        except (ValueError, TypeError):
            pass
    if not formatted:
        return 0
    digits = "".join(ch for ch in formatted if ch.isdigit())
    return int(digits) if digits else 0


def parse_feed_units(data):
    global qualifying_count
    try:
        edges = data["data"]["marketplace_search"]["feed_units"]["edges"]
    except (KeyError, TypeError):
        return
    for edge in edges:
        try:
            listing = edge["node"]["listing"]
            lid = str(listing.get("id", ""))
            if not lid or lid in vehicles:
                continue
            title = listing.get("marketplace_listing_title") or listing.get("custom_title", "")
            if not title:
                continue
            price_info = listing.get("listing_price") or {}
            loc = listing.get("location") or {}
            rev = loc.get("reverse_geocode") or {}
            city_page = rev.get("city_page") or {}
            location_text = (
                city_page.get("display_name")
                or ", ".join(p for p in [rev.get("city", ""), rev.get("state", "")] if p)
                or ""
            )
            price_clp = _parse_price_clp(
                price_info.get("amount", "0"),
                price_info.get("formatted_amount", ""),
            )
            is_v = _is_v_region(location_text)
            qualifies = is_v and price_clp >= MIN_PRICE_CLP

            subtitles = listing.get("custom_sub_titles_with_rendering_flags") or []
            custom_sub_titles = [{"subtitle": s.get("subtitle", "")} for s in subtitles]
            primary_photo = listing.get("primary_listing_photo") or {}
            photo_url = (primary_photo.get("image") or {}).get("uri", "")
            seller = listing.get("marketplace_listing_seller") or {}

            vehicles[lid] = {
                "id": lid,
                "itemUrl": "https://www.facebook.com/marketplace/item/%s/" % lid,
                "listingTitle": title,
                "listingPrice": {
                    "amount": price_info.get("amount", "0"),
                    "formatted_amount": price_info.get("formatted_amount", ""),
                    "currency": "CLP",
                },
                "locationText": {"text": location_text},
                "customSubTitlesWithRenderingFlags": custom_sub_titles,
                "primaryListingPhoto": {"photo_image_url": photo_url},
                "isSold": listing.get("is_sold", False),
                "sellerName": seller.get("name", ""),
                "sellerId": seller.get("id", ""),
                "v_region": is_v,
                "qualifies": qualifies,
                "source": "scraper",
                "scraped_at": datetime.now().isoformat(),
            }
            if qualifies:
                qualifying_count += 1
                tag = "Q%3d/%d" % (qualifying_count, TARGET_LEADS)
            else:
                tag = "skip"
            print(
                "  %s [%4d] %-45s | %-18s | %s" % (
                    tag, len(vehicles), title[:45],
                    price_info.get("formatted_amount", "N/A"),
                    location_text
                ),
                file=sys.stderr,
            )
        except Exception:
            continue


def handle_response(response):
    global graphql_count
    if "/api/graphql" not in response.url:
        return
    graphql_count += 1
    try:
        text = response.text()
        for line in text.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                parse_feed_units(json.loads(line))
            except json.JSONDecodeError:
                pass
    except Exception:
        pass


def dismiss_cookie_banner(page):
    """Dismiss GDPR / cookie consent banners (DE/ES/EN)."""
    # First try the data-attribute selectors (most reliable)
    for sel in [
        'button[data-cookiebanner="accept_button"]',
        'button[data-cookiebanner="accept_only_essential_button"]',
        '[data-testid="cookie-policy-manage-dialog-accept-button"]',
    ]:
        try:
            btn = page.query_selector(sel)
            if btn:
                btn.click(force=True)
                print("  -> Dismissed cookie banner: %s" % sel, file=sys.stderr)
                human_delay(1, 2)
                return True
        except Exception:
            pass

    # Then try text-based selectors
    for text in [
        "Alle Cookies erlauben", "Alle akzeptieren",
        "Permitir todas las cookies", "Allow all cookies", "Accept all",
        "Nur essenzielle Cookies erlauben", "Optionale Cookies ablehnen",
        "Decline optional cookies", "Rechazar cookies opcionales",
    ]:
        try:
            btn = page.query_selector('button:has-text("%s")' % text)
            if btn:
                btn.click(force=True)
                print("  -> Dismissed cookie banner: %s" % text, file=sys.stderr)
                human_delay(1, 2)
                return True
        except Exception:
            pass

    # Nuclear option: find any div with cookie-related classes and click all buttons in it
    try:
        # Try to dismiss via JS - find and click the first visible large button at bottom
        result = page.evaluate("""() => {
            const buttons = document.querySelectorAll('button');
            for (const btn of buttons) {
                const text = btn.innerText.toLowerCase();
                if (text.includes('cookie') || text.includes('erlauben') ||
                    text.includes('akzeptieren') || text.includes('permitir') ||
                    text.includes('allow') || text.includes('accept')) {
                    btn.click();
                    return 'clicked: ' + btn.innerText.trim().substring(0, 40);
                }
            }
            return 'none found';
        }""")
        if result and result != 'none found':
            print("  -> Cookie banner JS: %s" % result, file=sys.stderr)
            human_delay(1, 2)
            return True
    except Exception:
        pass

    print("  -> No cookie banner found (may be OK)", file=sys.stderr)
    return False


def _click_continue(page):
    """Click any Continue/Continuar/Weiter button on the page."""
    for sel in [
        'a:has-text("Continue")', 'a:has-text("Continuar")', 'a:has-text("Weiter")',
        'button:has-text("Continue")', 'button:has-text("Continuar")', 'button:has-text("Weiter")',
        'div[role="button"]:has-text("Continue")',
        'div[role="button"]:has-text("Continuar")',
        'div[role="button"]:has-text("Weiter")',
        'input[value="Continue"]', 'input[value="Continuar"]', 'input[value="Weiter"]',
        '[data-testid="cookie-policy-manage-dialog-accept-button"]',
    ]:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.click(force=True)
                print("  -> Clicked: %s" % sel, file=sys.stderr)
                human_delay(2, 3)
                return True
        except Exception:
            pass
    # JS fallback — find any clickable with continue/continuar/weiter text
    try:
        clicked = page.evaluate("""() => {
            const all = document.querySelectorAll('a, button, div[role="button"], input[type="submit"]');
            for (const el of all) {
                const t = (el.innerText || el.value || '').trim().toLowerCase();
                if (t === 'continue' || t === 'continuar' || t === 'weiter' ||
                    t === 'get started' || t === 'comenzar' || t === 'los geht\\'s') {
                    el.click();
                    return t;
                }
            }
            return null;
        }""")
        if clicked:
            print("  -> JS clicked: '%s'" % clicked, file=sys.stderr)
            human_delay(2, 3)
            return True
    except Exception:
        pass
    return False


def _try_reauth(page):
    """Check if FB is showing a password-only re-auth page and handle it.
    Returns True if re-auth succeeded, False if failed, None if not a re-auth page."""
    pass_el = None
    for sel in ['input[name="pass"]', '#pass', 'input[type="password"]']:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                pass_el = el
                break
        except Exception:
            pass

    email_el = None
    for sel in ['input[name="email"]', '#email', 'input[type="email"]']:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                email_el = el
                break
        except Exception:
            pass

    if not pass_el or email_el:
        # Not a password-only re-auth page
        return None

    # PASSWORD-ONLY re-auth — click into the field, clear it, type password char-by-char
    print("  -> Re-auth page detected (password only)...", file=sys.stderr)
    pass_el.click()
    human_delay(0.3, 0.5)
    # Select all + delete to clear any pre-filled text
    page.keyboard.press("Meta+a")
    page.keyboard.press("Backspace")
    human_delay(0.2, 0.4)
    page.keyboard.type(FB_PASSWORD, delay=80)
    human_delay(0.5, 1.0)

    # Click "Iniciar sesión" / "Log In" / "Anmelden"
    login_clicked = False
    for sel in [
        'div[role="button"]:has-text("Iniciar sesión")',
        'div[role="button"]:has-text("Log In")',
        'div[role="button"]:has-text("Anmelden")',
        'button[type="submit"]', 'button[name="login"]',
        '#loginbutton', 'input[type="submit"]',
    ]:
        try:
            btn = page.query_selector(sel)
            if btn:
                btn.click(force=True)
                login_clicked = True
                print("  -> Clicked re-auth login: %s" % sel, file=sys.stderr)
                break
        except Exception:
            pass

    if not login_clicked:
        page.keyboard.press("Enter")
        print("  -> Pressed Enter for re-auth", file=sys.stderr)

    page.wait_for_load_state("networkidle", timeout=30000)
    human_delay(3, 5)

    url = page.url.lower()
    if "login" not in url and "checkpoint" not in url:
        print("  -> Re-auth successful!", file=sys.stderr)
        return True
    if "checkpoint" in url:
        print("  !! Checkpoint after re-auth — waiting 120s...", file=sys.stderr)
        start = time.time()
        while time.time() - start < 120:
            if "login" not in page.url.lower() and "checkpoint" not in page.url.lower():
                return True
            time.sleep(2)
        return False
    print("  -> Re-auth didn't work...", file=sys.stderr)
    return False


def login_to_facebook(page):
    """Log into Facebook — exact same pattern as Marketing version."""
    print("  -> Navigating to Facebook login...", file=sys.stderr)
    page.goto("https://www.facebook.com/login", wait_until="networkidle", timeout=30000)
    human_delay(2, 4)

    # Dismiss cookie consent banner FIRST (blocks the form on DE/EU)
    dismiss_cookie_banner(page)
    human_delay(1, 2)

    # ── CHECK: password-only re-auth (before any Continue clicks) ────────
    reauth = _try_reauth(page)
    if reauth is True:
        return True
    if reauth is False:
        pass  # failed, continue to try full login

    # Try clicking Continue/Continuar/Weiter (checkpoint/review pages)
    if _click_continue(page):
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        human_delay(3, 5)

        # ── CHECK: Continue may have revealed a re-auth page ─────────────
        reauth = _try_reauth(page)
        if reauth is True:
            return True

        # Try second Continue only if re-auth didn't fire
        if _click_continue(page):
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            human_delay(3, 5)

            # ── CHECK AGAIN after second Continue ────────────────────────
            reauth = _try_reauth(page)
            if reauth is True:
                return True

    # Re-check URL after banner dismissal and continue clicks
    current_url = page.url.lower()
    print("  -> Current URL: %s" % page.url, file=sys.stderr)

    # If cookies worked, already logged in
    if "login" not in current_url and "checkpoint" not in current_url:
        print("  -> Already logged in via cookies!", file=sys.stderr)
        return True

    print("  -> Logging in as %s..." % FB_EMAIL, file=sys.stderr)

    # ── FULL LOGIN: email + password ─────────────────────────────────────
    # Try standard selectors first, then fallback
    email_filled = False
    for sel in ['input[name="email"]', '#email', 'input[type="email"]', 'input[id="email"]']:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.fill(FB_EMAIL)
                email_filled = True
                print("  -> Email filled via %s" % sel, file=sys.stderr)
                break
        except Exception:
            pass

    if not email_filled:
        print("  ERROR: Could not find email field!", file=sys.stderr)
        page.screenshot(path="/tmp/fb_login_debug.png")
        print("  Screenshot: /tmp/fb_login_debug.png", file=sys.stderr)
        return False

    human_delay(0.5, 1.5)

    pass_filled = False
    for sel in ['input[name="pass"]', '#pass', 'input[type="password"]']:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.fill(FB_PASSWORD)
                pass_filled = True
                print("  -> Password filled via %s" % sel, file=sys.stderr)
                break
        except Exception:
            pass

    if not pass_filled:
        print("  ERROR: Could not find password field!", file=sys.stderr)
        return False

    human_delay(0.5, 1.0)

    # Debug: verify the fields actually have our values
    try:
        email_val = page.evaluate('document.querySelector(\'input[name="email"]\').value')
        pass_val = page.evaluate('document.querySelector(\'input[name="pass"]\').value')
        print("  -> DEBUG email value: '%s' (len=%d)" % (email_val[:3] + "***", len(email_val)), file=sys.stderr)
        print("  -> DEBUG pass value length: %d" % len(pass_val), file=sys.stderr)

        # Check for any error messages or overlays
        page_text = page.evaluate("document.body.innerText.substring(0, 500)")
        print("  -> DEBUG page text: %s" % repr(page_text[:300]), file=sys.stderr)

        # Count visible buttons AND inputs
        btn_info = page.evaluate("""() => {
            const btns = document.querySelectorAll('button, input[type="submit"], [role="button"]');
            return Array.from(btns).map(b => ({
                tag: b.tagName,
                type: b.type || '',
                name: b.name || '',
                id: b.id || '',
                text: b.innerText ? b.innerText.trim().substring(0,40) : b.value || '',
                visible: b.offsetParent !== null
            }));
        }""")
        print("  -> DEBUG clickables: %s" % json.dumps(btn_info, ensure_ascii=False)[:600], file=sys.stderr)
    except Exception as e:
        print("  -> DEBUG check failed: %s" % e, file=sys.stderr)

    # Click login button — FB uses <div role="button">, NOT <button>!
    login_clicked = False
    for sel in [
        'div[role="button"]:has-text("Iniciar sesión")',
        'div[role="button"]:has-text("Log In")',
        'div[role="button"]:has-text("Anmelden")',
        'button[name="login"]', '#loginbutton',
        'button[type="submit"]', 'input[type="submit"]',
        '[data-testid="royal_login_button"]',
    ]:
        try:
            el = page.query_selector(sel)
            if el:
                el.click(force=True)
                login_clicked = True
                print("  -> Clicked login via %s" % sel, file=sys.stderr)
                break
        except Exception:
            pass

    if not login_clicked:
        # JS fallback — find div[role=button] with login text and click it
        try:
            page.evaluate("""() => {
                const divs = document.querySelectorAll('div[role="button"]');
                for (const d of divs) {
                    const t = d.innerText.trim().toLowerCase();
                    if (t === 'iniciar sesión' || t === 'log in' || t === 'anmelden') {
                        d.click();
                        return;
                    }
                }
                // Last resort: submit the form
                const form = document.querySelector('#login_form') || document.querySelector('form');
                if (form) form.submit();
            }""")
            print("  -> Clicked login via JS div[role=button] search", file=sys.stderr)
        except Exception:
            # Absolute last resort
            page.keyboard.press("Enter")
            print("  -> Pressed Enter as last resort", file=sys.stderr)

    page.wait_for_load_state("networkidle", timeout=30000)
    human_delay(3, 5)

    url = page.url.lower()
    print("  -> Post-login URL: %s" % page.url, file=sys.stderr)
    print("  -> Page title: %s" % page.title(), file=sys.stderr)

    # Try clicking "Continue" / "Continuar" / "Weiter" buttons that FB shows
    _click_continue(page)

    if "checkpoint" in url:
        print("  !! 2FA / CHECKPOINT — trying to click Continue...", file=sys.stderr)
        _click_continue(page)
        human_delay(3, 5)
        # Check again after clicking
        if "login" not in page.url.lower() and "checkpoint" not in page.url.lower():
            print("  -> Checkpoint cleared after Continue click!", file=sys.stderr)
            return True
        print("  !! Still on checkpoint — waiting 120s for manual action...", file=sys.stderr)
        start = time.time()
        while time.time() - start < 120:
            _click_continue(page)  # Keep trying to click Continue
            if "login" not in page.url.lower() and "checkpoint" not in page.url.lower():
                print("  -> Checkpoint cleared!", file=sys.stderr)
                return True
            time.sleep(3)
        print("  Checkpoint timeout.", file=sys.stderr)
        return False

    if "login" not in url:
        print("  -> Login successful!", file=sys.stderr)
        return True

    print("  Login failed — still on login page.", file=sys.stderr)
    page.screenshot(path="/tmp/fb_login_debug.png")
    return False


def save_cookies(context):
    cookies = context.cookies()
    COOKIES_FILE.write_text(json.dumps(cookies, indent=2))
    print("  -> Saved %d cookies" % len(cookies), file=sys.stderr)


def load_cookies(context):
    if COOKIES_FILE.exists():
        try:
            cookies = json.loads(COOKIES_FILE.read_text())
            if cookies:
                context.add_cookies(cookies)
                print("  -> Loaded %d saved cookies" % len(cookies), file=sys.stderr)
                return True
        except Exception as e:
            print("  Cookie load error: %s" % e, file=sys.stderr)
    return False


def dismiss_popups(page):
    for sel in ['[aria-label="Close"]', '[aria-label="Cerrar"]', '[aria-label="Schließen"]',
                'div[role="dialog"] [aria-label="Close"]', 'div[role="dialog"] [aria-label="Cerrar"]']:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                el.click()
                human_delay(0.5, 1.0)
        except Exception:
            pass


def handle_login_wall(page):
    """If FB shows a login modal on marketplace, fill credentials in that popup."""
    try:
        email_el = page.query_selector('input[name="email"]') or page.query_selector('#email')
        if not email_el or not email_el.is_visible():
            return False

        print("  !! Login wall on marketplace — filling credentials in popup...", file=sys.stderr)
        email_el.fill(FB_EMAIL)
        human_delay(0.5, 1.0)

        pass_el = page.query_selector('input[name="pass"]') or page.query_selector('input[type="password"]')
        if pass_el:
            pass_el.fill(FB_PASSWORD)
            human_delay(0.5, 1.0)

        for sel in ['div[role="button"]:has-text("Iniciar sesión")',
                     'div[role="button"]:has-text("Log In")',
                     'div[role="button"]:has-text("Anmelden")',
                     'button[name="login"]', '#loginbutton', 'button[type="submit"]',
                     'button:has-text("Anmelden")', 'button:has-text("Log In")',
                     'button:has-text("Iniciar sesion")', 'button:has-text("Iniciar sesión")']:
            try:
                btn = page.query_selector(sel)
                if btn and btn.is_visible():
                    btn.click()
                    print("  -> Clicked popup login: %s" % sel, file=sys.stderr)
                    break
            except Exception:
                pass

        page.wait_for_load_state("networkidle", timeout=30000)
        human_delay(3, 5)
        print("  -> Login wall handled.", file=sys.stderr)
        return True
    except Exception as e:
        print("  Login wall error: %s" % e, file=sys.stderr)
        return False


def run_scrape(target_url, scroll_steps):
    global vehicles, graphql_count, qualifying_count
    vehicles = {}
    graphql_count = 0
    qualifying_count = 0

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: playwright not installed.", file=sys.stderr)
        return []

    stealth_cls = None
    try:
        from playwright_stealth import Stealth
        stealth_cls = Stealth
        print("  Stealth mode enabled", file=sys.stderr)
    except ImportError:
        print("  (no playwright_stealth — running without)", file=sys.stderr)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        context = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1366, "height": 768},
            locale="es-CL",
            timezone_id="America/Santiago",
            permissions=["geolocation"],
        )

        load_cookies(context)
        page = context.new_page()

        if stealth_cls:
            try:
                stealth = stealth_cls(
                    navigator_languages_override=("es-CL", "es"),
                    navigator_vendor_override="Google Inc.",
                    webgl_vendor_override="Intel Inc.",
                    webgl_renderer_override="Intel Iris OpenGL Engine",
                )
                stealth.apply_stealth_sync(page)
            except Exception as e:
                print("  Stealth error: %s" % e, file=sys.stderr)

        page.on("response", handle_response)

        # ── WARMUP: visit Google first so FB sees a real browsing session ──
        print("  Warming up browser (Google)...", file=sys.stderr)
        try:
            page.goto("https://www.google.com", wait_until="domcontentloaded", timeout=15000)
            human_delay(2, 4)
        except Exception:
            pass  # not critical if it fails

        # LOGIN
        if not login_to_facebook(page):
            browser.close()
            return []
        save_cookies(context)

        # NAVIGATE TO MARKETPLACE
        print("\n  Navigating to Marketplace...", file=sys.stderr)
        page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
        human_delay(3, 5)
        dismiss_popups(page)
        handle_login_wall(page)

        if "login" in page.url.lower():
            print("  Redirected to login — trying again...", file=sys.stderr)
            if not login_to_facebook(page):
                browser.close()
                return []
            save_cookies(context)
            page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
            human_delay(3, 5)
            dismiss_popups(page)

        # SCROLL & COLLECT
        print("\n  Scraping: target %d qualifying leads (max %d scrolls)...\n" % (TARGET_LEADS, scroll_steps), file=sys.stderr)
        for i in range(1, scroll_steps + 1):
            if i % 10 == 0:
                dismiss_popups(page)
            if i % 20 == 0:
                handle_login_wall(page)

            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            print(
                "  Scroll %4d/%d -- %d/%d qualifying | %d total | %d GraphQL" % (
                    i, scroll_steps, qualifying_count, TARGET_LEADS,
                    len(vehicles), graphql_count),
                file=sys.stderr,
            )
            human_delay(SCROLL_DELAY_MIN, SCROLL_DELAY_MAX)

            if qualifying_count >= TARGET_LEADS:
                print("\n  Reached %d qualifying leads! Stopping." % qualifying_count, file=sys.stderr)
                break

        human_delay(2, 3)
        browser.close()
        print("\n  Done! %d total | %d qualifying V-Region." % (len(vehicles), qualifying_count), file=sys.stderr)

    return list(vehicles.values())


def main():
    parser = argparse.ArgumentParser(description="Scrape Facebook Marketplace")
    parser.add_argument("--output", "-o", help="Output JSON file path")
    parser.add_argument("--scrolls", "-s", type=int, default=DEFAULT_SCROLL_STEPS)
    parser.add_argument("--url", default=DEFAULT_TARGET_URL)
    parser.add_argument("--headless", action="store_true", help="(ignored)")
    args = parser.parse_args()

    listings = run_scrape(args.url, args.scrolls)

    output = json.dumps(listings, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")
        print("  Saved %d listings -> %s" % (len(listings), args.output), file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
