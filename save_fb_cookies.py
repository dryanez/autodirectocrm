#!/usr/bin/env python3
"""
Grab Facebook cookies from Chrome and upload to Supabase.
Chrome must be CLOSED before running this.

Usage:
    # 1. Close Chrome
    # 2. Run:
    .venv/bin/python save_fb_cookies.py
"""
import asyncio, json, os, sys, requests
from pathlib import Path
from datetime import datetime
from playwright.async_api import async_playwright

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kqympdxeszdyppbhtzbm.supabase.co")
SUPABASE_KEY = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
                or os.environ.get("SUPABASE_SERVICE_KEY")
                or os.environ.get("SUPABASE_KEY")
                or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtxeW1wZHhlc3pkeXBwYmh0emJtIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDc0OTc3MCwiZXhwIjoyMDg2MzI1NzcwfQ.6WoE2Y7Hzkbrn2xf0va_X57vd40q1zjkz2tWs_mPDyA")

CHROME_USER_DATA = Path.home() / "Library" / "Application Support" / "Google" / "Chrome"


async def main():
    singleton = CHROME_USER_DATA / "SingletonLock"
    if singleton.exists():
        print("❌ Cierra Google Chrome primero y vuelve a ejecutar.")
        sys.exit(1)

    print("🚀 Abriendo Chrome con tu perfil…")
    async with async_playwright() as p:
        browser = await p.chromium.launch_persistent_context(
            user_data_dir=str(CHROME_USER_DATA),
            headless=False,
            channel="chrome",
            args=["--no-first-run", "--no-default-browser-check",
                  "--disable-extensions", "--disable-sync"],
        )

        page = browser.pages[0] if browser.pages else await browser.new_page()

        print("🌐 Navegando a Facebook…")
        await page.goto("https://www.facebook.com", wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)

        if "login" in page.url.lower() or "checkpoint" in page.url.lower():
            print("❌ No estás logueado en Facebook.")
            await browser.close()
            sys.exit(1)

        print("✅ Sesión de Facebook activa!")

        # Use CDP to get ALL cookies including httpOnly
        cdp = await page.context.new_cdp_session(page)
        result = await cdp.send("Network.getAllCookies")
        all_cookies = result.get("cookies", [])

        fb_cookies = []
        for c in all_cookies:
            domain = c.get("domain", "")
            if "facebook.com" not in domain:
                continue

            val = c.get("value", "")
            try:
                val.encode("ascii")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue

            same_site = c.get("sameSite", "Lax")
            if same_site not in ("Strict", "Lax", "None"):
                same_site = "Lax"

            fb_cookies.append({
                "name": c["name"],
                "value": val,
                "domain": domain,
                "path": c.get("path", "/"),
                "secure": bool(c.get("secure", False)),
                "httpOnly": bool(c.get("httpOnly", False)),
                "sameSite": same_site,
            })

        await browser.close()

    if not fb_cookies:
        print("❌ No se pudieron extraer cookies.")
        sys.exit(1)

    cookie_names = {c["name"] for c in fb_cookies}
    print(f"🍪 {len(fb_cookies)} cookies: {', '.join(sorted(cookie_names))}")

    critical = {"c_user", "xs"}
    missing = critical - cookie_names
    if missing:
        print(f"⚠️  Faltan cookies críticas: {missing}")
        sys.exit(1)

    # Upload to Supabase
    print("☁️  Subiendo a Supabase…")
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    payload = {
        "key": "fb_playwright_cookies",
        "value": json.dumps(fb_cookies),
        "updated_at": datetime.utcnow().isoformat(),
    }
    resp = requests.post(f"{SUPABASE_URL}/rest/v1/app_settings", json=payload, headers=headers, timeout=10)

    if resp.status_code in (200, 201, 204):
        print(f"✅ ¡{len(fb_cookies)} cookies guardadas en Supabase!")
        print("   El scraper Vercel → Railway ahora funcionará.")
    else:
        print(f"❌ Error Supabase: {resp.status_code} {resp.text}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
