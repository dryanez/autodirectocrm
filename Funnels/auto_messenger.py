import asyncio
import argparse
import sys
import json
import time
import random
import requests
import shutil
import tempfile
from pathlib import Path
from playwright.async_api import async_playwright

DASHBOARD_URL = "http://localhost:5001"
CHROME_USER_DATA_SRC = Path.home() / "Library/Application Support/Google/Chrome"

# ─── API Helpers ─────────────────────────────────────────────────────────────
def get_new_leads():
    """Fetch all leads from the dashboard that have status='new'."""
    try:
        resp = requests.get(f"{DASHBOARD_URL}/api/leads", timeout=5)
        resp.raise_for_status()
        leads = resp.json()
        return [l for l in leads if l.get("status") == "new"]
    except Exception as e:
        print(f"❌ Error fetching leads from dashboard: {e}")
        return []

def get_valuation(lead):
    """Call the dashboard API to get the AI valuation for this lead."""
    try:
        payload = {
            "make": lead.get("title", "").split()[0] if lead.get("title") else "Unknown",
            "model": " ".join(lead.get("title", "").split()[1:]) if lead.get("title") else "Unknown",
            "year": str(lead.get("year") or 2020),
            "mileage": str(lead.get("mileage") or "0")
        }
        resp = requests.post(f"{DASHBOARD_URL}/api/valuation", json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("success"):
            return data.get("pricing")
        else:
            print(f"⚠️ Valuation failed for {lead['url']}: {data}")
            return None
    except Exception as e:
        print(f"❌ Error fetching valuation: {e}")
        return None

def mark_contacted(lead, valuation_data):
    """Call the dashboard API to mark the lead as contacted."""
    try:
        payload = {
            "url": lead["url"],
            "status": "contacted",
            "valuation": valuation_data
        }
        resp = requests.post(f"{DASHBOARD_URL}/api/leads/status", json=payload, timeout=5)
        resp.raise_for_status()
        print(f"✅ Marked {lead['url']} as contacted in dashboard.")
    except Exception as e:
        print(f"❌ Error updating lead status: {e}")

def generate_message(lead, valuation_data):
    """Generate a personalised valuation message with slight variation to avoid spam detection."""
    seller_name = lead.get('seller') or 'Amigo'
    first_name = seller_name.split(' ')[0]
    
    brand = lead.get("title", "").split()[0] if lead.get("title") else "Auto"
    model = " ".join(lead.get("title", "").split()[1:]) if lead.get("title") else ""
    year = lead.get("year", "2020")
    
    amount = valuation_data.get('consignment_liquidation', 0)
    formatted_amount = f"CLP {amount:,}".replace(",", ".")
    
    # Slightly rotate the opener to avoid identical message fingerprints
    openers = [
        f"Hola {first_name}, vi que tienes publicado tu {brand.upper()} {model.upper()} {year}.",
        f"Hola {first_name}, me encontré con tu {brand.upper()} {model.upper()} {year} publicado.",
        f"Hola {first_name}, noté que tienes en venta tu {brand.upper()} {model.upper()} {year}.",
        f"Hola {first_name}, revisé tu publicación del {brand.upper()} {model.upper()} {year}.",
        f"Hola {first_name}, pasé por tu anuncio del {brand.upper()} {model.upper()} {year}.",
    ]
    opener = random.choice(openers)

    message = f"""{opener}

Nuestra tecnología analizó el mercado y estima que podríamos gestionar la venta asegurándote {formatted_amount} líquidos para tu bolsillo.

Así tú obtienes el monto que buscas sin tener que lidiar con extraños ni trámites.

¿Te hace sentido esa cifra para conversar?

Me puedes escribir directo por WhatsApp: +56940441470

Quedo atento,
Felipe Yanez
Agente de Ventas | Autodirecto.cl"""
    return message

# ─── Playwright Automation ───────────────────────────────────────────────────
async def login_facebook(page):
    print("🔐 Checking Facebook login status...")
    await page.goto("https://www.facebook.com", wait_until="domcontentloaded")
    await asyncio.sleep(3)
    
    cookie_consent = await page.query_selector('button[title="Allow all cookies"]')
    if cookie_consent:
        await cookie_consent.click()
        await asyncio.sleep(1)
        
    email_input = await page.query_selector('#email')
    if email_input:
        print("  Filling credentials for felipe@autodirecto.cl...")
        await page.fill('#email', 'felipe@autodirecto.cl')
        await asyncio.sleep(0.5)
        await page.fill('#pass', 'Comoestas01@')
        await asyncio.sleep(0.5)
        
        login_btn = await page.query_selector('[name="login"]')
        if login_btn:
            await login_btn.click()
        else:
            await page.press('#pass', 'Enter')
            
        await asyncio.sleep(5)  # wait for redirect
        
        url = page.url
        if "checkpoint" in url:
            print("⚠️  Facebook checkpoint/2FA hit. Waiting 45s for manual intervention.")
            await asyncio.sleep(45)
        else:
            print("✅ Logged in!")
    else:
        print("✅ Already logged in (no login form found).")

async def send_message_to_lead(page, lead, message_text, test_mode=False):
    """Navigate to the listing and send the message as the active page."""
    print(f"\n🚗 Navigating to lead: {lead['title']}")
    print(f"🔗 URL: {lead['url']}")
    
    await page.goto(lead['url'], wait_until="domcontentloaded")
    await asyncio.sleep(4) # Let the marketplace UI settle
    
    # Check for Marketplace delayed login popup overlay (e.g. "Mehr auf Facebook ansehen")
    print("👀 Checking for login overlay popup...")
    try:
        # Often pops up exactly 3-5 seconds after page load. Wait briefly for it.
        email_input = await page.wait_for_selector('div[role="dialog"] input[type="text"]:not([type="hidden"])', state="visible", timeout=3000)
        if email_input:
            print("⚠️ Detected login popup overlay on marketplace! Filling it...")
            await email_input.fill('felipe@autodirecto.cl')
            await asyncio.sleep(0.5)
            pass_input = await page.query_selector('div[role="dialog"] input[type="password"]')
            if pass_input:
                await pass_input.fill('Comoestas01@')
                await asyncio.sleep(0.5)
                await pass_input.press("Enter")
                print("✅ Submitted popup login. Waiting for page to reload/settle...")
                await asyncio.sleep(8)
    except Exception:
        print("✅ No login popup overlay detected.")

    try:
        # Click the 'Message' button on the listing
        # Try a robust fallback of multiple languages and aria-labels
        selectors = [
            'div[aria-label="Message"]',
            'div[aria-label="Send Message"]',
            'div[aria-label="Enviar mensaje"]',
            'div[aria-label="Mensaje"]',
            'text="Message"',
            'text="Enviar mensaje"',
            'text="Enviar Mensaje"',
            'text="Nachricht senden"',
            'span:has-text("Message")',
            'span:has-text("Enviar mensaje")',
            'span:has-text("Nachricht senden")',
        ]
        
        message_btn = None
        for sel in selectors:
            elements = await page.query_selector_all(sel)
            for el in elements:
                if await el.is_visible():
                    message_btn = el
                    break
            if message_btn:
                break
            
        if not message_btn:
            print("❌ Could not find the Message button on the listing.")
            print("Dumping all buttons on page for debugging:")
            buttons = await page.query_selector_all('div[role="button"]')
            for b in buttons:
                text = await b.inner_text()
                aria = await b.get_attribute("aria-label")
                if text.strip() or aria:
                    print(f"  - text: {repr(text.strip().replace(chr(10), ' '))} | aria: {repr(aria)}")
            
            await page.screenshot(path="fb_marketplace_button_error.png")
            return False
            
        await message_btn.click()
        print("💬 Clicked Message button. Waiting for chat input field...")
        await asyncio.sleep(4)
        
        # Locate the chat input box
        input_selectors = [
            'textarea[placeholder="Please type your message to the seller"]',
            'textarea[placeholder="Escribe un mensaje..."]',
            'textarea[placeholder="Nachricht schreiben ..."]',
            'textarea',
            'div[aria-label="Message"] > div',
            'div[aria-label="Send a message..."]',
            'div[aria-label="Escribe un mensaje..."]',
            'div[aria-label="Nachricht schreiben ..."]',
            'div[aria-label="Mensaje"] > div',
            'div[role="textbox"]',
        ]
        
        chat_input = None
        for sel in input_selectors:
            # We use query_selector and check visibility since there might be multiple textboxes (e.g. search)
            # Prioritize elements inside the dialog popup
            boxes = await page.query_selector_all(f'div[role="dialog"] {sel}')
            if not boxes:
                boxes = await page.query_selector_all(sel)
                
            for b in boxes:
                if await b.is_visible():
                    chat_input = b
                    break
            if chat_input:
                break
        
        if not chat_input:
            print("❌ Could not find the chat input text area inside the popup. Taking screenshot...")
            await page.screenshot(path="fb_marketplace_input_error.png")
            return False
            
        # Type the message
        print("✍️ Typing message...")
        try:
            # In simple textareas, fill() is robust and clears it first
            await chat_input.fill(message_text, timeout=3000)
        except Exception:
            # Fallback for complex rich-text divs
            await chat_input.click(force=True)
            await asyncio.sleep(0.5)
            await page.keyboard.insert_text(message_text)
            
        await asyncio.sleep(2)
        
        if test_mode:
            print("🧪 TEST MODE: Waiting 5 seconds then aborting (not clicking send).")
            await asyncio.sleep(5)
            # Close chat box popup using Esc or clicking X
            close_btn = await page.query_selector('div[role="dialog"] div[aria-label="Close"]')
            if close_btn:
                await close_btn.click(force=True)
            else:
                await page.keyboard.press("Escape")
            return True
            
        else:
            print("🚀 Clicking SEND...")
            # Click the exact send Button inside the dialog
            send_btn = None
            send_selectors = [
                'div[role="dialog"] div[aria-label="Send Message"]',
                'div[role="dialog"] div[aria-label="Send"]',
                'div[role="dialog"] span:text-is("Send Message")',
                'div[role="dialog"] div[aria-label="Enviar mensaje"]',
                'div[role="dialog"] span:text-is("Enviar")',
                'div[role="dialog"] div[aria-label="Nachricht senden"]',
                'div[role="dialog"] span:text-is("Senden")',
                'div[role="dialog"] div[role="button"]:has-text("Send")',
                'div[role="dialog"] div[role="button"]:has-text("Enviar")'
            ]
            for sel in send_selectors:
                elements = await page.query_selector_all(sel)
                for el in elements:
                    if await el.is_visible() and (await el.inner_text() or await el.get_attribute('aria-label')):
                        send_btn = el
                        break
                if send_btn:
                    break
                    
            if send_btn:
                await send_btn.click(force=True)
            else:
                # Fallback to Enter if we couldn't find the button
                await page.keyboard.press("Enter")
                
            print("✅ Message sent!")
            await asyncio.sleep(3)
            return True

    except Exception as e:
        print(f"❌ Error while trying to send message: {e}")
        return False
        

# ─── Main Execution ──────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Run in test mode (do not actually click send)")
    parser.add_argument("--limit", type=int, default=1, help="Max number of leads to process")
    args = parser.parse_args()

    # 1. Fetch leads from Dashboard
    leads = get_new_leads()
    print(f"📊 Found {len(leads)} 'new' leads in the dashboard.")
    if not leads:
        print("🎉 No new leads to process. Exiting.")
        return
        
    leads_to_process = leads[:args.limit]
    print(f"🎯 Will process {len(leads_to_process)} lead(s) this run.")

    # 2. Setup Chrome Profile Copy
    if not CHROME_USER_DATA_SRC.exists():
        print(f"❌ Chrome user data directory not found at {CHROME_USER_DATA_SRC}")
        return

    print("📂 Copying Chrome profile to temp directory...")
    tmp_dir = Path(tempfile.mkdtemp(prefix="fb_messenger_"))
    default_src = CHROME_USER_DATA_SRC / "Default"
    default_dst = tmp_dir / "Default"
    shutil.copytree(default_src, default_dst, ignore=shutil.ignore_patterns(
        'Cache', 'Code Cache', 'GPUCache', 'Service Worker',
        'blob_storage', 'IndexedDB', 'File System',
        'GCM Store', 'BudgetDatabase', 'optimization_guide*',
        'heavy_ad*', 'AutofillStrikeDatabase',
        'databases', 'Platform Notifications', 'shared_proto_db',
    ), dirs_exist_ok=True)
    local_state = CHROME_USER_DATA_SRC / "Local State"
    if local_state.exists():
        shutil.copy2(local_state, tmp_dir / "Local State")
    print(f"✅ Profile ready at: {tmp_dir}")
    
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(tmp_dir),
            headless=False,
            channel="chromium",
            viewport={"width": 1280, "height": 900},
            args=["--disable-blink-features=AutomationControlled"]
        )
        page = context.pages[0] if context.pages else await context.new_page()

        # 3. Ensure we are logged in
        await login_facebook(page)

        # 4. Process Leads
        for i, lead in enumerate(leads_to_process, 1):
            print(f"\n--- Processing Lead {i}/{len(leads_to_process)} ---")
            
            # 4a. Get Valuation
            print("🤖 Asking Dashboard API for AI Valuation...")
            valuation = get_valuation(lead)
            if not valuation:
                print("⏭️ Skipping lead because valuation failed.")
                continue
                
            # 4b. Format Message
            msg = generate_message(lead, valuation)
            print("📝 Generated Message:")
            print("--------------------------------------------------")
            print(msg)
            print("--------------------------------------------------")
            
            # 4c. Send on Facebook
            success = await send_message_to_lead(page, lead, msg, test_mode=args.test)
            
            # 4d. Update Dashboard Status
            if success:
                # Mark contacted regardless of test_mode, so we know it worked
                mark_contacted(lead, valuation)
            else:
                print(f"⚠️ Failed to send message for {lead['url']}")
                
            if i < len(leads_to_process):
                delay = random.randint(300, 480)  # 5–8 minutes
                print(f"⏳ Waiting {delay//60}m {delay%60}s before next lead (anti-spam)...")
                await asyncio.sleep(delay)
                
        print("\n🎉 Auto-messenger run complete!")
        await asyncio.sleep(2)
        await context.close()
        
    shutil.rmtree(tmp_dir, ignore_errors=True)
    print("🗑️ Temp profile cleaned up.")

if __name__ == "__main__":
    asyncio.run(main())
