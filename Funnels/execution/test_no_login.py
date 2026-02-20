
from playwright.sync_api import sync_playwright
import time
import random

def test_no_login():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        url = "https://www.facebook.com/marketplace/santiago/vehicles?minYear=2015&sortBy=date_listed_newest&exact=false"
        print(f"Navigating to {url} without login...")
        
        page.goto(url)
        time.sleep(5)
        
        # Check if redirected to login
        if "login" in page.url:
            print("Redirected to login page immediately.")
        else:
            print("Landed on Marketplace page.")
            
            # Try to scroll
            print("Attempting to scroll...")
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            
            # Check for listings
            listings = page.query_selector_all('a[href*="/marketplace/item/"]')
            print(f"Found {len(listings)} listings visible.")
            
            # Check for "Log In" modal blocking content
            modal = page.query_selector('div[role="dialog"]')
            if modal:
                print("Modal detected (likely login prompt).")
            else:
                print("No obvious modal blocking content.")

        browser.close()

if __name__ == "__main__":
    test_no_login()
