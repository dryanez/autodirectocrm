from apify_client import ApifyClient
import os

API_TOKEN = os.getenv("APIFY_TOKEN")

CANDIDATES = [
    "getdataforme/facebook-marketplace-scraper",
    "dtrungtin/facebook-marketplace-scraper",
    "pocesar/facebook-marketplace-scraper",
]

def check_actors():
    client = ApifyClient(API_TOKEN)
    print("Checking alternative actors...")
    
    for actor_id in CANDIDATES:
        try:
            actor = client.actor(actor_id).get()
            if actor:
                print(f"✓ FOUND: {actor.get('name')} (ID: {actor.get('id')})")
                print(f"  Description: {actor.get('description')}")
        except Exception as e:
            # print(f"❌ {actor_id}: Not found or error ({e})")
            pass

if __name__ == "__main__":
    check_actors()
