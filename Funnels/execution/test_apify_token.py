from apify_client import ApifyClient
import os

API_TOKEN = os.getenv("APIFY_TOKEN")
ACTOR_ID = "apify/hello-world"

def test_token():
    client = ApifyClient(API_TOKEN)
    print(f"Testing token with {ACTOR_ID}...")
    try:
        run = client.actor(ACTOR_ID).call()
        print(f"✓ Success! Run ID: {run['id']}")
    except Exception as e:
        print(f"❌ Failed: {e}")

if __name__ == "__main__":
    test_token()
