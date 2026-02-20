from apify_client import ApifyClient
import os
import json

API_TOKEN = os.getenv("APIFY_TOKEN")
ACTOR_ID = "U5DUNxhH3qKt5PnCf" # apify/facebook-marketplace-scraper

def inspect_actor():
    client = ApifyClient(API_TOKEN)
    print(f"Inspecting Actor: {ACTOR_ID}...")
    
    try:
        actor = client.actor(ACTOR_ID).get()
        print(f"Name: {actor.get('name')}")
        print(f"Example Run Input: {json.dumps(actor.get('exampleRunInput'), indent=2)}")
        
        # Check if we can get input schema (might be separate API call, or in 'latest' build)
        # Usually client.actor(id).builds().list() -> get build -> get input schema
        # But let's start with this.
        
    except Exception as e:
        print(f"Error inspecting actor: {e}")

if __name__ == "__main__":
    inspect_actor()
