from apify_client import ApifyClient
import os

API_TOKEN = os.getenv("APIFY_TOKEN")

def list_my_actors():
    client = ApifyClient(API_TOKEN)
    
    print("Listing ALL available actors for this token...")
    
    try:
        # List actors owned by the user (or accessible)
        # Note: This usually only lists actors created by the user or added to their account.
        actors = client.actors().list()
        
        if not actors.items:
            print("No actors found in your account.")
        else:
            print(f"Found {len(actors.items)} actors:")
            for actor in actors.items:
                print(f" - {actor.get('name')} (ID: {actor.get('id')})")
                
    except Exception as e:
        print(f"Error listing actors: {e}")

if __name__ == "__main__":
    list_my_actors()
