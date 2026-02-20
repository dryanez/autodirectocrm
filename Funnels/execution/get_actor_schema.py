from apify_client import ApifyClient
import os
import json

API_TOKEN = os.getenv("APIFY_TOKEN")
ACTOR_ID = "apify/facebook-marketplace-scraper"

def get_schema():
    client = ApifyClient(API_TOKEN)
    print(f"Fetching Schema for: {ACTOR_ID}...")
    
    try:
        # Get the actor details first
        actor = client.actor(ACTOR_ID).get()
        
        # Then verify if we can access the build/version to get schema
        # Since client.actor(id).get() returns the object, acts typically have a "taggedBuilds"
        # or we can try to fetch the build directly if we had a build ID.
        # EASIER WAY: List builds and get the latest one's schema?
        
        # Actually, let's try to run with a clearly invalid input to see if it spits back the schema error?
        # Or better, use the client to get the actor's default run input which hints at structure.
        
        # Alternative: Just print keys from example run input again?
        # The previous attempt showed a very empty example.
        
        # Let's try to list versions
        versions = client.actor(ACTOR_ID).versions().list()
        latest = next((v for v in versions.items if v.get('versionNumber') == '0.0'), versions.items[0])
        print(f"Versions found. Inspecting version: {latest.get('versionNumber')}")
        
        # Schema is usually stored in the build or version object?
        # The python client might not expose schema fetching easily without build ID.
        
        print("Schema (via version):")
        # In Apify API, version object contains 'inputSchema' usually
        # Let's verify that.
        full_version = client.actor(ACTOR_ID).version(latest.get('versionNumber')).get()
        print(json.dumps(full_version.get('inputSchema'), indent=2))
        
    except Exception as e:
        print(f"Error fetching schema: {e}")

if __name__ == "__main__":
    get_schema()
