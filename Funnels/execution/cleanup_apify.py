from apify_client import ApifyClient
import os

API_TOKEN = os.getenv("APIFY_TOKEN")

def cleanup():
    client = ApifyClient(API_TOKEN)
    print("Cleaning up Apify Runs...")
    
    # 1. List Running Actors
    runs = client.runs().list(status='RUNNING').items
    for run in runs:
        print(f"Aborting Run: {run['id']} (Actor: {run.get('actId')})")
        client.run(run['id']).abort()
        
    runs = client.runs().list(status='READY').items
    for run in runs:
        print(f"Aborting Run: {run['id']} (Actor: {run.get('actId')})")
        client.run(run['id']).abort()

    # 2. List Running Builds (if any occupy memory?)
    try:
        builds = client.builds().list().items
        for build in builds:
            if build.get('status') == 'RUNNING':
                print(f"Aborting Build: {build['id']}")
                client.build(build['id']).abort()
    except Exception as e:
        print(f"Error checking builds: {e}")

    print("Cleanup Complete!")

if __name__ == "__main__":
    cleanup()
