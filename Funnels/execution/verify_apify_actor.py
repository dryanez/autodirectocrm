from apify_client import ApifyClient
import os

API_TOKEN = os.getenv("APIFY_TOKEN")
POSSIBLE_ID = "impressive_operators/facebook-marketplace-scraper"

def check_apify():
    client = ApifyClient(API_TOKEN)
    
    print(f"Checking ID: {POSSIBLE_ID}...")
    
    # Check if it's an Actor
    try:
        actor = client.actor(POSSIBLE_ID).get()
        if actor:
            print(f"✓ Found Actor: {actor.get('name')} ({actor.get('id')})")
            return
    except Exception as e:
        print(f"Not an actor: {e}")

    # Check if it's a Task
    try:
        task = client.task(POSSIBLE_ID).get()
        if task:
            print(f"✓ Found Task: {task.get('name')} ({task.get('id')})")
            return
    except Exception as e:
        print(f"Not a task: {e}")

    # Check if it's a User? (Client doesn't have direct user lookup by ID usually easily like this)
    try:
        user = client.user(POSSIBLE_ID).get()
        if user:
             print(f"✓ Found User: {user.get('username')} ({user.get('id')})")
             return
    except:
        pass
        
    print("❌ ID not found as Actor or Task.")
    print("Defaulting to standard actor: moJkRxrc85HZ6 (menew/facebook-marketplace-scraper)")

if __name__ == "__main__":
    check_apify()
