from apify_client import ApifyClient
import os

API_TOKEN = os.getenv("APIFY_TOKEN")
RUN_ID = "tcDMTcZjKQ8coUroo"

def debug_run():
    client = ApifyClient(API_TOKEN)
    print(f"Debugging Run: {RUN_ID}...")
    
    # 1. Get Run Info
    run = client.run(RUN_ID).get()
    print(f"Status: {run.get('status')}")
    print(f"Actor ID: {run.get('actId')}")
    
    # 2. Get Input
    try:
        kv_store = client.key_value_store(run.get('defaultKeyValueStoreId'))
        input_data = kv_store.get_record('INPUT')
        print("\n--- INPUT ---")
        print(input_data['value'] if input_data else "No Input Found")
    except Exception as e:
        print(f"Error getting input: {e}")

    # 3. Get Log (tail)
    try:
        log = client.log(RUN_ID).get()
        print("\n--- LOG TAIL ---")
        # Print last 1000 chars
        print(log[-2000:] if log else "No Log Found")
    except Exception as e:
        print(f"Error getting log: {e}")

if __name__ == "__main__":
    debug_run()
