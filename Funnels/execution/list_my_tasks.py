from apify_client import ApifyClient
import os

API_TOKEN = os.getenv("APIFY_TOKEN")

def list_my_tasks():
    client = ApifyClient(API_TOKEN)
    
    print("Listing available TASKS for this token...")
    
    try:
        tasks = client.tasks().list()
        
        if not tasks.items:
            print("No tasks found.")
        else:
            print(f"Found {len(tasks.items)} tasks:")
            for task in tasks.items:
                print(f" - {task.get('name')} (ID: {task.get('id')}) (Actor ID: {task.get('actId')})")
                
    except Exception as e:
        print(f"Error listing tasks: {e}")

if __name__ == "__main__":
    list_my_tasks()
