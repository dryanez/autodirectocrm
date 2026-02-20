from apify_client import ApifyClient
import os
import json
# from execution.scrape_apify import map_apify_result

API_TOKEN = os.getenv("APIFY_TOKEN")
RUN_ID = "fmj7TBXGdz8ZMdwrB" # The successful but aborted run

def map_apify_result(item: dict) -> dict:
    """Map Apify result format (U5DUNxhH3qKt5PnCf) to our pipeline format."""
    
    # Extract price (often formatted string or raw number)
    price = item.get("listingPrice") or item.get("price")
    if price and isinstance(price, (int, float)):
         price = f"${price}"

    # Extract location
    location = item.get("location") or item.get("marketplace_listing_location_name")

    # Extract Year from title (simple heuristic if not provided)
    title = item.get("title") or item.get("marketplace_listing_title") or ""
    
    return {
        "url": item.get("url") or item.get("link"),
        "title": title,
        "price": str(price) if price else None,
        "location": location,
        "region": "Santiago", # Hardcoded for this fetch
        "region_key": "santiago",
        "date_text": "Scraped via Apify", 
        "listed_date": None, 
        "days_active": None,
        "is_sold": item.get("isSold", False),
        "seller_name": item.get("sellerName"),
        "scraped_at": str(item.get("scrapedAt")), 
    }

def fetch_results():
    client = ApifyClient(API_TOKEN)
    print(f"Fetching results for run: {RUN_ID}...")
    
    run = client.run(RUN_ID).get()
    dataset_id = run["defaultDatasetId"]
    print(f"Dataset ID: {dataset_id}")
    
    listings = []
    dataset_items = client.dataset(dataset_id).list_items().items
    
    print(f"Found {len(dataset_items)} items in dataset.")
    
    for item in dataset_items:
        try:
            mapped = map_apify_result(item)
            listings.append(mapped)
        except Exception as e:
            print(f"Error mapping item: {e}")

    output = {
        "scraped_at": str(run.get("finishedAt") or run.get("startedAt")),
        "total_listings": len(listings),
        "listings": listings,
    }
    
    with open(".tmp/scraped_cars.json", "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
        
    print(f"Saved {len(listings)} listings to .tmp/scraped_cars.json")

if __name__ == "__main__":
    fetch_results()
