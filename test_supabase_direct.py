#!/usr/bin/env python3
"""Test saving edits DIRECTLY to Supabase (bypass Flask)."""
import requests, json

SUPA_URL = "https://kqympdxeszdyppbhtzbm.supabase.co"
SUPA_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtxeW1wZHhlc3pkeXBwYmh0emJtIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDc0OTc3MCwiZXhwIjoyMDg2MzI1NzcwfQ.6WoE2Y7Hzkbrn2xf0va_X57vd40q1zjkz2tWs_mPDyA"
IMAGE_ID = "93c23c62-8e29-4ef4-8ee2-5a8f8695e771"
HEADERS = {
    "apikey": SUPA_KEY,
    "Authorization": f"Bearer {SUPA_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

edits = {"zoom": 1.5, "panX": 10, "panY": -5, "brightness": 110, "contrast": 105, "saturate": 120}

# 1. Try writing to 'edits' column directly
print("=== TEST 1: Try PATCH with 'edits' column ===")
r = requests.patch(
    f"{SUPA_URL}/rest/v1/vehicle_images",
    params={"id": f"eq.{IMAGE_ID}"},
    json={"edits": edits},
    headers=HEADERS, timeout=10
)
print(f"Status: {r.status_code}")
print(f"Response: {r.text}")

resp = None
try:
    resp = r.json()
except:
    print("(no JSON body — likely 204 success)")

# Check if it's an error
error_code = resp.get("code") if isinstance(resp, dict) else None
print(f"Error code: {error_code}")

# 2. Fallback: write to 'label' column with prefix
if error_code in ("42703", "PGRST204"):
    print("\n=== TEST 2: Fallback to 'label' column ===")
    label_value = "__edits__:" + json.dumps(edits)
    r2 = requests.patch(
        f"{SUPA_URL}/rest/v1/vehicle_images",
        params={"id": f"eq.{IMAGE_ID}"},
        json={"label": label_value},
        headers=HEADERS, timeout=10
    )
    print(f"Status: {r2.status_code}")
    print(f"Response: {r2.text}")
else:
    print("\nNo fallback needed (edits column exists or different error)")

# 3. Verify what's stored
print("\n=== VERIFY: Read back from Supabase ===")
r3 = requests.get(
    f"{SUPA_URL}/rest/v1/vehicle_images",
    params={"select": "id,label,photo_type", "id": f"eq.{IMAGE_ID}"},
    headers={
        "apikey": SUPA_KEY,
        "Authorization": f"Bearer {SUPA_KEY}",
    }, timeout=10
)
print(f"Status: {r3.status_code}")
data = r3.json()
print(f"Data: {json.dumps(data, indent=2)}")

# Parse label if it has edits
if data and isinstance(data, list) and data[0].get("label", "").startswith("__edits__:"):
    parsed = json.loads(data[0]["label"][len("__edits__:"):])
    print(f"\n✅ Parsed edits from label: {json.dumps(parsed, indent=2)}")
