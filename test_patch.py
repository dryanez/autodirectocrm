#!/usr/bin/env python3
"""Quick test: PATCH vehicle-image edits via local Flask server."""
import requests, json, sys

IMAGE_ID = "93c23c62-8e29-4ef4-8ee2-5a8f8695e771"
URL = f"http://127.0.0.1:8080/api/vehicle-images/{IMAGE_ID}"

edits = {"zoom": 1.5, "panX": 10, "panY": -5, "brightness": 110, "contrast": 105, "saturate": 120}

print(f"PATCH {URL}")
print(f"Body: {json.dumps({'edits': edits})}")
print("---")

try:
    r = requests.patch(URL, json={"edits": edits}, timeout=10)
    print(f"Status: {r.status_code}")
    print(f"Response: {r.text}")
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)

# Now verify: check Supabase directly for label value
import os
SUPA_URL = os.environ.get("SUPABASE_URL", "https://kqympdxeszdyppbhtzbm.supabase.co")
SUPA_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtxeW1wZHhlc3pkeXBwYmh0emJtIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDc0OTc3MCwiZXhwIjoyMDg2MzI1NzcwfQ.6WoE2Y7Hzkbrn2xf0va_X57vd40q1zjkz2tWs_mPDyA")

print("\n--- Verifying in Supabase ---")
r2 = requests.get(
    SUPA_URL + "/rest/v1/vehicle_images",
    params={"select": "id,label,photo_type", "id": f"eq.{IMAGE_ID}"},
    headers={"apikey": SUPA_KEY, "Authorization": f"Bearer {SUPA_KEY}"},
    timeout=8
)
print(f"Supabase status: {r2.status_code}")
print(f"Supabase data: {r2.text}")
