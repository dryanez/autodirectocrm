#!/usr/bin/env python3
"""Test PATCH directly via subprocess curl (avoids terminal collision)."""
import subprocess, json

IMAGE_ID = "93c23c62-8e29-4ef4-8ee2-5a8f8695e771"
url = f"http://127.0.0.1:8080/api/vehicle-images/{IMAGE_ID}"

edits = {"zoom": 1.5, "panX": 10, "panY": -5, "brightness": 110, "contrast": 105, "saturate": 120}
body = json.dumps({"edits": edits})

result = subprocess.run(
    ["curl", "-s", "-w", "\n%{http_code}", "-X", "PATCH", url,
     "-H", "Content-Type: application/json", "-d", body],
    capture_output=True, text=True, timeout=15
)

lines = result.stdout.strip().split("\n")
http_code = lines[-1] if lines else "?"
body_text = "\n".join(lines[:-1])

print(f"HTTP Status: {http_code}")
print(f"Response body: {body_text}")

if http_code == "000":
    print("\n❌ CONNECTION REFUSED — server is not running!")
elif http_code == "200":
    print("\n✅ PATCH succeeded!")
    resp = json.loads(body_text)
    if resp.get("fallback"):
        print("   (used label-column fallback because 'edits' column doesn't exist)")
else:
    print(f"\n⚠️ Unexpected status: {http_code}")
