#!/usr/bin/env python3
"""
Export Filtered Listings to Google Sheets
==========================================
Reads filtered car listings and pushes them to a Google Sheet.

Prerequisites:
  1. Create a Google Cloud project and enable Sheets API
  2. Create OAuth2 credentials → download as credentials.json to project root
  3. Run this script once to get the OAuth token (opens browser)
  4. Set GOOGLE_SHEET_ID in .env

Usage:
    python execution/export_to_sheets.py
    python execution/export_to_sheets.py --csv   # CSV fallback (no Google auth needed)
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# ─── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = PROJECT_ROOT / ".tmp"
INPUT_FILE = TMP_DIR / "filtered_cars.json"
CSV_OUTPUT = TMP_DIR / "leads.csv"
CREDENTIALS_FILE = PROJECT_ROOT / "credentials.json"
TOKEN_FILE = PROJECT_ROOT / "token.json"
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

# Columns for the export
COLUMNS = [
    "url",
    "title",
    "year",
    "price",
    "location",
    "region",
    "seller_name",
    "listed_date",
    "days_active",
    "messenger_link",
    "is_sold",
    "date_text",
    "_flag",
]

HEADER_ROW = [
    "Listing URL",
    "Title",
    "Year",
    "Price",
    "Location",
    "Region",
    "Seller",
    "Listed Date",
    "Days Active",
    "Messenger Link",
    "Sold?",
    "Date Text (Raw)",
    "Flags",
]


# ─── Google Sheets Export ──────────────────────────────────────────────────────

def export_to_google_sheets(listings: list[dict]):
    """Push listings to Google Sheets."""
    try:
        import gspread
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print("  ✗ Google Sheets deps missing. Run: pip install gspread google-auth google-auth-oauthlib")
        sys.exit(1)

    if not GOOGLE_SHEET_ID:
        print("  ✗ GOOGLE_SHEET_ID not set in .env")
        sys.exit(1)

    if not CREDENTIALS_FILE.exists():
        print(f"  ✗ credentials.json not found at {CREDENTIALS_FILE}")
        print("    Download OAuth2 credentials from Google Cloud Console")
        sys.exit(1)

    # Authenticate
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = None

    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDENTIALS_FILE), SCOPES
            )
            creds = flow.run_local_server(port=0)

        TOKEN_FILE.write_text(creds.to_json())
        print("  → Google auth token saved.")

    gc = gspread.authorize(creds)

    # Open sheet
    print(f"  → Opening Google Sheet: {GOOGLE_SHEET_ID}")
    spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)

    # Create or get worksheet
    worksheet_title = f"Leads {datetime.now().strftime('%Y-%m-%d')}"
    try:
        worksheet = spreadsheet.worksheet(worksheet_title)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=worksheet_title,
            rows=len(listings) + 10,
            cols=len(HEADER_ROW),
        )

    # Prepare data
    rows = [HEADER_ROW]
    for listing in listings:
        row = []
        for col in COLUMNS:
            val = listing.get(col, "")
            if val is None:
                val = ""
            elif isinstance(val, bool):
                val = "Sí" if val else "No"
            row.append(str(val))
        rows.append(row)

    # Write to sheet
    worksheet.update(range_name=f"A1:M{len(rows)}", values=rows)

    # Format header row (bold)
    worksheet.format("A1:M1", {
        "textFormat": {"bold": True},
        "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.3},
        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
    })

    print(f"  ✓ Exported {len(listings)} listings to sheet '{worksheet_title}'")
    print(f"  → https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}")


# ─── CSV Fallback Export ──────────────────────────────────────────────────────

def export_to_csv(listings: list[dict], output_path: Path = None):
    """Export listings to CSV file as fallback."""
    out = output_path or CSV_OUTPUT

    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER_ROW)

        for listing in listings:
            row = []
            for col in COLUMNS:
                val = listing.get(col, "")
                if val is None:
                    val = ""
                elif isinstance(val, bool):
                    val = "Sí" if val else "No"
                row.append(str(val))
            writer.writerow(row)

    print(f"  ✓ Exported {len(listings)} listings to {out}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Export filtered listings")
    parser.add_argument("--csv", action="store_true",
                        help="Export to CSV instead of Google Sheets")
    parser.add_argument("--input", type=str, default=None,
                        help="Path to filtered JSON (default: .tmp/filtered_cars.json)")
    parser.add_argument("--output", type=str, default=None,
                        help="Path for CSV output (only used with --csv)")
    args = parser.parse_args()

    input_file = Path(args.input) if args.input else INPUT_FILE

    # ── Load ───────────────────────────────────────────────────────────────
    if not input_file.exists():
        print(f"✗ Input file not found: {input_file}")
        print(f"  Run the filter first: python execution/filter_listings.py")
        sys.exit(1)

    data = json.loads(input_file.read_text())
    listings = data.get("listings", [])

    print(f"{'='*60}")
    print(f"  EXPORT LISTINGS")
    print(f"{'='*60}")
    print(f"  Input:    {input_file}")
    print(f"  Listings: {len(listings)}")
    print(f"  Mode:     {'CSV' if args.csv else 'Google Sheets'}")

    if len(listings) == 0:
        print("\n  ⚠ No listings to export!")
        return

    # ── Export ─────────────────────────────────────────────────────────────
    if args.csv:
        output_path = Path(args.output) if args.output else CSV_OUTPUT
        export_to_csv(listings, output_path)
    else:
        export_to_google_sheets(listings)

    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
