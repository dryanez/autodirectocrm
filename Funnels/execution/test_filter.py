#!/usr/bin/env python3
"""
Unit tests for filter_listings.py
===================================
Tests the filter logic with synthetic sample data.

Usage:
    python execution/test_filter.py
"""

import json
import sys
from pathlib import Path

# Add parent to path so we can import
sys.path.insert(0, str(Path(__file__).resolve().parent))

from filter_listings import filter_listings, sort_listings


def create_sample_listings():
    """Create synthetic test data covering all filter edge cases."""
    return [
        # ✓ Should pass: 2020 car, 20 days active, not sold
        {
            "url": "https://facebook.com/marketplace/item/001",
            "title": "Toyota Corolla 2020 - Excelente estado",
            "price": "$8.500.000",
            "year": 2020,
            "location": "Santiago",
            "region": "Región Metropolitana (Santiago)",
            "listed_date": "2026-01-28",
            "days_active": 20,
            "is_sold": False,
            "seller_name": "Juan Pérez",
        },
        # ✓ Should pass: 2015 car (minimum), exactly 14 days
        {
            "url": "https://facebook.com/marketplace/item/002",
            "title": "Hyundai Accent 2015",
            "price": "$5.000.000",
            "year": 2015,
            "location": "Valparaíso",
            "region": "V Región (Valparaíso)",
            "listed_date": "2026-02-03",
            "days_active": 14,
            "is_sold": False,
            "seller_name": "María González",
        },
        # ✗ Should be REMOVED: year 2012 (below minimum)
        {
            "url": "https://facebook.com/marketplace/item/003",
            "title": "Suzuki Swift 2012",
            "price": "$3.500.000",
            "year": 2012,
            "location": "La Serena",
            "region": "IV Región (Coquimbo / La Serena)",
            "listed_date": "2026-01-15",
            "days_active": 33,
            "is_sold": False,
            "seller_name": "Carlos Muñoz",
        },
        # ✗ Should be REMOVED: only 5 days active (too recent)
        {
            "url": "https://facebook.com/marketplace/item/004",
            "title": "Mazda 3 2022",
            "price": "$12.000.000",
            "year": 2022,
            "location": "Santiago",
            "region": "Región Metropolitana (Santiago)",
            "listed_date": "2026-02-12",
            "days_active": 5,
            "is_sold": False,
            "seller_name": "Ana Soto",
        },
        # ✗ Should be REMOVED: marked as sold
        {
            "url": "https://facebook.com/marketplace/item/005",
            "title": "Kia Cerato 2019 - VENDIDO",
            "price": "$7.000.000",
            "year": 2019,
            "location": "Viña del Mar",
            "region": "V Región (Valparaíso)",
            "listed_date": "2026-01-20",
            "days_active": 28,
            "is_sold": True,
            "seller_name": "Pedro Rojas",
        },
        # ✓ Should pass (with flag): no year detected
        {
            "url": "https://facebook.com/marketplace/item/006",
            "title": "Auto en venta - Excelente precio",
            "price": "$6.000.000",
            "year": None,
            "location": "Coquimbo",
            "region": "IV Región (Coquimbo / La Serena)",
            "listed_date": "2026-01-25",
            "days_active": 23,
            "is_sold": False,
            "seller_name": "Luis Vargas",
        },
        # ✓ Should pass (with flag): no date info
        {
            "url": "https://facebook.com/marketplace/item/007",
            "title": "Chevrolet Sail 2018",
            "price": "$4.500.000",
            "year": 2018,
            "location": "Santiago",
            "region": "Región Metropolitana (Santiago)",
            "listed_date": None,
            "days_active": None,
            "is_sold": False,
            "seller_name": "Roberto Díaz",
        },
        # ✗ Should be REMOVED: duplicate of item 001
        {
            "url": "https://facebook.com/marketplace/item/001",
            "title": "Toyota Corolla 2020 - Excelente estado",
            "price": "$8.500.000",
            "year": 2020,
            "location": "Santiago",
            "region": "Región Metropolitana (Santiago)",
            "listed_date": "2026-01-28",
            "days_active": 20,
            "is_sold": False,
            "seller_name": "Juan Pérez",
        },
        # ✓ Should pass: 2023 car, 30 days active
        {
            "url": "https://facebook.com/marketplace/item/008",
            "title": "Nissan Kicks 2023 - Full Equipo",
            "price": "$15.000.000",
            "year": 2023,
            "location": "Valparaíso",
            "region": "V Región (Valparaíso)",
            "listed_date": "2026-01-18",
            "days_active": 30,
            "is_sold": False,
            "seller_name": "Andrea Molina",
        },
    ]


def test_basic_filtering():
    """Test that all filter rules work correctly."""
    listings = create_sample_listings()
    filtered, stats = filter_listings(listings, min_year=2015, min_days_active=14)

    # Expected: items 001, 002, 006 (flagged), 007 (flagged), 008
    # Removed: 003 (year), 004 (too new), 005 (sold), duplicate 001

    assert stats["total_input"] == 9, f"Expected 9 input, got {stats['total_input']}"
    assert stats["removed_sold"] == 1, f"Expected 1 sold removed, got {stats['removed_sold']}"
    assert stats["removed_too_new_listing"] == 1, f"Expected 1 too-new removed, got {stats['removed_too_new_listing']}"
    assert stats["removed_year_too_old"] == 1, f"Expected 1 year-old removed, got {stats['removed_year_too_old']}"
    assert stats["removed_duplicate"] == 1, f"Expected 1 duplicate removed, got {stats['removed_duplicate']}"
    assert stats["total_output"] == 5, f"Expected 5 output, got {stats['total_output']}"

    # Check URLs of remaining listings
    remaining_urls = {l["url"] for l in filtered}
    assert "https://facebook.com/marketplace/item/001" in remaining_urls
    assert "https://facebook.com/marketplace/item/002" in remaining_urls
    assert "https://facebook.com/marketplace/item/006" in remaining_urls
    assert "https://facebook.com/marketplace/item/007" in remaining_urls
    assert "https://facebook.com/marketplace/item/008" in remaining_urls

    # Check that removed ones are gone
    assert "https://facebook.com/marketplace/item/003" not in remaining_urls  # year
    assert "https://facebook.com/marketplace/item/004" not in remaining_urls  # too new
    assert "https://facebook.com/marketplace/item/005" not in remaining_urls  # sold

    print("  ✓ test_basic_filtering PASSED")


def test_flagged_listings():
    """Test that listings with missing data are flagged, not dropped."""
    listings = create_sample_listings()
    filtered, stats = filter_listings(listings)

    # Find flagged listings
    flagged = [l for l in filtered if l.get("_flag")]
    assert len(flagged) >= 2, f"Expected ≥2 flagged, got {len(flagged)}"

    # Item 006 should have no_year_detected flag
    item_006 = next(l for l in filtered if l["url"].endswith("/006"))
    assert "no_year" in item_006.get("_flag", ""), \
        f"Item 006 should be flagged for no year, got: {item_006.get('_flag')}"

    # Item 007 should have no_date flag
    item_007 = next(l for l in filtered if l["url"].endswith("/007"))
    assert "no_date" in item_007.get("_flag", ""), \
        f"Item 007 should be flagged for no date, got: {item_007.get('_flag')}"

    print("  ✓ test_flagged_listings PASSED")


def test_sorting():
    """Test that sort puts oldest (most days active) first."""
    listings = create_sample_listings()
    filtered, _ = filter_listings(listings)
    sorted_listings = sort_listings(filtered, sort_by="days_active")

    # First listing should have the highest days_active
    days = [l.get("days_active") for l in sorted_listings if l.get("days_active") is not None]
    assert days == sorted(days, reverse=True), \
        f"Listings not sorted by days_active descending: {days}"

    print("  ✓ test_sorting PASSED")


def test_custom_thresholds():
    """Test with different min_year and min_days values."""
    listings = create_sample_listings()

    # More restrictive: year >= 2020, days >= 20
    filtered, stats = filter_listings(listings, min_year=2020, min_days_active=20)

    remaining_years = [l.get("year") for l in filtered if l.get("year")]
    for y in remaining_years:
        assert y >= 2020, f"Found year {y} which is < 2020"

    remaining_days = [l.get("days_active") for l in filtered if l.get("days_active")]
    for d in remaining_days:
        assert d >= 20, f"Found days_active {d} which is < 20"

    print("  ✓ test_custom_thresholds PASSED")


def test_include_sold():
    """Test that include_sold flag keeps sold listings."""
    listings = create_sample_listings()
    filtered, stats = filter_listings(listings, exclude_sold=False)
    assert stats["removed_sold"] == 0, "No sold listings should be removed when exclude_sold=False"

    print("  ✓ test_include_sold PASSED")


# ─── Run ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  FILTER LOGIC — UNIT TESTS")
    print("=" * 60)

    tests = [
        test_basic_filtering,
        test_flagged_listings,
        test_sorting,
        test_custom_thresholds,
        test_include_sold,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"  ✗ {test.__name__} FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"  ✗ {test.__name__} ERROR: {e}")
            failed += 1

    print(f"\n  {'─'*40}")
    print(f"  Results: {passed} passed, {failed} failed")

    if failed > 0:
        sys.exit(1)
    else:
        print("  ✓ All tests passed!")
        sys.exit(0)
