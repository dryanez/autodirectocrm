# Facebook Marketplace Car Funnel — Chile

## Goal

Scrape Facebook Marketplace for used cars (2015+) in Chile's IV Region (Coquimbo), V Region (Valparaíso), and Región Metropolitana (Santiago). Identify listings that have been active for at least 2 weeks and have NOT been marked as sold. Collect seller contact info and listing details so an outbound sales team can reach out.

## Target Criteria

| Filter              | Value                                                      |
|---------------------|------------------------------------------------------------|
| Platform            | Facebook Marketplace                                       |
| Category            | Vehicles → Cars                                            |
| Year                | 2015 or newer                                              |
| Regions             | IV Región (Coquimbo), V Región (Valparaíso), RM (Santiago) |
| Listing Status      | NOT marked as sold                                         |
| Listing Age         | Active for ≥ 14 days (not sold in last 2 weeks)            |

## Inputs

- Facebook login credentials (stored in `.env`)
- Region search URLs or location coordinates for each target region
- Date threshold: today − 14 days

## Outputs

- **Google Sheet** (deliverable) with columns:
  - Listing URL
  - Title / Car description
  - Year
  - Price
  - Location / Region
  - Seller name
  - Listing date
  - Days active
  - Contact link / Messenger link
  - Status (active / sold)
- **.tmp/scraped_cars.json** — raw intermediate data

## Tools / Execution Scripts

| Script                         | Purpose                                         |
|--------------------------------|-------------------------------------------------|
| `execution/scrape_fb_marketplace.py` | Scrape listings from FB Marketplace       |
| `execution/filter_listings.py`       | Filter by year ≥ 2015, age ≥ 14 days, not sold |
| `execution/export_to_sheets.py`      | Push filtered results to Google Sheets    |

## Process

1. **Login** to Facebook using credentials from `.env`
2. **Navigate** to Marketplace → Vehicles → Cars for each target region
3. **Scrape** all listings: title, price, year, location, seller, listing date, status
4. **Filter**: keep only year ≥ 2015, listed ≥ 14 days, not sold
5. **Deduplicate** by listing URL
6. **Export** to Google Sheets
7. **Log** run timestamp and count

## Edge Cases & Learnings

- Facebook may rate-limit or block scraping → use delays, rotate user-agent, consider Playwright with stealth
- Listing dates may be relative ("2 weeks ago") — normalize to absolute dates
- Some listings may not show year — skip or flag for manual review
- Facebook's DOM structure changes frequently — selectors will need maintenance
- Consider using Facebook's Graph API if marketplace data is accessible (check permissions)

## Important: Legal & Compliance

- Scraping Facebook may violate their Terms of Service
- Consider using official APIs or approved data sources where possible
- Ensure compliance with Chilean data protection laws (Ley 19.628)
