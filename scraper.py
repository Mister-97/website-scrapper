"""
No-Website Business Scraper
Finds local businesses with no website listed on Google Maps.
Output: no_website_leads.csv
"""

import requests
import csv
import time
import os

API_KEY = os.environ.get("GOOGLE_API_KEY", "")
if not API_KEY:
    raise SystemExit("Set GOOGLE_API_KEY env var first: export GOOGLE_API_KEY=...")

CATEGORIES = [
    "restaurant",
    "nail salon",
    "barber shop",
    "hair salon",
    "auto repair",
    "plumber",
    "electrician",
    "landscaping",
    "daycare",
    "personal trainer",
    "tattoo shop",
    "dry cleaner",
    "alterations",
    "locksmith",
    "pest control",
    "painting contractor",
    "flooring",
    "roofing",
    "hvac",
    "cleaning service",
]

LOCATIONS = [
    "Naperville IL",
    "Schaumburg IL",
    "Elmhurst IL",
    "Orland Park IL",
    "Bolingbrook IL",
    "Aurora IL",
    "Joliet IL",
    "Oak Park IL",
    "Evanston IL",
    "Downers Grove IL",
]

TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
DETAILS_URL     = "https://maps.googleapis.com/maps/api/place/details/json"


def search_places(query, location):
    params = {
        "query":  f"{query} in {location}",
        "key":    API_KEY,
        "type":   "establishment",
    }
    results = []
    while True:
        r = requests.get(TEXT_SEARCH_URL, params=params, timeout=10)
        data = r.json()
        if data.get("status") not in ("OK", "ZERO_RESULTS"):
            print(f"  Search error: {data.get('status')} — {data.get('error_message','')}")
            break
        results.extend(data.get("results", []))
        token = data.get("next_page_token")
        if not token:
            break
        time.sleep(2)  # Google requires a short delay before next_page_token is valid
        params = {"pagetoken": token, "key": API_KEY}
    return results


def get_details(place_id):
    params = {
        "place_id": place_id,
        "fields":   "name,formatted_phone_number,formatted_address,website,business_status,url",
        "key":      API_KEY,
    }
    r = requests.get(DETAILS_URL, params=params, timeout=10)
    return r.json().get("result", {})


def main():
    seen_ids  = set()
    leads     = []
    total_checked = 0

    print(f"Searching {len(CATEGORIES)} categories across {len(LOCATIONS)} locations...\n")

    for location in LOCATIONS:
        for category in CATEGORIES:
            print(f"  {category} in {location}")
            places = search_places(category, location)
            for place in places:
                pid = place.get("place_id")
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)
                total_checked += 1

                details = get_details(pid)
                time.sleep(0.05)

                if details.get("business_status") == "CLOSED_PERMANENTLY":
                    continue

                website = details.get("website", "").strip()
                if website:
                    continue  # has a website, skip

                phone = details.get("formatted_phone_number", "").strip()
                if not phone:
                    continue  # no phone = can't call them anyway

                leads.append({
                    "Business Name": details.get("name", place.get("name", "")),
                    "Phone":         phone,
                    "Address":       details.get("formatted_address", ""),
                    "Category":      category,
                    "Location":      location,
                    "Google Maps":   details.get("url", ""),
                    "Website":       "NONE",
                })

            time.sleep(0.2)

    print(f"\nChecked {total_checked} unique businesses.")
    print(f"Found {len(leads)} with no website and a phone number.\n")

    if not leads:
        print("No leads found.")
        return

    output = "no_website_leads.csv"
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=leads[0].keys())
        writer.writeheader()
        writer.writerows(leads)

    print(f"Saved to {output}")
    print("\nTop 10 preview:")
    for lead in leads[:10]:
        print(f"  {lead['Business Name']} | {lead['Phone']} | {lead['Category']} | {lead['Location']}")


if __name__ == "__main__":
    main()
