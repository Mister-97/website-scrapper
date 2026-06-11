"""
No-Website Business Scraper — Google Maps via Playwright
Finds businesses with no website listed. Outputs no_website_leads.csv
Run: python scraper_playwright.py
"""

import asyncio
import csv
import re
import time
from playwright.async_api import async_playwright

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
    "tattoo shop",
    "dry cleaner",
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
    "Downers Grove IL",
]

OUTPUT_FILE = "no_website_leads.csv"


async def scrape_category(page, category, location, seen_names):
    leads = []
    query = f"{category} in {location}"
    url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"

    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(2500)

    # Scroll the results panel to load more listings
    results_panel = page.locator('div[role="feed"]')
    for _ in range(5):
        await results_panel.evaluate("el => el.scrollBy(0, 800)")
        await page.wait_for_timeout(800)

    # Collect all result links
    listings = await page.locator('a[href*="/maps/place/"]').all()
    hrefs = []
    seen_hrefs = set()
    for listing in listings:
        href = await listing.get_attribute("href")
        if href and href not in seen_hrefs:
            seen_hrefs.add(href)
            hrefs.append(href)

    print(f"  Found {len(hrefs)} listings for {category} in {location}")

    for href in hrefs[:25]:  # cap at 25 per search to stay fast
        try:
            await page.goto(href, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(1500)

            # Business name
            name_el = page.locator('h1').first
            name = (await name_el.inner_text()).strip() if await name_el.count() > 0 else ""
            if not name or name in seen_names:
                continue

            # Check for website button — if present, skip
            website_btn = page.locator('a[data-item-id="authority"]')
            has_website = await website_btn.count() > 0

            if has_website:
                seen_names.add(name)
                continue

            # Phone number
            phone_el = page.locator('button[data-item-id*="phone"]')
            phone = ""
            if await phone_el.count() > 0:
                phone_text = await phone_el.first.get_attribute("aria-label") or ""
                phone = phone_text.replace("Phone:", "").strip()
                # fallback: read visible text
                if not phone:
                    phone = (await phone_el.first.inner_text()).strip()

            if not phone:
                seen_names.add(name)
                continue  # can't call them anyway

            # Address
            address_el = page.locator('button[data-item-id="address"]')
            address = ""
            if await address_el.count() > 0:
                addr_label = await address_el.first.get_attribute("aria-label") or ""
                address = addr_label.replace("Address:", "").strip()
                if not address:
                    address = (await address_el.first.inner_text()).strip()

            # Logo / profile image from Maps listing
            logo_url = ""
            try:
                # Google Maps shows the business profile photo as a button with an img inside
                img_el = page.locator('button[jsaction*="heroHeaderImage"] img, img[decoding="async"][src*="googleusercontent"]').first
                if await img_el.count() > 0:
                    src = await img_el.get_attribute("src") or ""
                    if src.startswith("http"):
                        # Request a larger version by bumping the size param
                        logo_url = src.split("=")[0] + "=s400-c" if "=" in src else src
            except Exception:
                pass

            seen_names.add(name)
            leads.append({
                "Business Name": name,
                "Phone":         phone,
                "Address":       address,
                "Category":      category,
                "Location":      location,
                "Google Maps":   page.url,
                "Logo URL":      logo_url,
                "Website":       "NONE",
            })
            print(f"    + {name} | {phone}")

        except Exception as e:
            continue

    return leads


async def main():
    all_leads = []
    seen_names = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # headless=True to run silently
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        )
        page = await context.new_page()

        for location in LOCATIONS:
            for category in CATEGORIES:
                print(f"\n[{category}] in [{location}]")
                try:
                    leads = await scrape_category(page, category, location, seen_names)
                    all_leads.extend(leads)
                except Exception as e:
                    print(f"  Error: {e}")
                await asyncio.sleep(1)

        await browser.close()

    print(f"\n{'='*50}")
    print(f"Total no-website leads found: {len(all_leads)}")

    if all_leads:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_leads[0].keys())
            writer.writeheader()
            writer.writerows(all_leads)
        print(f"Saved to {OUTPUT_FILE}")
    else:
        print("No leads found.")


if __name__ == "__main__":
    asyncio.run(main())
