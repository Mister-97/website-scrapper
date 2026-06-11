"""
Lead Gen Dashboard — FastAPI backend
Run: python app.py
Open: http://localhost:8000
"""

import asyncio
import csv
import io
import json
import os
import re
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import uvicorn
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

DB_PATH    = "leads.db"
CONFIG_PATH = "config.json"
scraper_running = False
scraper_log: list[str] = []


# ── Config (Twilio creds stored locally) ──────────────────────────────────────

def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}

def save_config(data: dict):
    existing = load_config()
    existing.update(data)
    with open(CONFIG_PATH, "w") as f:
        json.dump(existing, f, indent=2)


# ── Database ──────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS leads (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            phone        TEXT,
            address      TEXT,
            category     TEXT,
            location     TEXT,
            maps_url     TEXT,
            logo_url     TEXT,
            preview_url  TEXT,
            status       TEXT DEFAULT 'new',
            notes        TEXT DEFAULT '',
            sms_sent     INTEGER DEFAULT 0,
            created_at   TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS deals (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id  INTEGER REFERENCES leads(id),
            business TEXT NOT NULL,
            amount   REAL NOT NULL,
            date     TEXT DEFAULT (date('now')),
            notes    TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS sms_log (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id   INTEGER REFERENCES leads(id),
            phone     TEXT,
            message   TEXT,
            status    TEXT,
            sent_at   TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    # Add sequence columns if they don't exist yet
    for col, typedef in [("sequence_step","INTEGER DEFAULT 0"), ("sequence_active","INTEGER DEFAULT 0"), ("follow_up_at","TEXT")]:
        try:
            conn.execute(f"ALTER TABLE leads ADD COLUMN {col} {typedef}")
            conn.commit()
        except Exception:
            pass
    conn.close()


# ── Sequence templates ────────────────────────────────────────────────────────

SEQUENCE = [
    "Hey I was searching for {name} but couldn't find a website, do you guys have one?",
    "Yeah I actually built you a free preview to show you what it could look like. Want to see it? {preview_url}",
    "No worries if not, just didn't want you losing customers to competitors who have sites.",
    "Last one I promise. I'm about to give this slot to another {category} in {city}, wanted to check if you wanted it first.",
]

SEQUENCE_DELAYS = [0, 1, 3, 5]  # days after sequence start


def classify_reply(text: str) -> str:
    """Return 'no_website', 'has_website', or 'other'."""
    t = text.lower().strip()
    # A URL anywhere in the reply means they have a site, even if the
    # message also contains a "no" ("no worries, we have one at joes.com")
    url_patterns = [r"https?://", r"www\.", r"\.com\b", r"\.net\b", r"\.org\b"]
    for p in url_patterns:
        if re.search(p, t):
            return "has_website"
    no_patterns = [
        r"\bno\b", r"\bnope\b", r"\bnah\b", r"\bnot yet\b",
        r"\bdon'?t have\b", r"\bdo not have\b",
        r"\bwe don'?t\b", r"\bwe do not\b",
        r"\bno website\b", r"\bno web\b",
        r"\bwe haven'?t\b", r"\bdon'?t have one\b",
        r"\bnot have\b", r"\bno we don'?t\b",
    ]
    yes_patterns = [
        r"\byes\b", r"\byeah\b", r"\byep\b",
        r"\bwe do\b", r"\bwe have\b", r"\bwe got\b",
        r"\byes we\b", r"\bwe already\b", r"\balready have\b",
    ]
    for p in no_patterns:
        if re.search(p, t):
            return "no_website"
    for p in yes_patterns:
        if re.search(p, t):
            return "has_website"
    return "other"


# ── App setup ─────────────────────────────────────────────────────────────────

async def sequence_loop():
    """Background loop: send due follow-ups every 30 minutes."""
    while True:
        await asyncio.sleep(1800)
        try:
            sent = await process_due_sequences()
            if sent:
                print(f"[scheduler] sent {sent} follow-up texts")
        except Exception as e:
            print(f"[scheduler] error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    os.makedirs("static", exist_ok=True)
    os.makedirs("previews", exist_ok=True)
    app.mount("/previews", StaticFiles(directory="previews"), name="previews")
    task = asyncio.create_task(sequence_loop())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)


# ── Scraper ───────────────────────────────────────────────────────────────────

async def run_scraper(categories: list[str], locations: list[str]):
    global scraper_running, scraper_log
    scraper_running = True
    scraper_log = []

    def log(msg):
        ts = datetime.now().strftime("%H:%M:%S")
        scraper_log.append(f"[{ts}] {msg}")

    try:
        from playwright.async_api import async_playwright
        log(f"Starting: {len(categories)} industries x {len(locations)} locations")
        # Dedupe by (name, location) so same-named shops in different towns both get scraped
        seen_names = set()
        conn = get_db()
        for row in conn.execute("SELECT name, location FROM leads"):
            seen_names.add((row["name"], row["location"]))
        conn.close()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            )
            page = await context.new_page()

            for location in locations:
                for category in categories:
                    log(f"Searching: {category} in {location}")
                    found = 0
                    try:
                        query = f"{category} in {location}"
                        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
                        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        await page.wait_for_timeout(2500)

                        panel = page.locator('div[role="feed"]')
                        for _ in range(5):
                            await panel.evaluate("el => el.scrollBy(0, 800)")
                            await page.wait_for_timeout(600)

                        listings = await page.locator('a[href*="/maps/place/"]').all()
                        hrefs, seen_hrefs = [], set()
                        for l in listings:
                            href = await l.get_attribute("href")
                            if href and href not in seen_hrefs:
                                seen_hrefs.add(href)
                                hrefs.append(href)

                        for href in hrefs[:25]:
                            try:
                                await page.goto(href, wait_until="domcontentloaded", timeout=20000)
                                await page.wait_for_timeout(1200)

                                name_el = page.locator("h1").first
                                name = (await name_el.inner_text()).strip() if await name_el.count() > 0 else ""
                                if not name or (name, location) in seen_names:
                                    continue

                                if await page.locator('a[data-item-id="authority"]').count() > 0:
                                    seen_names.add((name, location))
                                    continue

                                phone_el = page.locator('button[data-item-id*="phone"]')
                                phone = ""
                                if await phone_el.count() > 0:
                                    phone = (await phone_el.first.get_attribute("aria-label") or "").replace("Phone:", "").strip()
                                    if not phone:
                                        phone = (await phone_el.first.inner_text()).strip()
                                if not phone:
                                    seen_names.add((name, location))
                                    continue

                                address_el = page.locator('button[data-item-id="address"]')
                                address = ""
                                if await address_el.count() > 0:
                                    address = (await address_el.first.get_attribute("aria-label") or "").replace("Address:", "").strip()

                                logo_url = ""
                                try:
                                    img_el = page.locator('button[jsaction*="heroHeaderImage"] img, img[decoding="async"][src*="googleusercontent"]').first
                                    if await img_el.count() > 0:
                                        src = await img_el.get_attribute("src") or ""
                                        if src.startswith("http"):
                                            logo_url = src.split("=")[0] + "=s400-c" if "=" in src else src
                                except Exception:
                                    pass

                                seen_names.add((name, location))
                                conn = get_db()
                                conn.execute(
                                    "INSERT INTO leads (name, phone, address, category, location, maps_url, logo_url) VALUES (?,?,?,?,?,?,?)",
                                    (name, phone, address, category, location, page.url, logo_url)
                                )
                                conn.commit()
                                conn.close()
                                found += 1
                                log(f"  + {name} | {phone}")
                            except Exception:
                                continue

                        log(f"  Done — {found} new leads")
                    except Exception as e:
                        log(f"  Error: {e}")
                    await asyncio.sleep(0.5)

            await browser.close()

        total = get_db().execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        log(f"Scrape complete. Total leads: {total}")
    except Exception as e:
        log(f"Fatal error: {e}")
    finally:
        scraper_running = False


# ── Preview generator ─────────────────────────────────────────────────────────

CATEGORY_ACCENTS = {
    "restaurant": "#e63946",
    "nail salon": "#c77dff",
    "barber shop": "#2196f3",
    "hair salon": "#9c27b0",
    "auto repair": "#ff6b35",
    "plumber": "#0096c7",
    "electrician": "#f59e0b",
    "landscaping": "#2d9a4e",
    "daycare": "#ff9800",
    "tattoo shop": "#e63946",
    "dry cleaner": "#00b4d8",
    "locksmith": "#ffb300",
    "pest control": "#57cc99",
    "painting contractor": "#ff5722",
    "flooring": "#a0785a",
    "roofing": "#546e7a",
    "hvac": "#0096c7",
    "cleaning service": "#00b4d8",
}

INDUSTRY_DATA = {
    "auto repair": {
        "headline": "Fast, Honest Auto Repair",
        "sub": "From oil changes to engine diagnostics, we keep your car running right. No upsells, no surprises.",
        "services": [("Oil Change", "🔧"), ("Brake Service", "🛑"), ("Tire Rotation", "⚙️"), ("Engine Diagnostics", "🔍"), ("AC Repair", "❄️"), ("Transmission", "⚙️")],
        "trust": ["Same-Day Service Available", "ASE-Certified Technicians", "Warranty on All Work"],
        "photo": "photo-1625047509248-ec889cbff17f",
    },
    "nail salon": {
        "headline": "Beautiful Nails, Every Time",
        "sub": "Expert nail care in a clean, relaxing space. Gel, acrylics, nail art and more. Walk-ins always welcome.",
        "services": [("Manicure", "💅"), ("Pedicure", "👣"), ("Gel Nails", "✨"), ("Acrylic Nails", "💎"), ("Nail Art", "🎨"), ("Waxing", "🌿")],
        "trust": ["Walk-Ins Welcome", "Sterile Tools Every Visit", "Expert Nail Artists"],
        "photo": "photo-1604654894610-df63bc536371",
    },
    "barber shop": {
        "headline": "Fresh Cuts, Every Time",
        "sub": "Old-school craft, modern style. Walk in looking good, walk out looking great. Kids and adults welcome.",
        "services": [("Haircut", "✂️"), ("Fade", "💈"), ("Beard Trim", "🧔"), ("Line Up", "📐"), ("Kid's Cut", "👦"), ("Hot Towel Shave", "🪒")],
        "trust": ["Walk-Ins Welcome", "Experienced Barbers", "Kids & Adults"],
        "photo": "photo-1503951914875-452162b0f3f1",
    },
    "hair salon": {
        "headline": "Your Best Hair Starts Here",
        "sub": "Expert cuts, color, and styling. Walk out looking and feeling like a new person.",
        "services": [("Haircut & Style", "✂️"), ("Color & Highlights", "🎨"), ("Blowout", "💨"), ("Keratin Treatment", "✨"), ("Extensions", "💇"), ("Bridal", "👰")],
        "trust": ["Licensed Stylists", "Premium Products Only", "Online Booking Available"],
        "photo": "photo-1560066984-138dadb4c035",
    },
    "restaurant": {
        "headline": "Great Food, Great People",
        "sub": "Fresh ingredients, bold flavors, friendly service. Dine in or take out. We are ready when you are.",
        "services": [("Dine-In", "🍽️"), ("Takeout", "🥡"), ("Catering", "🎉"), ("Private Events", "🥂"), ("Delivery", "🛵"), ("Happy Hour", "🍻")],
        "trust": ["Fresh Made Daily", "Family Friendly", "Private Party Room"],
        "photo": "photo-1414235077428-338989a2e8c0",
    },
    "plumber": {
        "headline": "Plumbing Done Right",
        "sub": "Fast response, fair prices, licensed plumbers. We fix it the first time, every time.",
        "services": [("Drain Cleaning", "🚿"), ("Pipe Repair", "🔧"), ("Water Heater", "♨️"), ("Leak Detection", "💧"), ("Bathroom Remodel", "🛁"), ("Emergency Service", "🚨")],
        "trust": ["24/7 Emergency Service", "Licensed & Insured", "Upfront Pricing"],
        "photo": "photo-1588618656479-10e10dbebddf",
    },
    "electrician": {
        "headline": "Safe, Reliable Electrical Work",
        "sub": "Code-compliant electrical work by licensed pros. Call us first and get it done right.",
        "services": [("Panel Upgrade", "⚡"), ("Outlet Installation", "🔌"), ("Lighting", "💡"), ("EV Charger", "🚗"), ("Safety Inspection", "🔍"), ("Rewiring", "🔧")],
        "trust": ["Licensed & Bonded", "Same-Day Available", "Free Estimates"],
        "photo": "photo-1621905251189-08b45d6a269e",
    },
    "landscaping": {
        "headline": "Yards That Stand Out",
        "sub": "Professional lawn care and landscaping that makes your property the best on the block.",
        "services": [("Lawn Mowing", "🌿"), ("Mulching", "🍂"), ("Tree Trimming", "🌳"), ("Planting", "🌸"), ("Snow Removal", "❄️"), ("Irrigation", "💧")],
        "trust": ["Licensed & Insured", "Weekly or Monthly Plans", "Free Estimates"],
        "photo": "photo-1416879595882-3373a0480b5b",
    },
    "daycare": {
        "headline": "Safe, Loving Care Every Day",
        "sub": "A nurturing environment where your child learns, grows, and thrives. Trusted by local families.",
        "services": [("Infant Care", "👶"), ("Toddler Program", "🧒"), ("Preschool", "📚"), ("After School", "🏫"), ("Summer Camp", "☀️"), ("Drop-In Care", "🏠")],
        "trust": ["Licensed & Certified", "Low Child-to-Staff Ratio", "Nutritious Meals Included"],
        "photo": "photo-1503454537195-1dcabb73ffb9",
    },
    "tattoo shop": {
        "headline": "Custom Ink You Will Love",
        "sub": "Experienced artists, sterile environment, artwork you will be proud of for life.",
        "services": [("Custom Tattoos", "🎨"), ("Cover-Ups", "🔄"), ("Touch-Ups", "✨"), ("Piercings", "💎"), ("Flash Tattoos", "⚡"), ("Free Consult", "💬")],
        "trust": ["Licensed Artists", "Sterile Equipment", "Free Consultations"],
        "photo": "photo-1565058379802-bbe93b2f703a",
    },
    "cleaning service": {
        "headline": "Spotless Homes, Guaranteed",
        "sub": "Professional cleaning services that save you time and leave your home gleaming.",
        "services": [("Standard Cleaning", "🏠"), ("Deep Cleaning", "✨"), ("Move-In/Out", "📦"), ("Office Cleaning", "🏢"), ("Window Cleaning", "🪟"), ("Carpet Cleaning", "🧹")],
        "trust": ["Vetted & Insured Cleaners", "Eco-Friendly Products", "Satisfaction Guaranteed"],
        "photo": "photo-1563453392212-326f5e854473",
    },
    "locksmith": {
        "headline": "Locked Out? We Are On the Way",
        "sub": "Fast lockout service, lock replacement, and key duplication. There in 30 minutes or less.",
        "services": [("Lockout Service", "🔓"), ("Lock Replacement", "🔒"), ("Key Duplication", "🗝️"), ("Deadbolt Install", "🚪"), ("Car Lockout", "🚗"), ("Rekeying", "🔑")],
        "trust": ["30-Min Response Time", "24/7 Emergency Service", "Licensed & Bonded"],
        "photo": "photo-1586769852044-692d6e3703f0",
    },
    "pest control": {
        "headline": "Pest-Free Homes, Guaranteed",
        "sub": "We eliminate pests for good. Safe for kids and pets. Results guaranteed or we come back free.",
        "services": [("Ant Control", "🐜"), ("Rodent Removal", "🐭"), ("Bed Bugs", "🐛"), ("Termite Inspection", "🔍"), ("Mosquito Control", "🦟"), ("Wasp Removal", "🐝")],
        "trust": ["Pet-Safe Treatments", "Guaranteed Results", "Free Inspections"],
        "photo": "photo-1516822003754-cca485356ecb",
    },
    "painting contractor": {
        "headline": "Professional Painters You Can Trust",
        "sub": "Interior and exterior painting done clean, fast, and right the first time.",
        "services": [("Interior Painting", "🖌️"), ("Exterior Painting", "🏠"), ("Cabinet Painting", "🚪"), ("Deck Staining", "🌿"), ("Pressure Washing", "💦"), ("Color Consulting", "🎨")],
        "trust": ["Licensed & Insured", "Free Color Consultation", "2-Year Guarantee"],
        "photo": "photo-1562259949-e8e7689d7828",
    },
    "roofing": {
        "headline": "Trusted Roofers, Local Crew",
        "sub": "Roof repair, replacement, and inspection done by certified local roofers. We work with your insurance.",
        "services": [("Roof Repair", "🔧"), ("Roof Replacement", "🏠"), ("Shingle Install", "🧱"), ("Leak Detection", "💧"), ("Gutter Service", "🌧️"), ("Storm Damage", "⛈️")],
        "trust": ["Insurance Claims Help", "Free Inspections", "Lifetime Warranty Available"],
        "photo": "photo-1600585154526-990dced4db0d",
    },
    "hvac": {
        "headline": "Keep Your Home Comfortable",
        "sub": "Heating, cooling, and air quality solutions for your home or business. 24/7 emergency service available.",
        "services": [("AC Repair", "❄️"), ("Furnace Service", "🔥"), ("Installation", "⚙️"), ("Maintenance Plans", "📋"), ("Duct Cleaning", "🌬️"), ("Emergency Service", "🚨")],
        "trust": ["24/7 Emergency Service", "All Brands Serviced", "Financing Available"],
        "photo": "photo-1585771724684-38269d6639fd",
    },
    "dry cleaner": {
        "headline": "Drop Off Today, Pick Up Tomorrow",
        "sub": "Professional dry cleaning and laundry service. We handle your clothes like they are our own.",
        "services": [("Dry Cleaning", "👔"), ("Shirt Laundering", "👕"), ("Alterations", "✂️"), ("Wedding Gown", "👰"), ("Leather & Suede", "🧥"), ("Same-Day Service", "⚡")],
        "trust": ["Next-Day Turnaround", "Free Pickup & Delivery", "Eco-Friendly Solvents"],
        "photo": "photo-1558769132-cb1aea458c5e",
    },
    "flooring": {
        "headline": "Beautiful Floors Start Here",
        "sub": "Hardwood, LVP, tile, and carpet installation by local flooring pros. Free in-home estimates.",
        "services": [("Hardwood Floors", "🌳"), ("LVP / Laminate", "🏠"), ("Tile & Stone", "🧱"), ("Carpet", "🛋️"), ("Floor Refinishing", "✨"), ("Free Estimates", "📋")],
        "trust": ["Licensed & Insured", "Free In-Home Estimates", "Satisfaction Guaranteed"],
        "photo": "photo-1600607687939-ce8a6c25118c",
    },
}

_DEFAULT_INDUSTRY = {
    "headline": "Local {cat} You Can Trust",
    "sub": "Reliable, professional services serving {city} and surrounding areas. Call us today for a free estimate.",
    "services": [("Free Estimates", "📋"), ("Quality Work", "⭐"), ("Affordable Pricing", "💰"), ("Local Business", "🏠"), ("Licensed & Insured", "✅"), ("Satisfaction Guaranteed", "😊")],
    "trust": ["Licensed & Insured", "Free Estimates", "Satisfaction Guaranteed"],
    "photo": "photo-1486406146926-c627a92ad1ab",
}

import random

def generate_preview_html(lead: dict) -> str:
    name     = lead.get("name", "Your Business")
    phone    = lead.get("phone", "")
    address  = lead.get("address", "")
    category = (lead.get("category") or "business").lower()
    logo_url = lead.get("logo_url", "")
    city     = (lead.get("location") or "").split(" IL")[0].strip()
    lead_id  = lead.get("id", 0)

    accent    = CATEGORY_ACCENTS.get(category, "#6366f1")
    cat_label = category.title()
    template  = (lead_id or random.randint(0, 999999)) % 3

    ind       = INDUSTRY_DATA.get(category, _DEFAULT_INDUSTRY)
    headline  = ind["headline"].replace("{city}", city or "Your Area").replace("{cat}", cat_label)
    sub       = ind["sub"].replace("{city}", city or "the local area").replace("{cat}", cat_label.lower())
    services  = ind["services"]
    trust     = ind["trust"]
    photo_id  = ind.get("photo", "photo-1486406146926-c627a92ad1ab")
    photo_url = f"https://images.unsplash.com/{photo_id}?auto=format&fit=crop&w=1400&q=80"

    logo_nav = (
        f'<img src="{logo_url}" onerror="this.style.display=\'none\'" style="height:36px;width:36px;border-radius:8px;object-fit:cover;">'
    ) if logo_url else (
        f'<div style="width:36px;height:36px;border-radius:8px;background:{accent};display:flex;align-items:center;justify-content:center;font-size:16px;font-weight:900;color:white;flex-shrink:0;">{name[0].upper()}</div>'
    )
    logo_hero = (
        f'<img src="{logo_url}" onerror="this.style.display=\'none\'" style="width:96px;height:96px;border-radius:20px;object-fit:cover;border:4px solid rgba(255,255,255,0.3);">'
    ) if logo_url else (
        f'<div style="width:96px;height:96px;border-radius:20px;background:rgba(255,255,255,0.2);border:4px solid rgba(255,255,255,0.3);display:flex;align-items:center;justify-content:center;font-size:42px;font-weight:900;color:white;">{name[0].upper()}</div>'
    )

    svc_cards_light = "".join(
        f'<div style="background:#fff;border:1px solid #e5e7eb;border-radius:14px;padding:20px 16px;text-align:center;"><div style="font-size:28px;margin-bottom:8px;">{icon}</div><div style="font-weight:700;font-size:14px;color:#111827;">{svc}</div></div>'
        for svc, icon in services
    )
    svc_cards_dark = "".join(
        f'<div style="background:#1f2937;border-radius:14px;padding:20px 16px;text-align:center;"><div style="font-size:28px;margin-bottom:8px;">{icon}</div><div style="font-weight:700;font-size:14px;color:#f9fafb;">{svc}</div></div>'
        for svc, icon in services
    )
    trust_chips = "".join(
        f'<div style="display:inline-flex;align-items:center;gap:8px;background:rgba(255,255,255,0.15);border:1px solid rgba(255,255,255,0.25);border-radius:100px;padding:8px 16px;font-size:13px;font-weight:600;color:#fff;"><span>&#10003;</span>{t}</div>'
        for t in trust
    )
    trust_chips_dark = "".join(
        f'<div style="display:inline-flex;align-items:center;gap:8px;background:#f9fafb;border-radius:100px;padding:10px 18px;font-size:13px;font-weight:700;color:#111827;"><span style="color:{accent};">&#10003;</span>{t}</div>'
        for t in trust
    )

    phone_svg = '<svg width="17" height="17" fill="currentColor" viewBox="0 0 24 24"><path d="M6.6 10.8c1.4 2.8 3.8 5.1 6.6 6.6l2.2-2.2c.3-.3.7-.4 1-.2 1.1.4 2.3.6 3.6.6.6 0 1 .4 1 1V20c0 .6-.4 1-1 1-9.4 0-17-7.6-17-17 0-.6.4-1 1-1h3.5c.6 0 1 .4 1 1 0 1.3.2 2.5.6 3.6.1.3 0 .7-.2 1L6.6 10.8z"/></svg>'
    sms_svg  = '<svg width="17" height="17" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z"/></svg>'
    pin_svg  = '<svg width="15" height="15" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M17.657 16.657L13.414 20.9a2 2 0 01-2.828 0l-4.243-4.243a8 8 0 1111.314 0z"/></svg>'

    addr_line = f'<div style="display:flex;align-items:center;gap:6px;font-size:13px;color:rgba(255,255,255,0.75);">{pin_svg}{address}</div>' if address else ''

    footer_html = f"""<footer style="background:#111827;color:#9ca3af;padding:40px 5%;text-align:center;">
  <div style="font-weight:800;font-size:18px;color:#fff;margin-bottom:4px;">{name}</div>
  <div style="font-size:13px;margin-bottom:4px;">{cat_label} in {city if city else 'Your Area'}</div>
  {f'<div style="font-size:13px;margin-bottom:4px;">{address}</div>' if address else ''}
  <div style="font-size:13px;margin-bottom:16px;"><a href="tel:{phone}" style="color:{accent};text-decoration:none;font-weight:700;">{phone}</a></div>
  <div style="border-top:1px solid #1f2937;padding-top:16px;font-size:12px;color:#4b5563;">
    &copy; {name}. This is a free preview website.
    <a href="sms:{phone}" style="color:{accent};text-decoration:none;margin-left:8px;font-weight:600;">Claim it today.</a>
  </div>
</footer>"""

    # Shared building blocks for the redesigned templates
    words = headline.split()
    headline_hl = (" ".join(words[:-1]) + f' <span class="hl">{words[-1]}</span>') if len(words) > 1 else headline

    marq_items = "".join(f"<span>{svc}</span>" for svc, _ in services)

    svc_rows_bold = "".join(
        f'<div class="svc"><span class="svc-num">{str(i+1).zfill(2)}</span><span class="svc-name">{svc}</span><span class="svc-ico">{icon}</span></div>'
        for i, (svc, icon) in enumerate(services)
    )
    svc_rows_dark = "".join(
        f'<div class="row"><span class="row-num">{str(i+1).zfill(2)}</span><span class="row-name">{svc}</span><span class="row-ico">{icon}</span></div>'
        for i, (svc, icon) in enumerate(services)
    )
    svc_cards_warm = "".join(
        f'<div class="card"><div class="card-ico">{icon}</div><div class="card-name">{svc}</div></div>'
        for svc, icon in services
    )

    # ── Template 0: "Poster" — Anton display caps, full-bleed photo, marquee ──
    if template == 0:
        return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{name} | {cat_label} in {city or 'Your Area'}</title>
<link href="https://fonts.googleapis.com/css2?family=Anton&family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
html{{scroll-behavior:smooth;}}
body{{font-family:'Manrope',sans-serif;background:#101010;color:#181818;min-height:100vh;padding-bottom:74px;}}
.disp{{font-family:'Anton',sans-serif;font-weight:400;text-transform:uppercase;}}
.preview-bar{{background:#181818;color:#fff;text-align:center;padding:9px 20px;font-size:12px;font-weight:600;letter-spacing:.02em;}}
.preview-bar a{{color:{accent};text-decoration:none;margin-left:8px;font-weight:800;}}
nav{{position:absolute;top:38px;left:0;right:0;z-index:50;display:flex;align-items:center;justify-content:space-between;padding:18px 5%;}}
.nav-brand{{display:flex;align-items:center;gap:10px;}}
.nav-name{{font-family:'Anton';font-size:17px;letter-spacing:.06em;text-transform:uppercase;color:#fff;}}
.nav-phone{{display:inline-flex;align-items:center;gap:7px;background:{accent};color:#fff;font-weight:800;font-size:13px;padding:10px 20px;border-radius:4px;text-decoration:none;}}
.hero{{position:relative;min-height:92vh;display:flex;align-items:flex-end;overflow:hidden;}}
.hero-bg{{position:absolute;inset:0;background-image:url('{photo_url}');background-size:cover;background-position:center;}}
.hero-shade{{position:absolute;inset:0;background:linear-gradient(180deg,rgba(16,16,16,.45) 0%,rgba(16,16,16,.1) 40%,rgba(16,16,16,.92) 100%);}}
.hero-inner{{position:relative;z-index:2;width:100%;padding:0 5% 64px;}}
.hero-kicker{{display:inline-block;background:{accent};color:#fff;font-size:11px;font-weight:800;letter-spacing:.22em;text-transform:uppercase;padding:7px 16px;margin-bottom:22px;}}
.hero h1{{font-family:'Anton';font-weight:400;text-transform:uppercase;font-size:clamp(2.8rem,8.5vw,6rem);line-height:.96;letter-spacing:.01em;color:#fff;max-width:13ch;margin-bottom:18px;}}
.hero h1 .hl{{color:{accent};}}
.hero p{{color:rgba(255,255,255,.78);font-size:1.05rem;line-height:1.65;max-width:520px;margin-bottom:30px;font-weight:500;}}
.hero-btns{{display:flex;gap:12px;flex-wrap:wrap;}}
.btn-a{{display:inline-flex;align-items:center;gap:9px;background:{accent};color:#fff;font-weight:800;font-size:15px;padding:17px 34px;border-radius:4px;text-decoration:none;}}
.btn-b{{display:inline-flex;align-items:center;gap:9px;background:transparent;color:#fff;font-weight:700;font-size:15px;padding:17px 34px;border-radius:4px;text-decoration:none;border:1.5px solid rgba(255,255,255,.55);}}
.marq{{background:{accent};overflow:hidden;padding:13px 0;}}
.marq-track{{display:flex;gap:56px;width:max-content;animation:slide 26s linear infinite;}}
.marq span{{font-family:'Anton';font-size:15px;letter-spacing:.14em;text-transform:uppercase;color:#fff;white-space:nowrap;}}
@keyframes slide{{to{{transform:translateX(-50%);}}}}
.main{{background:#f4f1ec;}}
.services{{max-width:1020px;margin:0 auto;padding:84px 5% 70px;}}
.eyebrow{{color:{accent};font-size:11px;font-weight:800;letter-spacing:.24em;text-transform:uppercase;margin-bottom:12px;}}
.services h2{{font-family:'Anton';text-transform:uppercase;font-weight:400;font-size:clamp(1.9rem,4vw,2.8rem);color:#181818;margin-bottom:36px;}}
.svc{{display:flex;align-items:center;gap:22px;padding:22px 6px;border-top:1.5px solid #d9d3c9;transition:.25s;}}
.svc:last-of-type{{border-bottom:1.5px solid #d9d3c9;}}
.svc:hover{{padding-left:16px;background:#fff;}}
.svc-num{{font-family:'Anton';font-size:14px;color:{accent};letter-spacing:.08em;}}
.svc-name{{font-weight:800;font-size:clamp(1.05rem,2.4vw,1.45rem);color:#181818;flex:1;}}
.svc-ico{{font-size:26px;}}
.band{{max-width:1020px;margin:0 auto;padding:0 5% 84px;display:grid;grid-template-columns:1.2fr .8fr;gap:48px;}}
@media(max-width:760px){{.band{{grid-template-columns:1fr;}}}}
.band h3{{font-family:'Anton';text-transform:uppercase;font-weight:400;font-size:1.7rem;color:#181818;margin-bottom:14px;}}
.band p{{color:#55504a;line-height:1.75;font-weight:500;margin-bottom:22px;}}
.trust{{display:flex;flex-direction:column;gap:12px;}}
.trust div{{display:flex;align-items:center;gap:12px;font-weight:700;font-size:14.5px;color:#181818;}}
.trust b{{width:26px;height:26px;background:{accent};color:#fff;border-radius:3px;display:flex;align-items:center;justify-content:center;font-size:13px;flex-shrink:0;}}
.info{{display:flex;flex-direction:column;gap:10px;}}
.info-card{{background:#fff;border:1.5px solid #d9d3c9;padding:18px 22px;}}
.info-label{{font-size:10px;font-weight:800;letter-spacing:.2em;text-transform:uppercase;color:#a39b8d;margin-bottom:5px;}}
.info-val{{font-weight:800;font-size:15px;color:#181818;}}
.info-val a{{color:{accent};text-decoration:none;}}
.cta{{background:#181818;text-align:center;padding:90px 5%;}}
.cta .eyebrow{{margin-bottom:16px;}}
.cta h2{{font-family:'Anton';text-transform:uppercase;font-weight:400;font-size:clamp(2.2rem,6vw,4rem);color:#fff;line-height:1;margin-bottom:14px;}}
.cta h2 span{{color:{accent};}}
.cta p{{color:rgba(255,255,255,.6);font-weight:500;margin-bottom:34px;}}
.sticky-bar{{position:fixed;bottom:0;left:0;right:0;background:#181818;padding:11px 16px;display:flex;gap:10px;z-index:200;}}
.sticky-call{{flex:1;display:flex;align-items:center;justify-content:center;gap:8px;background:{accent};color:#fff;font-weight:800;font-size:14px;padding:13px;border-radius:4px;text-decoration:none;}}
.sticky-claim{{flex:1;display:flex;align-items:center;justify-content:center;background:#2c2c2c;color:#fff;font-weight:700;font-size:13.5px;padding:13px;border-radius:4px;text-decoration:none;}}
@media(max-width:640px){{nav{{top:34px;}} .nav-phone span{{display:none;}}}}
</style></head><body>
<div class="preview-bar">This is a FREE preview website built for {name}.<a href="sms:{phone}">Text us to claim it</a></div>
<nav>
  <div class="nav-brand">{logo_nav}<span class="nav-name">{name}</span></div>
  <a href="tel:{phone}" class="nav-phone">{phone_svg}<span>{phone}</span></a>
</nav>
<section class="hero">
  <div class="hero-bg"></div>
  <div class="hero-shade"></div>
  <div class="hero-inner">
    <div class="hero-kicker">{cat_label} &middot; {city if city else 'Your Area'}</div>
    <h1>{headline_hl}</h1>
    <p>{sub}</p>
    <div class="hero-btns">
      <a href="tel:{phone}" class="btn-a">{phone_svg}Call {phone}</a>
      <a href="sms:{phone}" class="btn-b">{sms_svg}Send a Text</a>
    </div>
  </div>
</section>
<div class="marq"><div class="marq-track">{marq_items}{marq_items}</div></div>
<div class="main">
  <section class="services" id="services">
    <div class="eyebrow">What we do</div>
    <h2>Our Services</h2>
    {svc_rows_bold}
  </section>
  <section class="band" id="about">
    <div>
      <h3>About {name}</h3>
      <p>Locally owned and operated, {name} proudly serves {city if city else 'our community'} with professional {cat_label.lower()} work. We treat every customer like a neighbor, because to us, you are one.</p>
      <div class="trust">{''.join(f'<div><b>&#10003;</b>{t}</div>' for t in trust)}</div>
    </div>
    <div class="info" id="contact">
      <div class="info-card"><div class="info-label">Phone</div><div class="info-val"><a href="tel:{phone}">{phone}</a></div></div>
      {f'<div class="info-card"><div class="info-label">Address</div><div class="info-val">{address}</div></div>' if address else ''}
      <div class="info-card"><div class="info-label">Hours</div><div class="info-val">Mon&ndash;Sat 8am&ndash;6pm</div></div>
      <div class="info-card"><div class="info-label">Service Area</div><div class="info-val">{city if city else 'Local Area'} &amp; Surrounding</div></div>
    </div>
  </section>
</div>
<section class="cta">
  <div class="eyebrow">Ready when you are</div>
  <h2>Call <span>{phone}</span></h2>
  <p>Fast response. Fair pricing. Local team.</p>
  <div style="display:flex;gap:12px;justify-content:center;flex-wrap:wrap;">
    <a href="tel:{phone}" class="btn-a">{phone_svg}Call Now</a>
    <a href="sms:{phone}" class="btn-b">{sms_svg}Text Us</a>
  </div>
</section>
{footer_html}
<div class="sticky-bar">
  <a href="tel:{phone}" class="sticky-call">{phone_svg}Call Now</a>
  <a href="sms:{phone}" class="sticky-claim">Claim This Site Free</a>
</div>
</body></html>"""

    # ── Template 1: "Dark Luxe" — Fraunces serif, warm black, framed photo ────
    elif template == 1:
        return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{name} | {cat_label} in {city or 'Your Area'}</title>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,300..600;1,9..144,300..600&family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
html{{scroll-behavior:smooth;}}
body{{font-family:'Manrope',sans-serif;background:#12100d;color:#ece7df;min-height:100vh;padding-bottom:74px;}}
.serif{{font-family:'Fraunces',serif;}}
.preview-bar{{background:{accent};color:#fff;text-align:center;padding:9px 20px;font-size:12px;font-weight:700;}}
.preview-bar a{{color:#fff;text-decoration:underline;margin-left:8px;font-weight:800;}}
nav{{display:flex;align-items:center;justify-content:space-between;padding:20px 5%;border-bottom:1px solid rgba(236,231,223,.1);position:sticky;top:0;background:rgba(18,16,13,.92);backdrop-filter:blur(10px);z-index:100;}}
.nav-brand{{display:flex;align-items:center;gap:11px;}}
.nav-name{{font-family:'Fraunces',serif;font-size:19px;font-weight:500;color:#ece7df;}}
.nav-phone{{display:inline-flex;align-items:center;gap:7px;border:1px solid {accent};color:{accent};font-weight:700;font-size:13px;padding:10px 20px;border-radius:100px;text-decoration:none;transition:.3s;}}
.nav-phone:hover{{background:{accent};color:#fff;}}
.hero{{max-width:1100px;margin:0 auto;padding:76px 5% 84px;display:grid;grid-template-columns:1.05fr .95fr;gap:60px;align-items:center;}}
@media(max-width:820px){{.hero{{grid-template-columns:1fr;padding-top:52px;gap:44px;}}}}
.hero-kicker{{display:flex;align-items:center;gap:12px;color:{accent};font-size:11px;font-weight:800;letter-spacing:.26em;text-transform:uppercase;margin-bottom:24px;}}
.hero-kicker::before{{content:'';width:36px;height:1px;background:{accent};}}
.hero h1{{font-family:'Fraunces',serif;font-weight:400;font-size:clamp(2.5rem,5.4vw,4rem);line-height:1.07;letter-spacing:-.01em;color:#fff;margin-bottom:20px;}}
.hero h1 .hl{{font-style:italic;color:{accent};}}
.hero p{{color:rgba(236,231,223,.62);font-size:1.06rem;line-height:1.75;margin-bottom:32px;max-width:480px;}}
.hero-btns{{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:36px;}}
.btn-a{{display:inline-flex;align-items:center;gap:9px;background:{accent};color:#fff;font-weight:800;font-size:15px;padding:16px 32px;border-radius:100px;text-decoration:none;box-shadow:0 8px 30px {accent}44;}}
.btn-b{{display:inline-flex;align-items:center;gap:9px;color:#ece7df;font-weight:700;font-size:15px;padding:16px 32px;border-radius:100px;text-decoration:none;border:1px solid rgba(236,231,223,.3);}}
.hero-trust{{display:flex;flex-wrap:wrap;gap:10px;}}
.hero-trust span{{font-size:12.5px;font-weight:600;color:rgba(236,231,223,.7);border:1px solid rgba(236,231,223,.16);border-radius:100px;padding:8px 16px;}}
.photo-wrap{{position:relative;}}
.photo-wrap::after{{content:'';position:absolute;inset:18px -18px -18px 18px;border:1px solid {accent}66;border-radius:22px;z-index:0;}}
.photo-wrap img{{position:relative;z-index:1;width:100%;aspect-ratio:4/4.6;object-fit:cover;border-radius:22px;display:block;filter:saturate(.92);}}
.photo-badge{{position:absolute;z-index:2;bottom:22px;left:22px;background:rgba(18,16,13,.82);backdrop-filter:blur(8px);border:1px solid rgba(236,231,223,.15);border-radius:14px;padding:13px 18px;font-size:13px;font-weight:700;color:#ece7df;}}
.photo-badge em{{display:block;font-style:normal;color:{accent};font-size:10.5px;font-weight:800;letter-spacing:.18em;text-transform:uppercase;margin-bottom:3px;}}
.divider{{max-width:1100px;margin:0 auto;padding:0 5%;}}
.divider hr{{border:none;border-top:1px solid rgba(236,231,223,.1);}}
.services{{max-width:1100px;margin:0 auto;padding:76px 5%;}}
.eyebrow{{color:{accent};font-size:11px;font-weight:800;letter-spacing:.26em;text-transform:uppercase;margin-bottom:14px;}}
.services h2{{font-family:'Fraunces',serif;font-weight:400;font-size:clamp(1.9rem,4vw,2.7rem);color:#fff;margin-bottom:38px;}}
.row{{display:flex;align-items:center;gap:24px;padding:21px 4px;border-top:1px solid rgba(236,231,223,.12);transition:.25s;}}
.row:last-of-type{{border-bottom:1px solid rgba(236,231,223,.12);}}
.row:hover{{padding-left:14px;}}
.row-num{{font-family:'Fraunces',serif;font-style:italic;color:{accent};font-size:15px;}}
.row-name{{flex:1;font-weight:700;font-size:clamp(1rem,2.2vw,1.3rem);color:#ece7df;}}
.row-ico{{font-size:24px;opacity:.9;}}
.quote{{text-align:center;max-width:760px;margin:0 auto;padding:24px 5% 80px;}}
.quote p{{font-family:'Fraunces',serif;font-style:italic;font-weight:300;font-size:clamp(1.4rem,3.2vw,2rem);line-height:1.4;color:rgba(236,231,223,.85);}}
.quote p b{{color:{accent};font-weight:400;}}
.contact{{max-width:1100px;margin:0 auto;padding:0 5% 80px;display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;}}
.c-card{{background:rgba(236,231,223,.04);border:1px solid rgba(236,231,223,.12);border-radius:18px;padding:24px;}}
.c-label{{font-size:10px;font-weight:800;letter-spacing:.22em;text-transform:uppercase;color:rgba(236,231,223,.45);margin-bottom:8px;}}
.c-val{{font-weight:700;font-size:15.5px;color:#ece7df;line-height:1.5;}}
.c-val a{{color:{accent};text-decoration:none;}}
.cta{{background:{accent};text-align:center;padding:84px 5%;}}
.cta h2{{font-family:'Fraunces',serif;font-weight:400;font-size:clamp(2rem,5vw,3.2rem);color:#fff;margin-bottom:12px;}}
.cta h2 em{{font-style:italic;}}
.cta p{{color:rgba(255,255,255,.85);margin-bottom:32px;font-weight:600;}}
.btn-w{{display:inline-flex;align-items:center;gap:9px;background:#12100d;color:#fff;font-weight:800;font-size:15px;padding:16px 34px;border-radius:100px;text-decoration:none;}}
.btn-w2{{display:inline-flex;align-items:center;gap:9px;background:rgba(255,255,255,.16);color:#fff;font-weight:700;font-size:15px;padding:16px 34px;border-radius:100px;text-decoration:none;border:1px solid rgba(255,255,255,.45);}}
.sticky-bar{{position:fixed;bottom:0;left:0;right:0;background:rgba(18,16,13,.96);backdrop-filter:blur(10px);border-top:1px solid rgba(236,231,223,.12);padding:11px 16px;display:flex;gap:10px;z-index:200;}}
.sticky-call{{flex:1;display:flex;align-items:center;justify-content:center;gap:8px;background:{accent};color:#fff;font-weight:800;font-size:14px;padding:13px;border-radius:100px;text-decoration:none;}}
.sticky-claim{{flex:1;display:flex;align-items:center;justify-content:center;background:transparent;border:1px solid rgba(236,231,223,.3);color:#ece7df;font-weight:700;font-size:13.5px;padding:13px;border-radius:100px;text-decoration:none;}}
</style></head><body>
<div class="preview-bar">Free website preview for {name}.<a href="sms:{phone}">Text to claim it</a></div>
<nav>
  <div class="nav-brand">{logo_nav}<span class="nav-name">{name}</span></div>
  <a href="tel:{phone}" class="nav-phone">{phone_svg}{phone}</a>
</nav>
<section class="hero">
  <div>
    <div class="hero-kicker">{cat_label} &middot; {city if city else 'Your Area'}</div>
    <h1>{headline_hl}</h1>
    <p>{sub}</p>
    <div class="hero-btns">
      <a href="tel:{phone}" class="btn-a">{phone_svg}Call {phone}</a>
      <a href="sms:{phone}" class="btn-b">{sms_svg}Text Us</a>
    </div>
    <div class="hero-trust">{''.join(f'<span>&#10003;&nbsp; {t}</span>' for t in trust)}</div>
  </div>
  <div class="photo-wrap">
    <img src="{photo_url}" alt="{name} {cat_label}">
    <div class="photo-badge"><em>Locally owned</em>Serving {city if city else 'the local area'} &amp; surrounding</div>
  </div>
</section>
<div class="divider"><hr></div>
<section class="services" id="services">
  <div class="eyebrow">What we offer</div>
  <h2>Our Services</h2>
  {svc_rows_dark}
</section>
<section class="quote">
  <p>&ldquo;Every job, big or small, gets our full attention. That&rsquo;s the <b>{name}</b> promise.&rdquo;</p>
</section>
<section class="contact" id="contact">
  <div class="c-card"><div class="c-label">Phone</div><div class="c-val"><a href="tel:{phone}">{phone}</a></div></div>
  {f'<div class="c-card"><div class="c-label">Address</div><div class="c-val">{address}</div></div>' if address else ''}
  <div class="c-card"><div class="c-label">Hours</div><div class="c-val">Mon&ndash;Sat 8am&ndash;6pm</div></div>
  <div class="c-card"><div class="c-label">Service Area</div><div class="c-val">{city if city else 'Local Area'} &amp; Surrounding</div></div>
</section>
<section class="cta">
  <h2>Ready to <em>get started?</em></h2>
  <p>Call or text us today. We respond fast.</p>
  <div style="display:flex;gap:12px;justify-content:center;flex-wrap:wrap;">
    <a href="tel:{phone}" class="btn-w">{phone_svg}Call {phone}</a>
    <a href="sms:{phone}" class="btn-w2">{sms_svg}Send a Text</a>
  </div>
</section>
{footer_html}
<div class="sticky-bar">
  <a href="tel:{phone}" class="sticky-call">{phone_svg}Call Now</a>
  <a href="sms:{phone}" class="sticky-claim">Claim This Site Free</a>
</div>
</body></html>"""

    # ── Template 2: "Warm Editorial" — DM Serif arch photo, cream palette ─────
    else:
        return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{name} | {cat_label} in {city or 'Your Area'}</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=Karla:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
html{{scroll-behavior:smooth;}}
body{{font-family:'Karla',sans-serif;background:#faf6ef;color:#241d14;min-height:100vh;padding-bottom:74px;}}
.preview-bar{{background:#241d14;color:#faf6ef;text-align:center;padding:9px 20px;font-size:12px;font-weight:700;}}
.preview-bar a{{color:{accent};text-decoration:none;margin-left:8px;font-weight:800;}}
nav{{display:flex;align-items:center;justify-content:space-between;padding:18px 5%;position:sticky;top:0;background:rgba(250,246,239,.94);backdrop-filter:blur(10px);z-index:100;border-bottom:1px solid rgba(36,29,20,.1);}}
.nav-brand{{display:flex;align-items:center;gap:11px;}}
.nav-name{{font-family:'DM Serif Display',serif;font-size:20px;color:#241d14;}}
.nav-phone{{display:inline-flex;align-items:center;gap:7px;background:#241d14;color:#faf6ef;font-weight:800;font-size:13px;padding:10px 22px;border-radius:100px;text-decoration:none;}}
.hero{{max-width:1080px;margin:0 auto;padding:64px 5% 80px;display:grid;grid-template-columns:1.05fr .95fr;gap:56px;align-items:center;}}
@media(max-width:820px){{.hero{{grid-template-columns:1fr;padding-top:46px;gap:42px;}}}}
.hero-kicker{{display:inline-block;background:{accent}1a;color:{accent};font-size:11px;font-weight:800;letter-spacing:.2em;text-transform:uppercase;padding:8px 18px;border-radius:100px;margin-bottom:22px;}}
.hero h1{{font-family:'DM Serif Display',serif;font-weight:400;font-size:clamp(2.6rem,5.6vw,4.1rem);line-height:1.05;letter-spacing:-.01em;color:#241d14;margin-bottom:18px;}}
.hero h1 .hl{{font-style:italic;color:{accent};}}
.hero p{{color:#6d6354;font-size:1.06rem;line-height:1.75;margin-bottom:30px;max-width:470px;}}
.hero-btns{{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:30px;}}
.btn-a{{display:inline-flex;align-items:center;gap:9px;background:{accent};color:#fff;font-weight:800;font-size:15px;padding:16px 32px;border-radius:100px;text-decoration:none;box-shadow:0 10px 26px {accent}3d;}}
.btn-b{{display:inline-flex;align-items:center;gap:9px;background:transparent;color:#241d14;font-weight:700;font-size:15px;padding:16px 32px;border-radius:100px;text-decoration:none;border:1.5px solid #241d14;}}
.hero-meta{{display:flex;flex-wrap:wrap;gap:18px;font-size:13px;font-weight:600;color:#8a7e6c;}}
.hero-meta b{{color:#241d14;}}
.arch{{position:relative;}}
.arch img{{width:100%;aspect-ratio:4/4.8;object-fit:cover;border-radius:300px 300px 20px 20px;display:block;border:1.5px solid rgba(36,29,20,.18);}}
.arch-badge{{position:absolute;bottom:-18px;left:50%;transform:translateX(-50%);background:#fff;border:1.5px solid rgba(36,29,20,.12);border-radius:100px;padding:12px 26px;font-size:13px;font-weight:800;color:#241d14;white-space:nowrap;box-shadow:0 12px 30px rgba(36,29,20,.12);}}
.arch-badge span{{color:{accent};}}
.services{{background:#fff;padding:84px 5%;}}
.services-inner{{max-width:1020px;margin:0 auto;}}
.eyebrow{{color:{accent};font-size:11px;font-weight:800;letter-spacing:.24em;text-transform:uppercase;margin-bottom:12px;text-align:center;}}
.services h2{{font-family:'DM Serif Display',serif;font-weight:400;font-size:clamp(2rem,4.4vw,2.9rem);color:#241d14;margin-bottom:40px;text-align:center;}}
.svc-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(165px,1fr));gap:16px;}}
.card{{background:#faf6ef;border:1.5px solid rgba(36,29,20,.1);border-radius:20px;padding:26px 18px;text-align:center;transition:.25s;}}
.card:hover{{transform:translateY(-4px);box-shadow:0 14px 30px rgba(36,29,20,.1);}}
.card-ico{{width:52px;height:52px;border-radius:50%;background:{accent}1a;display:flex;align-items:center;justify-content:center;font-size:24px;margin:0 auto 14px;}}
.card-name{{font-weight:800;font-size:14.5px;color:#241d14;}}
.about{{max-width:1020px;margin:0 auto;padding:84px 5%;display:grid;grid-template-columns:1.1fr .9fr;gap:52px;align-items:start;}}
@media(max-width:760px){{.about{{grid-template-columns:1fr;}}}}
.about h3{{font-family:'DM Serif Display',serif;font-weight:400;font-size:2rem;color:#241d14;margin-bottom:14px;}}
.about h3 em{{color:{accent};}}
.about p{{color:#6d6354;line-height:1.8;margin-bottom:24px;}}
.trust{{display:flex;flex-direction:column;gap:12px;}}
.trust div{{display:flex;align-items:center;gap:12px;font-weight:700;font-size:14.5px;color:#241d14;}}
.trust b{{width:28px;height:28px;border-radius:50%;background:{accent};color:#fff;display:flex;align-items:center;justify-content:center;font-size:13px;flex-shrink:0;}}
.info{{display:flex;flex-direction:column;gap:12px;}}
.info-card{{background:#fff;border:1.5px solid rgba(36,29,20,.1);border-radius:18px;padding:20px 24px;}}
.info-label{{font-size:10px;font-weight:800;letter-spacing:.22em;text-transform:uppercase;color:#a89c88;margin-bottom:5px;}}
.info-val{{font-weight:800;font-size:15.5px;color:#241d14;}}
.info-val a{{color:{accent};text-decoration:none;}}
.cta-wrap{{padding:0 5% 90px;}}
.cta{{max-width:1020px;margin:0 auto;background:{accent};border-radius:32px;text-align:center;padding:74px 8%;}}
.cta h2{{font-family:'DM Serif Display',serif;font-weight:400;font-size:clamp(2rem,4.8vw,3rem);color:#fff;margin-bottom:12px;}}
.cta h2 em{{font-style:italic;}}
.cta p{{color:rgba(255,255,255,.88);font-weight:600;margin-bottom:32px;}}
.btn-w{{display:inline-flex;align-items:center;gap:9px;background:#fff;color:{accent};font-weight:800;font-size:15px;padding:16px 32px;border-radius:100px;text-decoration:none;}}
.btn-w2{{display:inline-flex;align-items:center;gap:9px;background:rgba(255,255,255,.16);color:#fff;font-weight:700;font-size:15px;padding:16px 32px;border-radius:100px;text-decoration:none;border:1.5px solid rgba(255,255,255,.5);}}
.sticky-bar{{position:fixed;bottom:0;left:0;right:0;background:rgba(250,246,239,.97);backdrop-filter:blur(10px);border-top:1.5px solid rgba(36,29,20,.12);padding:11px 16px;display:flex;gap:10px;z-index:200;}}
.sticky-call{{flex:1;display:flex;align-items:center;justify-content:center;gap:8px;background:{accent};color:#fff;font-weight:800;font-size:14px;padding:13px;border-radius:100px;text-decoration:none;}}
.sticky-claim{{flex:1;display:flex;align-items:center;justify-content:center;background:#241d14;color:#faf6ef;font-weight:700;font-size:13.5px;padding:13px;border-radius:100px;text-decoration:none;}}
</style></head><body>
<div class="preview-bar">Free website preview for {name}.<a href="sms:{phone}">Text to claim it today</a></div>
<nav>
  <div class="nav-brand">{logo_nav}<span class="nav-name">{name}</span></div>
  <a href="tel:{phone}" class="nav-phone">{phone_svg}{phone}</a>
</nav>
<section class="hero">
  <div>
    <div class="hero-kicker">{cat_label} in {city if city else 'Your Area'}</div>
    <h1>{headline_hl}</h1>
    <p>{sub}</p>
    <div class="hero-btns">
      <a href="tel:{phone}" class="btn-a">{phone_svg}Call {phone}</a>
      <a href="sms:{phone}" class="btn-b">{sms_svg}Text Us</a>
    </div>
    <div class="hero-meta">
      <span><b>Hours:</b> Mon&ndash;Sat 8am&ndash;6pm</span>
      {f'<span><b>Find us:</b> {address}</span>' if address else ''}
    </div>
  </div>
  <div class="arch">
    <img src="{photo_url}" alt="{name} {cat_label}">
    <div class="arch-badge"><span>&#10003;</span> Locally Owned &amp; Operated</div>
  </div>
</section>
<section class="services" id="services">
  <div class="services-inner">
    <div class="eyebrow">What we offer</div>
    <h2>Our Services</h2>
    <div class="svc-grid">{svc_cards_warm}</div>
  </div>
</section>
<section class="about" id="about">
  <div>
    <h3>Why choose <em>{name}?</em></h3>
    <p>We are a locally owned {cat_label.lower()} business dedicated to serving {city if city else 'our community'} with integrity. Every job, big or small, gets our full attention.</p>
    <div class="trust">{''.join(f'<div><b>&#10003;</b>{t}</div>' for t in trust)}</div>
  </div>
  <div class="info" id="contact">
    <div class="info-card"><div class="info-label">Phone</div><div class="info-val"><a href="tel:{phone}">{phone}</a></div></div>
    {f'<div class="info-card"><div class="info-label">Address</div><div class="info-val">{address}</div></div>' if address else ''}
    <div class="info-card"><div class="info-label">Hours</div><div class="info-val">Mon&ndash;Sat 8am&ndash;6pm</div></div>
    <div class="info-card"><div class="info-label">Service Area</div><div class="info-val">{city if city else 'Local Area'} &amp; Nearby</div></div>
  </div>
</section>
<div class="cta-wrap">
  <div class="cta">
    <h2>Ready to <em>get started?</em></h2>
    <p>Call or text us today. Fast response, fair pricing, local team.</p>
    <div style="display:flex;gap:12px;justify-content:center;flex-wrap:wrap;">
      <a href="tel:{phone}" class="btn-w">{phone_svg}Call {phone}</a>
      <a href="sms:{phone}" class="btn-w2">{sms_svg}Send a Text</a>
    </div>
  </div>
</div>
{footer_html}
<div class="sticky-bar">
  <a href="tel:{phone}" class="sticky-call">{phone_svg}Call Now</a>
  <a href="sms:{phone}" class="sticky-claim">Claim This Site Free</a>
</div>
</body></html>"""


# ── Preview helpers ───────────────────────────────────────────────────────────

def write_preview(lead: dict) -> str:
    """Generate the preview HTML file for a lead, save its path, return the path."""
    slug = re.sub(r"[^a-z0-9]+", "-", lead["name"].lower()).strip("-")
    filename = f"{slug}-{lead['id']}.html"
    with open(os.path.join("previews", filename), "w") as f:
        f.write(generate_preview_html(lead))
    path = f"/previews/{filename}"
    conn = get_db()
    conn.execute("UPDATE leads SET preview_url = ? WHERE id = ?", (path, lead["id"]))
    conn.commit()
    conn.close()
    return path


def ensure_preview(lead: dict) -> str:
    """Return the lead's preview path, generating it first if it doesn't exist."""
    return lead.get("preview_url") or write_preview(lead)


def absolute_url(path: str) -> str:
    """Turn a /previews/... path into a full link using the configured base URL."""
    base = (load_config().get("base_url") or "").rstrip("/")
    if base and path.startswith("/"):
        return f"{base}{path}"
    return path


def validate_twilio_signature(request_url: str, params: dict, signature: str) -> bool:
    """Verify X-Twilio-Signature. Only enforced when base_url is configured."""
    cfg = load_config()
    token = cfg.get("twilio_auth_token", "")
    base = (cfg.get("base_url") or "").rstrip("/")
    if not token or not base:
        return True  # dev mode: nothing to validate against
    import hmac, hashlib, base64 as b64
    url = f"{base}/api/webhooks/sms/reply"
    payload = url + "".join(f"{k}{v}" for k, v in sorted(params.items()))
    expected = b64.b64encode(hmac.new(token.encode(), payload.encode(), hashlib.sha1).digest()).decode()
    return hmac.compare_digest(expected, signature or "")


# ── API Routes ────────────────────────────────────────────────────────────────

@app.post("/api/scrape/start")
async def start_scrape(request: Request, background_tasks: BackgroundTasks):
    global scraper_running
    if scraper_running:
        return JSONResponse({"ok": False, "error": "Scraper already running"})
    body = await request.json()
    cats = body.get("categories", [])
    locs = body.get("locations", [])
    if not cats or not locs:
        return JSONResponse({"ok": False, "error": "No categories or locations selected"})
    background_tasks.add_task(run_scraper, cats, locs)
    return JSONResponse({"ok": True})


@app.get("/api/scrape/status")
async def scrape_status():
    return {"running": scraper_running, "log": scraper_log[-50:]}


@app.get("/api/leads")
async def get_leads(status: Optional[str] = None, search: Optional[str] = None):
    conn = get_db()
    query = "SELECT * FROM leads"
    params, conditions = [], []
    if status and status != "all":
        conditions.append("status = ?")
        params.append(status)
    if search:
        conditions.append("(name LIKE ? OR phone LIKE ? OR address LIKE ? OR category LIKE ?)")
        params += [f"%{search}%"] * 4
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY created_at DESC"
    rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()
    return rows


@app.patch("/api/leads/{lead_id}")
async def update_lead(lead_id: int, request: Request):
    body = await request.json()
    allowed = {"status", "notes", "preview_url"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        return JSONResponse({"ok": False, "error": "Nothing to update"})
    conn = get_db()
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    conn.execute(f"UPDATE leads SET {set_clause} WHERE id = ?", list(updates.values()) + [lead_id])
    conn.commit()
    conn.close()
    return {"ok": True}


@app.post("/api/leads/bulk-status")
async def bulk_status(request: Request):
    body = await request.json()
    ids    = body.get("ids", [])
    status = body.get("status")
    if not ids or not status:
        return JSONResponse({"ok": False, "error": "Missing ids or status"})
    conn = get_db()
    conn.execute(f"UPDATE leads SET status = ? WHERE id IN ({','.join('?'*len(ids))})", [status] + ids)
    conn.commit()
    conn.close()
    return {"ok": True, "updated": len(ids)}


@app.get("/api/leads/export")
async def export_leads(status: Optional[str] = None):
    conn = get_db()
    query = "SELECT name, phone, address, category, location, status, notes, preview_url, maps_url, created_at FROM leads"
    params = []
    if status and status != "all":
        query += " WHERE status = ?"
        params.append(status)
    query += " ORDER BY created_at DESC"
    rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()

    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=leads_{datetime.now().strftime('%Y%m%d')}.csv"}
    )


@app.post("/api/preview/generate/{lead_id}")
async def generate_preview(lead_id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    if not row:
        return JSONResponse({"ok": False, "error": "Lead not found"})

    preview_url = write_preview(dict(row))
    return {"ok": True, "preview_url": preview_url}


@app.post("/api/preview/generate-bulk")
async def generate_preview_bulk(request: Request):
    body = await request.json()
    ids = body.get("ids", [])
    if not ids:
        return JSONResponse({"ok": False, "error": "No leads selected"})
    results = []
    conn = get_db()
    rows = conn.execute(f"SELECT * FROM leads WHERE id IN ({','.join('?'*len(ids))})", ids).fetchall()
    conn.close()
    for row in rows:
        lead = dict(row)
        results.append({"id": lead["id"], "preview_url": write_preview(lead)})
    return {"ok": True, "results": results}


@app.post("/api/sms/send")
async def send_sms(request: Request):
    body    = await request.json()
    ids     = body.get("ids", [])
    message = body.get("message", "")
    if not ids or not message:
        return JSONResponse({"ok": False, "error": "Missing ids or message"})

    cfg = load_config()
    account_sid = cfg.get("twilio_account_sid")
    auth_token  = cfg.get("twilio_auth_token")
    from_number = cfg.get("twilio_from_number")

    if not all([account_sid, auth_token, from_number]):
        return JSONResponse({"ok": False, "error": "Twilio not configured. Go to Settings."})

    import base64, urllib.parse, urllib.request

    conn   = get_db()
    leads  = conn.execute(f"SELECT * FROM leads WHERE id IN ({','.join('?'*len(ids))})", ids).fetchall()
    conn.close()

    sent, failed = 0, 0
    for lead in leads:
        lead = dict(lead)
        if not lead.get("phone") or lead.get("status") == "opted_out":
            continue
        personalized = message.replace("{name}", lead["name"]).replace("{phone}", lead["phone"] or "")
        personalized = personalized.replace("{preview_url}", absolute_url(ensure_preview(lead)))
        try:
            await asyncio.sleep(1.1)  # Twilio caps ~1 msg/sec per number
            data = urllib.parse.urlencode({"To": lead["phone"], "From": from_number, "Body": personalized}).encode()
            req  = urllib.request.Request(
                f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
                data=data,
                headers={"Authorization": "Basic " + base64.b64encode(f"{account_sid}:{auth_token}".encode()).decode()}
            )
            urllib.request.urlopen(req, timeout=10)
            conn = get_db()
            conn.execute("UPDATE leads SET sms_sent = sms_sent + 1, status = CASE WHEN status = 'new' THEN 'contacted' ELSE status END WHERE id = ?", (lead["id"],))
            conn.execute("INSERT INTO sms_log (lead_id, phone, message, status) VALUES (?,?,?,?)", (lead["id"], lead["phone"], personalized, "sent"))
            conn.commit()
            conn.close()
            sent += 1
        except Exception as e:
            conn = get_db()
            conn.execute("INSERT INTO sms_log (lead_id, phone, message, status) VALUES (?,?,?,?)", (lead["id"], lead["phone"], personalized, f"failed: {e}"))
            conn.commit()
            conn.close()
            failed += 1

    return {"ok": True, "sent": sent, "failed": failed}


@app.get("/api/settings")
async def get_settings():
    cfg = load_config()
    return {
        "twilio_account_sid": cfg.get("twilio_account_sid", ""),
        "twilio_auth_token":  "***" if cfg.get("twilio_auth_token") else "",
        "twilio_from_number": cfg.get("twilio_from_number", ""),
        "notify_number":      cfg.get("notify_number", ""),
        "base_url":           cfg.get("base_url", ""),
    }

@app.post("/api/settings")
async def save_settings(request: Request):
    body = await request.json()
    allowed = {"twilio_account_sid", "twilio_auth_token", "twilio_from_number", "notify_number", "base_url"}
    updates = {k: v for k, v in body.items() if k in allowed and v and v != "***"}
    save_config(updates)
    return {"ok": True}


def send_twilio_sms(account_sid, auth_token, from_number, to, body):
    import base64, urllib.parse, urllib.request
    data = urllib.parse.urlencode({"To": to, "From": from_number, "Body": body}).encode()
    req  = urllib.request.Request(
        f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
        data=data,
        headers={"Authorization": "Basic " + base64.b64encode(f"{account_sid}:{auth_token}".encode()).decode()}
    )
    urllib.request.urlopen(req, timeout=10)


@app.post("/api/sequence/start")
async def sequence_start(request: Request):
    body = await request.json()
    ids  = body.get("ids", [])
    if not ids:
        return JSONResponse({"ok": False, "error": "No leads selected"})

    cfg = load_config()
    if not all([cfg.get("twilio_account_sid"), cfg.get("twilio_auth_token"), cfg.get("twilio_from_number")]):
        return JSONResponse({"ok": False, "error": "Twilio not configured. Go to Settings."})

    now   = datetime.now()
    sent  = 0
    failed = 0
    conn  = get_db()
    leads = conn.execute(f"SELECT * FROM leads WHERE id IN ({','.join('?'*len(ids))})", ids).fetchall()
    conn.close()

    for lead in leads:
        lead = dict(lead)
        if not lead.get("phone") or lead.get("status") in ("opted_out", "has_website"):
            continue
        city = (lead.get("location") or "your area").split(",")[0].split(" IL")[0].strip()
        msg  = SEQUENCE[0].replace("{name}", lead["name"]).replace("{preview_url}", absolute_url(ensure_preview(lead))).replace("{category}", lead.get("category") or "business").replace("{city}", city)
        try:
            await asyncio.sleep(1.1)
            send_twilio_sms(cfg["twilio_account_sid"], cfg["twilio_auth_token"], cfg["twilio_from_number"], lead["phone"], msg)
            follow_up_at = (now + timedelta(days=SEQUENCE_DELAYS[1])).strftime("%Y-%m-%d %H:%M:%S")
            conn = get_db()
            conn.execute("UPDATE leads SET sequence_active=1, sequence_step=1, follow_up_at=?, status=CASE WHEN status='new' THEN 'contacted' ELSE status END, sms_sent=sms_sent+1 WHERE id=?", (follow_up_at, lead["id"]))
            conn.execute("INSERT INTO sms_log (lead_id, phone, message, status) VALUES (?,?,?,?)", (lead["id"], lead["phone"], msg, "sent"))
            conn.commit()
            conn.close()
            sent += 1
        except Exception as e:
            failed += 1

    return {"ok": True, "sent": sent, "failed": failed}


async def process_due_sequences() -> int:
    """Send all due follow-up texts. Called by the hourly scheduler and the API."""
    cfg = load_config()
    if not all([cfg.get("twilio_account_sid"), cfg.get("twilio_auth_token"), cfg.get("twilio_from_number")]):
        return 0

    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_db()
    due  = conn.execute("SELECT * FROM leads WHERE sequence_active=1 AND follow_up_at <= ? AND sequence_step < ? AND status NOT IN ('replied','closed','opted_out','has_website')", (now, len(SEQUENCE))).fetchall()
    conn.close()

    sent = 0
    for lead in due:
        lead = dict(lead)
        step = lead["sequence_step"]
        if step >= len(SEQUENCE):
            conn = get_db()
            conn.execute("UPDATE leads SET sequence_active=0 WHERE id=?", (lead["id"],))
            conn.commit()
            conn.close()
            continue

        city    = (lead.get("location") or "your area").split(",")[0].split(" IL")[0].strip()
        preview = absolute_url(ensure_preview(lead))
        msg     = SEQUENCE[step].replace("{name}", lead["name"]).replace("{preview_url}", preview).replace("{category}", lead.get("category") or "business").replace("{city}", city)

        try:
            await asyncio.sleep(1.1)
            send_twilio_sms(cfg["twilio_account_sid"], cfg["twilio_auth_token"], cfg["twilio_from_number"], lead["phone"], msg)
            next_step = step + 1
            if next_step < len(SEQUENCE):
                follow_up_at = (datetime.now() + timedelta(days=SEQUENCE_DELAYS[next_step] - SEQUENCE_DELAYS[step])).strftime("%Y-%m-%d %H:%M:%S")
                active = 1
            else:
                follow_up_at = None
                active = 0
            conn = get_db()
            conn.execute("UPDATE leads SET sequence_step=?, sequence_active=?, follow_up_at=?, sms_sent=sms_sent+1 WHERE id=?", (next_step, active, follow_up_at, lead["id"]))
            conn.execute("INSERT INTO sms_log (lead_id, phone, message, status) VALUES (?,?,?,?)", (lead["id"], lead["phone"], msg, "sent"))
            conn.commit()
            conn.close()
            sent += 1
        except Exception:
            pass

    return sent


@app.post("/api/sequence/process")
async def sequence_process():
    cfg = load_config()
    if not all([cfg.get("twilio_account_sid"), cfg.get("twilio_auth_token"), cfg.get("twilio_from_number")]):
        return JSONResponse({"ok": False, "error": "Twilio not configured"})
    sent = await process_due_sequences()
    return {"ok": True, "processed": sent}


TWIML_EMPTY = '<?xml version="1.0" encoding="UTF-8"?><Response></Response>'
STOP_WORDS  = {"stop", "stopall", "unsubscribe", "cancel", "end", "quit", "revoke", "optout", "opt out"}


@app.post("/api/webhooks/sms/reply")
async def sms_reply_webhook(request: Request):
    form   = await request.form()
    params = dict(form)
    if not validate_twilio_signature(str(request.url), params, request.headers.get("X-Twilio-Signature", "")):
        return HTMLResponse(content=TWIML_EMPTY, media_type="application/xml", status_code=403)

    from_  = form.get("From", "")
    body   = form.get("Body", "").strip()

    def normalize(p):
        return re.sub(r"\D", "", p or "")

    from_digits = normalize(from_)

    conn  = get_db()
    leads = conn.execute("SELECT * FROM leads WHERE phone IS NOT NULL").fetchall()
    conn.close()

    matched = None
    for lead in leads:
        if normalize(lead["phone"]) == from_digits:
            matched = dict(lead)
            break

    if not matched:
        return HTMLResponse(content=TWIML_EMPTY, media_type="application/xml")

    # Opt-out: mark and stop everything for this lead, never message them again
    if body.lower().strip() in STOP_WORDS:
        conn = get_db()
        conn.execute("UPDATE leads SET status='opted_out', sequence_active=0 WHERE id=?", (matched["id"],))
        conn.execute("INSERT INTO sms_log (lead_id, phone, message, status) VALUES (?,?,?,?)",
                     (matched["id"], from_, f"INBOUND: {body}", "opted_out"))
        conn.commit()
        conn.close()
        return HTMLResponse(content=TWIML_EMPTY, media_type="application/xml")

    intent = classify_reply(body)

    # Determine new status
    if intent == "has_website":
        new_status = "has_website"
    elif intent == "no_website":
        new_status = "building"
    else:
        new_status = "replied"

    conn = get_db()
    conn.execute("UPDATE leads SET status=?, sequence_active=0 WHERE id=?", (new_status, matched["id"]))
    conn.execute("INSERT INTO sms_log (lead_id, phone, message, status) VALUES (?,?,?,?)",
                 (matched["id"], from_, f"INBOUND: {body}", "received"))
    conn.commit()
    conn.close()

    cfg = load_config()
    has_twilio = all([cfg.get("twilio_account_sid"), cfg.get("twilio_auth_token"), cfg.get("twilio_from_number")])

    if intent == "no_website" and has_twilio:
        # Build preview and send the link
        try:
            preview_path = ensure_preview(matched)
            base = (cfg.get("base_url") or str(request.base_url)).rstrip("/")
            preview_link = f"{base}{preview_path}"

            conn = get_db()
            conn.execute("UPDATE leads SET status='preview_sent' WHERE id=?", (matched["id"],))
            conn.commit()
            conn.close()

            reply_msg = (
                f"Perfect — I actually already built one for you. "
                f"Here's your free preview site: {preview_link}\n\n"
                f"Reply YES if you want to keep it and I'll get it live for you."
            )
            send_twilio_sms(
                cfg["twilio_account_sid"], cfg["twilio_auth_token"],
                cfg["twilio_from_number"], from_, reply_msg
            )
            conn = get_db()
            conn.execute("INSERT INTO sms_log (lead_id, phone, message, status) VALUES (?,?,?,?)",
                         (matched["id"], from_, reply_msg, "sent"))
            conn.commit()
            conn.close()
        except Exception:
            pass

    # Notify Josh with intent context
    notify_number = cfg.get("notify_number")
    if notify_number and has_twilio:
        try:
            labels = {"no_website": "NO website", "has_website": "HAS a website already", "other": "replied"}
            notify_msg = (
                f"Reply from {matched['name']}: \"{body}\"\n"
                f"Intent: {labels.get(intent, 'replied')}\n"
                f"Phone: {from_}"
            )
            send_twilio_sms(
                cfg["twilio_account_sid"], cfg["twilio_auth_token"],
                cfg["twilio_from_number"], notify_number, notify_msg
            )
        except Exception:
            pass

    return HTMLResponse(content=TWIML_EMPTY, media_type="application/xml")


@app.post("/api/sequence/stop")
async def sequence_stop(request: Request):
    body = await request.json()
    ids  = body.get("ids", [])
    if not ids:
        return JSONResponse({"ok": False, "error": "No leads selected"})
    conn = get_db()
    conn.execute(f"UPDATE leads SET sequence_active=0 WHERE id IN ({','.join('?'*len(ids))})", ids)
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/api/sequence/status")
async def sequence_status():
    conn  = get_db()
    active = conn.execute("SELECT COUNT(*) FROM leads WHERE sequence_active=1").fetchone()[0]
    due    = conn.execute("SELECT COUNT(*) FROM leads WHERE sequence_active=1 AND follow_up_at <= ?", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),)).fetchone()[0]
    conn.close()
    return {"active": active, "due": due}


@app.get("/api/stats")
async def get_stats():
    conn = get_db()
    total      = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    by_status  = {r["status"]: r["cnt"] for r in conn.execute("SELECT status, COUNT(*) as cnt FROM leads GROUP BY status").fetchall()}
    revenue    = conn.execute("SELECT COALESCE(SUM(amount),0) FROM deals").fetchone()[0]
    deals      = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
    avg_deal   = (revenue / deals) if deals > 0 else 0
    this_month = conn.execute("SELECT COALESCE(SUM(amount),0) FROM deals WHERE strftime('%Y-%m', date) = strftime('%Y-%m', 'now')").fetchone()[0]
    by_cat     = [dict(r) for r in conn.execute("SELECT category, COUNT(*) as cnt FROM leads GROUP BY category ORDER BY cnt DESC LIMIT 8").fetchall()]
    by_loc     = [dict(r) for r in conn.execute("SELECT location, COUNT(*) as cnt FROM leads GROUP BY location ORDER BY cnt DESC").fetchall()]
    sms_sent   = conn.execute("SELECT COALESCE(SUM(sms_sent),0) FROM leads").fetchone()[0]
    conn.close()
    return {"total": total, "by_status": by_status, "revenue": revenue, "deals": deals, "avg_deal": avg_deal, "this_month": this_month, "by_category": by_cat, "by_location": by_loc, "sms_sent": sms_sent}


@app.get("/api/deals")
async def get_deals():
    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM deals ORDER BY date DESC").fetchall()]
    conn.close()
    return rows

@app.post("/api/deals")
async def add_deal(request: Request):
    body = await request.json()
    if not body.get("business") or not body.get("amount"):
        return JSONResponse({"ok": False, "error": "Missing fields"})
    conn = get_db()
    conn.execute("INSERT INTO deals (lead_id, business, amount, date, notes) VALUES (?,?,?,?,?)",
        (body.get("lead_id"), body["business"], float(body["amount"]), body.get("date", datetime.now().strftime("%Y-%m-%d")), body.get("notes", "")))
    conn.commit()
    conn.close()
    return {"ok": True}

@app.delete("/api/deals/{deal_id}")
async def delete_deal(deal_id: int):
    conn = get_db()
    conn.execute("DELETE FROM deals WHERE id = ?", (deal_id,))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    with open("static/index.html") as f:
        return f.read()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=bool(os.environ.get("DEV")))
