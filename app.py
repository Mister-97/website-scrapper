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

import requests as http_requests
import uvicorn
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

DATA_DIR    = os.environ.get("DATA_DIR", ".")
DB_PATH    = os.path.join(DATA_DIR, "leads.db")
CONFIG_PATH = os.path.join(DATA_DIR, "config.json")
scraper_running = False
scraper_stop_requested = False
scraper_log: list[str] = []


def shorten_url(url: str) -> str:
    return url


# ── Config ────────────────────────────────────────────────────────────────────
# Sensitive Twilio creds are read from environment variables first.
# Non-sensitive config (sequences, base_url, notify_number) lives in config.json.

_ENV_KEYS = {
    "twilio_account_sid": "TWILIO_ACCOUNT_SID",
    "twilio_auth_token":  "TWILIO_AUTH_TOKEN",
    "twilio_from_number": "TWILIO_FROM_NUMBER",
    "base_url":           "BASE_URL",
}

def load_config():
    cfg: dict = {}
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
    # Env vars override config.json for sensitive fields
    for cfg_key, env_key in _ENV_KEYS.items():
        val = os.environ.get(env_key)
        if val:
            cfg[cfg_key] = val
    return cfg

def save_config(data: dict):
    existing = load_config()
    # Never persist creds that come from env vars back to disk
    env_sourced = {k for k, v in _ENV_KEYS.items() if os.environ.get(v)}
    for k, v in data.items():
        if k not in env_sourced:
            existing[k] = v
    with open(CONFIG_PATH, "w") as f:
        json.dump({k: v for k, v in existing.items() if k not in env_sourced}, f, indent=2)


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
    # Add direction column to sms_log for conversation thread view
    try:
        conn.execute("ALTER TABLE sms_log ADD COLUMN direction TEXT DEFAULT 'outbound'")
        conn.commit()
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE leads ADD COLUMN first_contacted_at TEXT")
        conn.commit()
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE sms_log ADD COLUMN twilio_sid TEXT")
        conn.commit()
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE sms_log ADD COLUMN delivery_status TEXT DEFAULT 'sent'")
        conn.commit()
    except Exception:
        pass
    conn.close()


# ── Sequence templates ────────────────────────────────────────────────────────

_DEFAULT_SEQUENCE = [
    "Hey I was searching for {name} but couldn't find a website, do you guys have one?",
    "Yeah I actually built you a free preview to show you what it could look like. Want to see it? {preview_url}",
    "No worries if not, just didn't want you losing customers to competitors who have sites.",
    "Last one I promise. I'm about to give this slot to another {category} in {city}, wanted to check if you wanted it first.",
]

SEQUENCE_DELAYS = [0, 1, 3, 5]  # days after sequence start


def get_sequence() -> list:
    """Return sequence messages from config (editable in Settings) or fall back to defaults."""
    cfg = load_config()
    custom = cfg.get("sequence_messages")
    if custom and len(custom) == len(_DEFAULT_SEQUENCE):
        return custom
    return _DEFAULT_SEQUENCE


def classify_reply(text: str, current_status: str = "") -> str:
    """Return 'claim', 'no_website', 'has_website', or 'other'."""
    t = text.lower().strip()
    # Explicit claim intent: CTA button text or high-intent phrasing
    claim_patterns = [
        r"\bclaim\b", r"want to claim", r"i want it", r"get it live",
        r"\bsign me up\b", r"\blet'?s do it\b", r"\blet'?s go\b",
        r"i'?m interested", r"\byes please\b", r"\bdo it\b",
    ]
    for p in claim_patterns:
        if re.search(p, t):
            return "claim"
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
            # If we already sent a preview, "yes" means they want it - not that they have a site
            if current_status in ("preview_sent", "building"):
                return "claim"
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


PREVIEWS_DIR = os.path.join(DATA_DIR, "previews")

@asynccontextmanager
async def lifespan(app: FastAPI):
    import shutil
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(DB_PATH):
        backup = os.path.join(DATA_DIR, f"leads_backup_{datetime.now().strftime('%Y%m%d')}.db")
        if not os.path.exists(backup):
            shutil.copy2(DB_PATH, backup)
            print(f"[backup] Created {backup}")
    init_db()
    os.makedirs("static", exist_ok=True)
    os.makedirs(PREVIEWS_DIR, exist_ok=True)
    app.mount("/previews", StaticFiles(directory=PREVIEWS_DIR), name="previews")
    app.mount("/static", StaticFiles(directory="static"), name="static")
    task = asyncio.create_task(sequence_loop())
    print("[tip] To keep follow-ups running while your Mac is idle: caffeinate -i python app.py")
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)


# ── Scraper ───────────────────────────────────────────────────────────────────

async def run_scraper(categories: list[str], locations: list[str]):
    global scraper_running, scraper_stop_requested, scraper_log
    scraper_running = True
    scraper_stop_requested = False
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
                    if scraper_stop_requested:
                        log("Scrape stopped early by user.")
                        break
                    log(f"Searching: {category} in {location}")
                    found = 0
                    try:
                        query = f"{category} in {location}"
                        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
                        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        await page.wait_for_timeout(2500)

                        panel = page.locator('div[role="feed"]')
                        prev_count = 0
                        for _ in range(20):
                            await panel.evaluate("el => el.scrollBy(0, 800)")
                            await page.wait_for_timeout(800)
                            cur_count = await page.locator('a[href*="/maps/place/"]').count()
                            if cur_count == prev_count:
                                break
                            prev_count = cur_count

                        listings = await page.locator('a[href*="/maps/place/"]').all()
                        hrefs, seen_hrefs = [], set()
                        for l in listings:
                            href = await l.get_attribute("href")
                            if href and href not in seen_hrefs:
                                seen_hrefs.add(href)
                                hrefs.append(href)

                        for href in hrefs[:60]:
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
                if scraper_stop_requested:
                    break

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
    "car detailing": "#d32f2f",
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
        "photo": "photo-1581578731548-c64695cc6952",
    },
    "dry cleaner": {
        "headline": "Drop Off Today, Pick Up Tomorrow",
        "sub": "Professional dry cleaning and laundry service. We handle your clothes like they are our own.",
        "services": [("Dry Cleaning", "👔"), ("Shirt Laundering", "👕"), ("Alterations", "✂️"), ("Wedding Gown", "👰"), ("Leather & Suede", "🧥"), ("Same-Day Service", "⚡")],
        "trust": ["Next-Day Turnaround", "Free Pickup & Delivery", "Eco-Friendly Solvents"],
        "photo": "photo-1558769132-cb1aea458c5e",
    },
    "car detailing": {
        "headline": "Showroom Shine, Every Time",
        "sub": "Professional detailing and paint protection that keeps your vehicle looking brand new. Interior, exterior, and everything in between.",
        "services": [("Full Detail", "🚗"), ("Interior Detail", "🧽"), ("Wash & Wax", "🫧"), ("Ceramic Coating", "🛡️"), ("Paint Correction", "✨"), ("Headlight Restoration", "💡")],
        "trust": ["Fully Mobile Available", "Premium Products Only", "Satisfaction Guaranteed"],
        "photo": "photo-1607860108855-64acf2078ed9",
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

# ── SVG icons (no emojis in previews — real icons only) ──────────────────────

_ICON_PATHS = {
    "check":    '<polyline points="20 6 9 17 4 12"/>',
    "star":     '<polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>',
    "scissors": '<circle cx="6" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><line x1="20" y1="4" x2="8.12" y2="15.88"/><line x1="14.47" y1="14.48" x2="20" y2="20"/><line x1="8.12" y1="8.12" x2="12" y2="12"/>',
    "pen":      '<path d="M17 3a2.83 2.83 0 0 1 4 4L7.5 20.5 2 22l1.5-5.5z"/>',
    "droplet":  '<path d="M12 2.69l5.66 5.66a8 8 0 1 1-11.31 0z"/>',
    "wind":     '<path d="M9.59 4.59A2 2 0 1 1 11 8H2m10.59 11.41A2 2 0 1 0 14 16H2m15.73-8.27A2.5 2.5 0 1 1 19.5 12H2"/>',
    "sun":      '<circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>',
    "zap":      '<polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>',
    "shield":   '<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>',
    "leaf":     '<path d="M11 20A7 7 0 0 1 9.8 6.1C15.5 5 17 4.48 19 2c1 2 2 4.18 2 8 0 5.5-4.78 10-10 10z"/><path d="M2 21c0-3 1.85-5.36 5.08-6C9.5 14.52 12 13 13 12"/>',
    "home":     '<path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/>',
    "heart":    '<path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/>',
    "key":      '<path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"/>',
    "truck":    '<rect x="1" y="3" width="15" height="13"/><polygon points="16 8 20 8 23 11 23 16 16 16 16 8"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/>',
    "wrench":   '<path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z"/>',
    "clipboard":'<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/>',
    "utensils": '<path d="M3 2v7a4 4 0 0 0 8 0V2"/><line x1="7" y1="13" x2="7" y2="22"/><path d="M18 2c-1.5 2-2 4-2 6 0 2.5 1 4 2 4s2-1.5 2-4c0-2-.5-4-2-6z"/><line x1="18" y1="12" x2="18" y2="22"/>',
    "award":    '<circle cx="12" cy="8" r="7"/><polyline points="8.21 13.89 7 23 12 20 17 23 15.79 13.88"/>',
    "diamond":  '<polygon points="12 2 22 12 12 22 2 12 12 2"/>',
}

def _icon(key: str, color: str, size: int = 24) -> str:
    path = _ICON_PATHS.get(key, _ICON_PATHS["star"])
    return (f'<svg width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" stroke="{color}" '
            f'stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" style="flex-shrink:0;">{path}</svg>')

_KEYWORD_ICONS = [
    (("lock", "key", "deadbolt", "rekey"), "key"),
    (("cut", "fade", "shave", "trim", "line up", "blowout", "haircut", "style", "extension"), "scissors"),
    (("paint", "color", "art", "stain", "highlight", "tattoo", "cover-up", "touch-up", "flash", "design"), "pen"),
    (("oil", "wash", "wax", "leak", "drain", "water", "irrigation", "pressure"), "droplet"),
    (("ac ", "a/c", "air", "duct"), "wind"),
    (("heat", "furnace"), "sun"),
    (("electric", "light", "panel", "outlet", "rewir", "ev ", "charger"), "zap"),
    (("pest", "termite", "bug", "ant", "rodent", "mosquito", "wasp", "protect", "coating", "ceramic"), "shield"),
    (("lawn", "tree", "mulch", "plant", "landscap", "snow"), "leaf"),
    (("roof", "floor", "gutter", "remodel", "home", "house", "move", "carpet", "tile", "hardwood", "laminate", "shingle", "storm", "window", "office"), "home"),
    (("infant", "toddler", "kid", "child", "preschool", "school", "camp", "drop-in"), "heart"),
    (("estimate", "consult", "inspect", "quote"), "clipboard"),
    (("dine", "takeout", "cater", "delivery", "happy", "event"), "utensils"),
    (("tire", "car", "auto", "vehicle", "transmission", "engine", "brake", "tow", "headlight", "detail"), "truck"),
    (("repair", "install", "replace", "diagnostic", "mainten", "rotation"), "wrench"),
]

def _svc_icon(svc: str, color: str, size: int = 24) -> str:
    s = svc.lower()
    for keys, icon_key in _KEYWORD_ICONS:
        if any(k in s for k in keys):
            return _icon(icon_key, color, size)
    return _icon("star", color, size)


def generate_preview_html(lead: dict) -> str:
    name     = lead.get("name", "Your Business")
    phone    = lead.get("phone", "")
    address  = lead.get("address", "")
    category = (lead.get("category") or "business").lower()
    logo_url = lead.get("logo_url", "")
    city     = (lead.get("location") or "").split(" IL")[0].strip()
    lead_id  = lead.get("id", 0)

    # "Claim this site" buttons text YOUR Twilio number (so the reply hits the
    # webhook and notifies you) — not the lead's own number
    from urllib.parse import quote
    claim_to  = load_config().get("twilio_from_number") or phone
    claim_sms = f"sms:{claim_to}?&body=" + quote(f"I want to claim the website for {name}")

    accent    = CATEGORY_ACCENTS.get(category, "#6366f1")
    cat_label = category.title()
    # Template is chosen by industry so each preview matches how real
    # competitor sites in that space actually look
    GROOMING = {"barber shop", "tattoo shop"}
    BEAUTY   = {"nail salon", "hair salon", "daycare", "cleaning service", "dry cleaner"}
    if category in GROOMING:
        template = 0
    elif category in BEAUTY:
        template = 1
    else:
        template = 2

    ind       = INDUSTRY_DATA.get(category, _DEFAULT_INDUSTRY)
    headline  = ind["headline"].replace("{city}", city or "Your Area").replace("{cat}", cat_label)
    sub       = ind["sub"].replace("{city}", city or "the local area").replace("{cat}", cat_label.lower())
    services  = ind["services"]
    trust     = ind["trust"]
    photo_id  = ind.get("photo", "photo-1486406146926-c627a92ad1ab")
    photo_url = f"https://images.unsplash.com/{photo_id}?auto=format&fit=crop&w=1400&q=80"
    # Use Google Maps hero photo as background when available (upscale stored thumbnail)
    if logo_url and "googleusercontent" in logo_url:
        hero_url = re.sub(r'=s\d+(-c)?$', '=s1400', logo_url)
    else:
        hero_url = photo_url

    logo_nav = (
        f'<img src="{logo_url}" onerror="this.style.display=\'none\'" style="height:36px;width:36px;border-radius:8px;object-fit:cover;">'
    ) if logo_url else (
        f'<div style="width:36px;height:36px;border-radius:8px;background:{accent};display:flex;align-items:center;justify-content:center;font-size:16px;font-weight:900;color:white;flex-shrink:0;">{name[0].upper()}</div>'
    )
    phone_svg = '<svg width="17" height="17" fill="currentColor" viewBox="0 0 24 24"><path d="M6.6 10.8c1.4 2.8 3.8 5.1 6.6 6.6l2.2-2.2c.3-.3.7-.4 1-.2 1.1.4 2.3.6 3.6.6.6 0 1 .4 1 1V20c0 .6-.4 1-1 1-9.4 0-17-7.6-17-17 0-.6.4-1 1-1h3.5c.6 0 1 .4 1 1 0 1.3.2 2.5.6 3.6.1.3 0 .7-.2 1L6.6 10.8z"/></svg>'
    sms_svg  = '<svg width="17" height="17" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z"/></svg>'
    footer_html = f"""<footer style="background:#111827;color:#9ca3af;padding:40px 5%;text-align:center;">
  <div style="font-weight:800;font-size:18px;color:#fff;margin-bottom:4px;">{name}</div>
  <div style="font-size:13px;margin-bottom:4px;">{cat_label} in {city if city else 'Your Area'}</div>
  {f'<div style="font-size:13px;margin-bottom:4px;">{address}</div>' if address else ''}
  <div style="font-size:13px;margin-bottom:16px;"><a href="tel:{phone}" style="color:{accent};text-decoration:none;font-weight:700;">{phone}</a></div>
  <div style="border-top:1px solid #1f2937;padding-top:16px;font-size:12px;color:#4b5563;">
    &copy; {name}. This is a free preview website.
    <a href="{claim_sms}" style="color:{accent};text-decoration:none;margin-left:8px;font-weight:600;">Claim it today.</a>
  </div>
</footer>"""

    # Shared building blocks — templates are modeled on real competitor sites
    menu_rows = "".join(
        f'<div class="menu-item"><span class="menu-name">{svc}</span><span class="dots"></span><a class="menu-call" href="tel:{phone}">Call to Book</a></div>'
        for svc, _ in services
    )
    beauty_tiles = "".join(
        f'<a class="tile" href="tel:{phone}"><span class="tile-name">{svc}</span><span class="tile-arrow">&rarr;</span></a>'
        for svc, _ in services
    )
    pro_cards = "".join(
        f'<div class="pro-card"><div class="pro-ico">{_svc_icon(svc, accent, 30)}</div><h3>{svc}</h3><p>Professional {svc.lower()} done right by an experienced local team, with honest pricing and quality workmanship you can count on.</p><a href="tel:{phone}">Free Estimate &rarr;</a></div>'
        for svc, _ in services
    )
    trust_checks = "".join(f'<li><span>{_icon("check", "#fff", 13)}</span>{t}</li>' for t in trust)

    # ── Template 0: Heritage grooming — barbershop/tattoo (The Chair style) ───
    if template == 0:
        return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{name} | {cat_label} in {city or 'Your Area'}</title>
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,500;0,600;0,700;1,500&family=Jost:wght@400;500;600&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
html{{scroll-behavior:smooth;}}
:root{{--ink:#141210;--panel:#1d1a17;--cream:#f1e9d8;--gold:#c2a05a;}}
body{{font-family:'Jost',sans-serif;background:var(--ink);color:var(--cream);min-height:100vh;padding-bottom:70px;}}
.preview-bar{{background:var(--gold);color:#141210;text-align:center;padding:8px 16px;font-size:12px;font-weight:600;}}
.preview-bar a{{color:#141210;text-decoration:underline;margin-left:6px;font-weight:700;}}
nav{{background:rgba(20,18,16,.96);border-bottom:1px solid rgba(241,233,216,.12);position:sticky;top:0;z-index:100;}}
.nav-inner{{max-width:1100px;margin:0 auto;padding:0 20px;height:76px;display:flex;align-items:center;justify-content:space-between;}}
.nav-name{{font-family:'Playfair Display',serif;font-weight:600;font-size:21px;color:var(--cream);letter-spacing:.02em;}}
.nav-links{{display:flex;align-items:center;gap:28px;}}
.nav-links a{{font-size:13px;font-weight:500;letter-spacing:.16em;text-transform:uppercase;color:rgba(241,233,216,.75);text-decoration:none;}}
.nav-links a:hover{{color:var(--gold);}}
.nav-book{{border:1px solid var(--gold);color:var(--gold) !important;padding:11px 26px;}}
.nav-book:hover{{background:var(--gold);color:#141210 !important;}}
.hero{{position:relative;min-height:88vh;display:flex;align-items:center;justify-content:center;text-align:center;}}
.hero-bg{{position:absolute;inset:0;background:url('{hero_url}') center/cover;}}
.hero-overlay{{position:absolute;inset:0;background:rgba(20,18,16,.74);}}
.hero-inner{{position:relative;z-index:2;max-width:740px;padding:90px 20px;}}
.hero-eyebrow{{color:var(--gold);font-size:12px;font-weight:600;letter-spacing:.34em;text-transform:uppercase;margin-bottom:22px;}}
.hero h1{{font-family:'Playfair Display',serif;font-weight:600;font-size:clamp(2.6rem,6.4vw,4.6rem);line-height:1.08;color:#fff;margin-bottom:18px;}}
.hero-tagline{{font-family:'Playfair Display',serif;font-style:italic;font-size:clamp(1.05rem,2.4vw,1.35rem);color:rgba(241,233,216,.85);margin-bottom:34px;}}
.ornament{{display:flex;align-items:center;justify-content:center;gap:16px;margin:0 auto 34px;color:var(--gold);}}
.ornament::before,.ornament::after{{content:'';width:64px;height:1px;background:var(--gold);opacity:.6;}}
.btn{{display:inline-flex;align-items:center;gap:9px;font-weight:600;font-size:13.5px;letter-spacing:.18em;text-transform:uppercase;padding:17px 38px;text-decoration:none;}}
.btn-gold{{background:var(--gold);color:#141210;}}
.btn-line{{border:1px solid rgba(241,233,216,.45);color:var(--cream);}}
.section{{max-width:1100px;margin:0 auto;padding:90px 20px;}}
.sec-head{{text-align:center;margin-bottom:50px;}}
.sec-head .eyebrow{{color:var(--gold);font-size:11.5px;font-weight:600;letter-spacing:.3em;text-transform:uppercase;margin-bottom:12px;}}
.sec-head h2{{font-family:'Playfair Display',serif;font-weight:600;font-size:clamp(1.9rem,4vw,2.7rem);color:#fff;}}
.menu{{max-width:720px;margin:0 auto;display:flex;flex-direction:column;gap:4px;}}
.menu-item{{display:flex;align-items:baseline;gap:14px;padding:17px 4px;}}
.menu-name{{font-family:'Playfair Display',serif;font-size:clamp(1.05rem,2.2vw,1.3rem);color:var(--cream);}}
.dots{{flex:1;border-bottom:1px dotted rgba(241,233,216,.3);transform:translateY(-5px);}}
.menu-call{{color:var(--gold);font-size:12px;font-weight:600;letter-spacing:.16em;text-transform:uppercase;text-decoration:none;white-space:nowrap;}}
.craft{{background:var(--panel);}}
.craft-inner{{max-width:1100px;margin:0 auto;padding:90px 20px;display:grid;grid-template-columns:1fr 1fr;gap:60px;align-items:center;}}
@media(max-width:820px){{.craft-inner{{grid-template-columns:1fr;}}}}
.craft-photo{{position:relative;}}
.craft-photo img{{width:100%;aspect-ratio:4/4.6;object-fit:cover;display:block;filter:grayscale(.25);}}
.craft-photo::after{{content:'';position:absolute;inset:14px;border:1px solid rgba(194,160,90,.5);pointer-events:none;}}
.craft-text .eyebrow{{color:var(--gold);font-size:11.5px;font-weight:600;letter-spacing:.3em;text-transform:uppercase;margin-bottom:14px;}}
.craft-text h2{{font-family:'Playfair Display',serif;font-weight:600;font-size:clamp(1.7rem,3.4vw,2.4rem);color:#fff;margin-bottom:18px;line-height:1.2;}}
.craft-text p{{color:rgba(241,233,216,.7);line-height:1.85;font-size:15.5px;margin-bottom:26px;}}
.craft-text ul{{list-style:none;display:flex;flex-direction:column;gap:13px;}}
.craft-text li{{display:flex;gap:13px;align-items:center;font-size:14.5px;color:var(--cream);}}
.craft-text li span{{color:var(--gold);font-size:15px;}}
.visit{{border-top:1px solid rgba(241,233,216,.12);}}
.visit-inner{{max-width:1100px;margin:0 auto;padding:80px 20px;display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:44px;text-align:center;}}
.visit h3{{font-size:11.5px;font-weight:600;letter-spacing:.3em;text-transform:uppercase;color:var(--gold);margin-bottom:14px;}}
.visit p{{font-family:'Playfair Display',serif;font-size:1.15rem;color:var(--cream);line-height:1.6;}}
.visit a{{color:var(--cream);text-decoration:none;}}
.cta{{text-align:center;background:var(--panel);border-top:1px solid rgba(241,233,216,.12);padding:90px 20px;}}
.cta h2{{font-family:'Playfair Display',serif;font-weight:600;font-size:clamp(2rem,4.6vw,3rem);color:#fff;margin-bottom:14px;}}
.cta p{{color:rgba(241,233,216,.7);margin-bottom:36px;font-size:15.5px;}}
.sticky-bar{{position:fixed;bottom:0;left:0;right:0;background:#141210;border-top:1px solid rgba(241,233,216,.18);padding:10px 16px;display:flex;gap:10px;z-index:200;}}
.sticky-call{{flex:1;display:flex;align-items:center;justify-content:center;gap:8px;background:var(--gold);color:#141210;font-weight:600;font-size:13px;letter-spacing:.1em;text-transform:uppercase;padding:14px;text-decoration:none;}}
.sticky-claim{{flex:1;display:flex;align-items:center;justify-content:center;border:1px solid rgba(241,233,216,.4);color:var(--cream);font-weight:500;font-size:12.5px;letter-spacing:.08em;text-transform:uppercase;padding:14px;text-decoration:none;}}
@media(max-width:700px){{.nav-links a:not(.nav-book){{display:none;}}}}
</style></head><body>
<div class="preview-bar">This is a FREE preview website built for {name}.<a href="{claim_sms}">Text us to claim it</a></div>
<nav><div class="nav-inner">
  <span class="nav-name">{name}</span>
  <div class="nav-links">
    <a href="#menu">Services</a>
    <a href="#about">About</a>
    <a href="#visit">Visit</a>
    <a href="tel:{phone}" class="nav-book">Book Now</a>
  </div>
</div></nav>
<section class="hero">
  <div class="hero-bg"></div><div class="hero-overlay"></div>
  <div class="hero-inner">
    <div class="hero-eyebrow">{cat_label} &mdash; {city if city else 'Your Area'}</div>
    <h1>{name}</h1>
    <div class="hero-tagline">{headline}</div>
    <div class="ornament">{_icon("diamond", "#c2a05a", 15)}</div>
    <div style="display:flex;gap:14px;justify-content:center;flex-wrap:wrap;">
      <a href="tel:{phone}" class="btn btn-gold">Call to Book</a>
      <a href="sms:{phone}" class="btn btn-line">Text Us</a>
    </div>
  </div>
</section>
<section class="section" id="menu">
  <div class="sec-head">
    <div class="eyebrow">Fine Edges, Timeless Craft</div>
    <h2>Service Menu</h2>
  </div>
  <div class="menu">{menu_rows}</div>
</section>
<section class="craft" id="about"><div class="craft-inner">
  <div class="craft-photo"><img src="{photo_url}" alt="{name}"></div>
  <div class="craft-text">
    <div class="eyebrow">Our Philosophy</div>
    <h2>We Respect the Craft</h2>
    <p>{sub} At {name}, every visit is a ritual, not a rush job. Locally owned in {city if city else 'your area'}, we take the time to get it right.</p>
    <ul>{''.join(f'<li><span>{_icon("check", "#c2a05a", 15)}</span>{t}</li>' for t in trust)}</ul>
  </div>
</div></section>
<section class="visit" id="visit"><div class="visit-inner">
  {f'<div><h3>Visit Us</h3><p>{address}</p></div>' if address else ''}
  <div><h3>Hours</h3><p>Mon&ndash;Sat 9am&ndash;7pm<br>Sunday Closed</p></div>
  <div><h3>Contact</h3><p><a href="tel:{phone}">{phone}</a></p></div>
</div></section>
<section class="cta">
  <h2>Look Sharp. Feel Sharp.</h2>
  <p>Call or text {name} to book your appointment today.</p>
  <div style="display:flex;gap:14px;justify-content:center;flex-wrap:wrap;">
    <a href="tel:{phone}" class="btn btn-gold">Call {phone}</a>
    <a href="sms:{phone}" class="btn btn-line">Send a Text</a>
  </div>
</section>
{footer_html}
<div class="sticky-bar">
  <a href="tel:{phone}" class="sticky-call">Call to Book</a>
  <a href="{claim_sms}" class="sticky-claim">Claim This Site Free</a>
</div>
</body></html>"""

    # ── Template 1: Serene beauty — salon/spa (Harmony / LaBo style) ──────────
    elif template == 1:
        return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{name} | {cat_label} in {city or 'Your Area'}</title>
<link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,500;0,600;1,500&family=Jost:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
html{{scroll-behavior:smooth;}}
:root{{--ink:#3b3531;--soft:#8c827a;--bg:#fffdfa;--panel:#f6f1ea;--rose:#b08968;}}
body{{font-family:'Jost',sans-serif;background:var(--bg);color:var(--ink);min-height:100vh;padding-bottom:70px;font-weight:400;}}
.preview-bar{{background:var(--ink);color:#fffdfa;text-align:center;padding:8px 16px;font-size:12px;font-weight:500;}}
.preview-bar a{{color:#e9c9a8;text-decoration:none;margin-left:6px;font-weight:600;}}
nav{{background:rgba(255,253,250,.96);backdrop-filter:blur(8px);border-bottom:1px solid #eee5da;position:sticky;top:0;z-index:100;}}
.nav-inner{{max-width:1100px;margin:0 auto;padding:0 20px;height:78px;display:flex;align-items:center;justify-content:space-between;}}
.nav-name{{font-family:'Cormorant Garamond',serif;font-weight:600;font-size:23px;color:var(--ink);letter-spacing:.04em;}}
.nav-links{{display:flex;align-items:center;gap:30px;}}
.nav-links a{{font-size:12.5px;font-weight:500;letter-spacing:.2em;text-transform:uppercase;color:var(--soft);text-decoration:none;}}
.nav-links a:hover{{color:var(--rose);}}
.nav-book{{border:1px solid var(--rose);color:var(--rose) !important;padding:11px 28px;border-radius:0;}}
.nav-book:hover{{background:var(--rose);color:#fff !important;}}
.hero{{position:relative;min-height:82vh;display:flex;align-items:center;justify-content:center;text-align:center;}}
.hero-bg{{position:absolute;inset:0;background:url('{hero_url}') center/cover;}}
.hero-overlay{{position:absolute;inset:0;background:rgba(43,36,30,.5);}}
.hero-inner{{position:relative;z-index:2;max-width:760px;padding:90px 20px;}}
.hero-welcome{{color:#fff;font-size:12.5px;font-weight:500;letter-spacing:.42em;text-transform:uppercase;margin-bottom:20px;opacity:.92;}}
.hero h1{{font-family:'Cormorant Garamond',serif;font-weight:500;font-size:clamp(2.6rem,6.2vw,4.4rem);line-height:1.1;color:#fff;letter-spacing:.02em;margin-bottom:18px;}}
.hero p{{color:rgba(255,255,255,.88);font-size:1.05rem;font-weight:300;line-height:1.8;max-width:540px;margin:0 auto 36px;}}
.btn{{display:inline-flex;align-items:center;justify-content:center;gap:9px;font-weight:500;font-size:12.5px;letter-spacing:.22em;text-transform:uppercase;padding:17px 40px;text-decoration:none;}}
.btn-fill{{background:var(--rose);color:#fff;}}
.btn-line{{border:1px solid rgba(255,255,255,.7);color:#fff;}}
.welcome{{max-width:1100px;margin:0 auto;padding:90px 20px 30px;display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:22px;}}
.w-card{{background:var(--panel);padding:42px 34px;text-align:center;}}
.w-card .ico{{font-family:'Cormorant Garamond',serif;font-size:30px;color:var(--rose);margin-bottom:14px;}}
.w-card h3{{font-family:'Cormorant Garamond',serif;font-weight:600;font-size:1.35rem;color:var(--ink);margin-bottom:10px;}}
.w-card p{{font-size:14px;font-weight:300;color:var(--soft);line-height:1.75;}}
.section{{max-width:1100px;margin:0 auto;padding:80px 20px;}}
.sec-head{{text-align:center;margin-bottom:46px;}}
.sec-head .eyebrow{{color:var(--rose);font-size:11.5px;font-weight:500;letter-spacing:.34em;text-transform:uppercase;margin-bottom:12px;}}
.sec-head h2{{font-family:'Cormorant Garamond',serif;font-weight:500;font-size:clamp(2rem,4.4vw,2.9rem);color:var(--ink);}}
.tiles{{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:16px;}}
.tile{{display:flex;align-items:center;justify-content:space-between;gap:14px;background:#fff;border:1px solid #eee5da;padding:26px 28px;text-decoration:none;transition:.25s;}}
.tile:hover{{border-color:var(--rose);background:var(--panel);}}
.tile-name{{font-family:'Cormorant Garamond',serif;font-weight:600;font-size:1.2rem;color:var(--ink);}}
.tile-arrow{{color:var(--rose);font-size:18px;}}
.story{{background:var(--panel);}}
.story-inner{{max-width:1100px;margin:0 auto;padding:90px 20px;display:grid;grid-template-columns:1fr 1fr;gap:60px;align-items:center;}}
@media(max-width:820px){{.story-inner{{grid-template-columns:1fr;}}}}
.story-text .eyebrow{{color:var(--rose);font-size:11.5px;font-weight:500;letter-spacing:.34em;text-transform:uppercase;margin-bottom:14px;}}
.story-text h2{{font-family:'Cormorant Garamond',serif;font-weight:500;font-size:clamp(1.8rem,3.6vw,2.5rem);color:var(--ink);margin-bottom:16px;line-height:1.2;}}
.story-text h2 em{{color:var(--rose);}}
.story-text p{{color:var(--soft);font-weight:300;line-height:1.9;font-size:15.5px;margin-bottom:24px;}}
.story-text ul{{list-style:none;display:flex;flex-direction:column;gap:12px;}}
.story-text li{{display:flex;gap:12px;align-items:center;font-size:14.5px;color:var(--ink);}}
.story-text li span{{color:var(--rose);}}
.story-photo img{{width:100%;aspect-ratio:4/4.8;object-fit:cover;display:block;}}
.info{{max-width:1100px;margin:0 auto;padding:80px 20px;display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:40px;text-align:center;}}
.info h3{{font-size:11.5px;font-weight:500;letter-spacing:.3em;text-transform:uppercase;color:var(--rose);margin-bottom:12px;}}
.info p{{font-family:'Cormorant Garamond',serif;font-size:1.2rem;color:var(--ink);line-height:1.6;}}
.info a{{color:var(--ink);text-decoration:none;}}
.cta{{background:var(--ink);text-align:center;padding:90px 20px;}}
.cta .eyebrow{{color:#e9c9a8;font-size:11.5px;font-weight:500;letter-spacing:.34em;text-transform:uppercase;margin-bottom:14px;}}
.cta h2{{font-family:'Cormorant Garamond',serif;font-weight:500;font-size:clamp(2rem,4.6vw,3rem);color:#fffdfa;margin-bottom:14px;}}
.cta p{{color:rgba(255,253,250,.7);font-weight:300;margin-bottom:36px;}}
.sticky-bar{{position:fixed;bottom:0;left:0;right:0;background:rgba(255,253,250,.97);border-top:1px solid #eee5da;padding:10px 16px;display:flex;gap:10px;z-index:200;}}
.sticky-call{{flex:1;display:flex;align-items:center;justify-content:center;gap:8px;background:var(--rose);color:#fff;font-weight:500;font-size:12.5px;letter-spacing:.14em;text-transform:uppercase;padding:14px;text-decoration:none;}}
.sticky-claim{{flex:1;display:flex;align-items:center;justify-content:center;border:1px solid var(--ink);color:var(--ink);font-weight:500;font-size:12px;letter-spacing:.1em;text-transform:uppercase;padding:14px;text-decoration:none;}}
@media(max-width:700px){{.nav-links a:not(.nav-book){{display:none;}}}}
</style></head><body>
<div class="preview-bar">Free website preview for {name}.<a href="{claim_sms}">Text to claim it</a></div>
<nav><div class="nav-inner">
  <span class="nav-name">{name}</span>
  <div class="nav-links">
    <a href="#services">Services</a>
    <a href="#about">About</a>
    <a href="#visit">Visit</a>
    <a href="tel:{phone}" class="nav-book">Booking</a>
  </div>
</div></nav>
<section class="hero">
  <div class="hero-bg"></div><div class="hero-overlay"></div>
  <div class="hero-inner">
    <div class="hero-welcome">Welcome to</div>
    <h1>{name}</h1>
    <p>{sub}</p>
    <div style="display:flex;gap:14px;justify-content:center;flex-wrap:wrap;">
      <a href="tel:{phone}" class="btn btn-fill">Book Appointment</a>
      <a href="sms:{phone}" class="btn btn-line">Text Us</a>
    </div>
  </div>
</section>
<div class="welcome">
  <div class="w-card"><div class="ico">{_icon("heart", "#b08968", 28)}</div><h3>Relaxation &amp; Care</h3><p>Every visit is designed to leave you feeling refreshed, pampered, and beautiful.</p></div>
  <div class="w-card"><div class="ico">{_icon("star", "#b08968", 28)}</div><h3>Satisfaction First</h3><p>Your happiness is our priority. We are not done until you love the result.</p></div>
  <div class="w-card"><div class="ico">{_icon("award", "#b08968", 28)}</div><h3>Qualified Specialists</h3><p>Skilled, experienced professionals using premium products and sterile tools.</p></div>
</div>
<section class="section" id="services">
  <div class="sec-head">
    <div class="eyebrow">Our Menu</div>
    <h2>Popular Services</h2>
  </div>
  <div class="tiles">{beauty_tiles}</div>
</section>
<section class="story" id="about"><div class="story-inner">
  <div class="story-text">
    <div class="eyebrow">About Us</div>
    <h2>A Moment of <em>Harmony</em> in {city if city else 'Your Day'}</h2>
    <p>{name} is a locally owned {cat_label.lower()} in {city if city else 'your area'} devoted to first-class service in a clean, calming space. Walk in as a guest, leave as a regular.</p>
    <ul>{''.join(f'<li><span>{_icon("check", "#b08968", 15)}</span>{t}</li>' for t in trust)}</ul>
  </div>
  <div class="story-photo"><img src="{photo_url}" alt="{name}"></div>
</div></section>
<section class="info" id="visit">
  <div><h3>Contact</h3><p><a href="tel:{phone}">{phone}</a></p></div>
  {f'<div><h3>Find Us</h3><p>{address}</p></div>' if address else ''}
  <div><h3>Hours</h3><p>Mon&ndash;Sat 9:30am&ndash;7pm<br>Sun 11am&ndash;5pm</p></div>
</section>
<section class="cta">
  <div class="eyebrow">We Cannot Wait to See You</div>
  <h2>Book Your Visit Today</h2>
  <p>Call or text {name} and treat yourself. You deserve it.</p>
  <div style="display:flex;gap:14px;justify-content:center;flex-wrap:wrap;">
    <a href="tel:{phone}" class="btn btn-fill">Call {phone}</a>
    <a href="sms:{phone}" class="btn btn-line">Send a Text</a>
  </div>
</section>
{footer_html}
<div class="sticky-bar">
  <a href="tel:{phone}" class="sticky-call">Book Now</a>
  <a href="{claim_sms}" class="sticky-claim">Claim This Site Free</a>
</div>
</body></html>"""

    # ── Template 2: Pro performance — auto/trades (Chicago Auto Pros style) ───
    else:
        return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{name} | {cat_label} in {city or 'Your Area'}</title>
<link href="https://fonts.googleapis.com/css2?family=Oswald:wght@500;600;700&family=Barlow:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
html{{scroll-behavior:smooth;}}
body{{font-family:'Barlow',sans-serif;color:#23272d;background:#fff;min-height:100vh;padding-bottom:70px;}}
.preview-bar{{background:#0c0e11;color:#fff;text-align:center;padding:8px 16px;font-size:12px;font-weight:600;}}
.preview-bar a{{color:{accent};text-decoration:none;margin-left:6px;font-weight:700;}}
.topbar{{background:#16191e;color:#aab2bc;font-size:12.5px;}}
.topbar-inner{{max-width:1160px;margin:0 auto;padding:8px 20px;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;}}
.topbar a{{color:#fff;font-weight:700;text-decoration:none;}}
.topbar a span{{color:{accent};}}
nav{{background:#fff;box-shadow:0 1px 8px rgba(0,0,0,.1);position:sticky;top:0;z-index:100;}}
.nav-inner{{max-width:1160px;margin:0 auto;padding:0 20px;height:76px;display:flex;align-items:center;justify-content:space-between;}}
.nav-brand{{display:flex;align-items:center;gap:11px;}}
.nav-name{{font-family:'Oswald',sans-serif;font-weight:700;font-size:19px;letter-spacing:.04em;text-transform:uppercase;color:#16191e;}}
.nav-links{{display:flex;align-items:center;gap:26px;}}
.nav-links a{{font-size:13.5px;font-weight:600;letter-spacing:.04em;text-transform:uppercase;color:#3d434b;text-decoration:none;}}
.nav-links a:hover{{color:{accent};}}
.nav-call{{background:{accent};color:#fff !important;font-weight:700;padding:12px 24px;letter-spacing:.06em;}}
.hero{{position:relative;min-height:78vh;display:flex;align-items:center;}}
.hero-bg{{position:absolute;inset:0;background:url('{hero_url}') center/cover;}}
.hero-overlay{{position:absolute;inset:0;background:linear-gradient(90deg,rgba(12,14,17,.88) 0%,rgba(12,14,17,.55) 60%,rgba(12,14,17,.3) 100%);}}
.hero-inner{{position:relative;z-index:2;max-width:1160px;margin:0 auto;padding:90px 20px;width:100%;}}
.hero-kicker{{display:inline-block;background:{accent};color:#fff;font-family:'Oswald',sans-serif;font-size:12.5px;font-weight:600;letter-spacing:.18em;text-transform:uppercase;padding:7px 16px;margin-bottom:20px;}}
.hero h1{{font-family:'Oswald',sans-serif;font-weight:700;text-transform:uppercase;font-size:clamp(2.1rem,5.4vw,3.8rem);line-height:1.1;color:#fff;max-width:700px;margin-bottom:16px;letter-spacing:.01em;}}
.hero p{{color:rgba(255,255,255,.85);font-size:1.05rem;line-height:1.7;max-width:560px;margin-bottom:30px;}}
.btn{{display:inline-flex;align-items:center;justify-content:center;gap:8px;font-weight:700;font-size:14.5px;letter-spacing:.05em;text-transform:uppercase;padding:16px 30px;text-decoration:none;}}
.btn-accent{{background:{accent};color:#fff;}}
.btn-white{{background:#fff;color:#16191e;}}
.btn-line{{border:2px solid rgba(255,255,255,.6);color:#fff;}}
.trustline{{background:#16191e;}}
.trustline-inner{{max-width:1160px;margin:0 auto;padding:22px 20px;display:flex;justify-content:center;gap:40px;flex-wrap:wrap;}}
.trustline span{{color:#fff;font-size:13.5px;font-weight:600;display:flex;align-items:center;gap:9px;}}
.trustline b{{color:{accent};font-size:16px;}}
.section{{max-width:1160px;margin:0 auto;padding:84px 20px;}}
.sec-head{{margin-bottom:42px;}}
.sec-head .kicker{{color:{accent};font-family:'Oswald',sans-serif;font-weight:600;font-size:13px;letter-spacing:.18em;text-transform:uppercase;margin-bottom:8px;}}
.sec-head h2{{font-family:'Oswald',sans-serif;font-weight:700;text-transform:uppercase;font-size:clamp(1.7rem,3.6vw,2.5rem);color:#16191e;}}
.pro-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(330px,1fr));gap:20px;}}
.pro-card{{border:1px solid #e3e7ec;border-top:3px solid {accent};padding:30px 28px;background:#fff;transition:.2s;}}
.pro-card:hover{{box-shadow:0 12px 30px rgba(22,25,30,.1);}}
.pro-ico{{margin-bottom:14px;display:flex;}}
.pro-card h3{{font-family:'Oswald',sans-serif;font-weight:600;text-transform:uppercase;font-size:16.5px;letter-spacing:.04em;color:#16191e;margin-bottom:9px;}}
.pro-card p{{font-size:14px;color:#5b626c;line-height:1.7;margin-bottom:16px;}}
.pro-card a{{color:{accent};font-weight:700;font-size:13.5px;letter-spacing:.05em;text-transform:uppercase;text-decoration:none;}}
.why{{background:#16191e;}}
.why-inner{{max-width:1160px;margin:0 auto;padding:84px 20px;display:grid;grid-template-columns:1fr 1fr;gap:56px;align-items:center;}}
@media(max-width:820px){{.why-inner{{grid-template-columns:1fr;}}}}
.why-photo img{{width:100%;aspect-ratio:4/3.1;object-fit:cover;display:block;border:3px solid {accent};}}
.why-text .kicker{{color:{accent};font-family:'Oswald',sans-serif;font-weight:600;font-size:13px;letter-spacing:.18em;text-transform:uppercase;margin-bottom:10px;}}
.why-text h2{{font-family:'Oswald',sans-serif;font-weight:700;text-transform:uppercase;font-size:clamp(1.6rem,3.2vw,2.2rem);color:#fff;margin-bottom:16px;}}
.why-text p{{color:#aab2bc;line-height:1.8;margin-bottom:24px;}}
.why-text ul{{list-style:none;display:flex;flex-direction:column;gap:13px;}}
.why-text li{{display:flex;gap:12px;align-items:center;font-size:15px;font-weight:600;color:#fff;}}
.why-text li span{{width:24px;height:24px;background:{accent};color:#fff;display:flex;align-items:center;justify-content:center;font-size:12px;flex-shrink:0;}}
.contact{{max-width:1160px;margin:0 auto;padding:84px 20px;display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:18px;}}
.c-card{{border:1px solid #e3e7ec;padding:28px;}}
.c-card h3{{font-family:'Oswald',sans-serif;font-weight:600;text-transform:uppercase;font-size:13px;letter-spacing:.12em;color:#8a919b;margin-bottom:8px;}}
.c-card p{{font-size:15.5px;font-weight:600;color:#16191e;line-height:1.6;}}
.c-card a{{color:{accent};text-decoration:none;font-weight:700;}}
.cta{{background:{accent};}}
.cta-inner{{max-width:1160px;margin:0 auto;padding:64px 20px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:24px;}}
.cta h2{{font-family:'Oswald',sans-serif;font-weight:700;text-transform:uppercase;color:#fff;font-size:clamp(1.5rem,3.2vw,2.2rem);}}
.cta p{{color:rgba(255,255,255,.85);margin-top:6px;font-size:15px;}}
.sticky-bar{{position:fixed;bottom:0;left:0;right:0;background:#fff;border-top:1px solid #e3e7ec;box-shadow:0 -4px 18px rgba(0,0,0,.1);padding:10px 16px;display:flex;gap:10px;z-index:200;}}
.sticky-call{{flex:1;display:flex;align-items:center;justify-content:center;gap:8px;background:{accent};color:#fff;font-weight:700;font-size:13.5px;letter-spacing:.04em;text-transform:uppercase;padding:14px;text-decoration:none;}}
.sticky-claim{{flex:1;display:flex;align-items:center;justify-content:center;background:#16191e;color:#fff;font-weight:600;font-size:13px;letter-spacing:.03em;text-transform:uppercase;padding:14px;text-decoration:none;}}
@media(max-width:700px){{.nav-links a:not(.nav-call){{display:none;}} .topbar-inner{{justify-content:center;}}}}
</style></head><body>
<div class="preview-bar">This is a FREE preview website built for {name}.<a href="{claim_sms}">Text us to claim it</a></div>
<div class="topbar"><div class="topbar-inner">
  <span>Serving {city if city else 'the local area'} &amp; surrounding &nbsp;&bull;&nbsp; Mon&ndash;Sat 8am&ndash;6pm</span>
  <a href="tel:{phone}" style="display:inline-flex;align-items:center;gap:7px;">{phone_svg}{phone}</a>
</div></div>
<nav><div class="nav-inner">
  <div class="nav-brand">{logo_nav}<span class="nav-name">{name}</span></div>
  <div class="nav-links">
    <a href="#services">Services</a>
    <a href="#why">Why Us</a>
    <a href="#contact">Contact</a>
    <a href="tel:{phone}" class="nav-call">Free Estimate</a>
  </div>
</div></nav>
<section class="hero">
  <div class="hero-bg"></div><div class="hero-overlay"></div>
  <div class="hero-inner">
    <div class="hero-kicker">{trust[0]}</div>
    <h1>{city if city else 'Your'}-Based {cat_label} Experts</h1>
    <p>{sub}</p>
    <div style="display:flex;gap:12px;flex-wrap:wrap;">
      <a href="tel:{phone}" class="btn btn-accent">Call {phone}</a>
      <a href="sms:{phone}" class="btn btn-white">Free Estimate</a>
      <a href="sms:{phone}" class="btn btn-line">Text Us</a>
    </div>
  </div>
</section>
<div class="trustline"><div class="trustline-inner">
  {''.join(f'<span>{_icon("check", accent, 16)}{t}</span>' for t in trust)}
</div></div>
<section class="section" id="services">
  <div class="sec-head">
    <div class="kicker">What We Do</div>
    <h2>Our Services</h2>
  </div>
  <div class="pro-grid">{pro_cards}</div>
</section>
<section class="why" id="why"><div class="why-inner">
  <div class="why-text">
    <div class="kicker">Why {name}</div>
    <h2>Done Right the First Time</h2>
    <p>{name} is locally owned and operated in {city if city else 'your area'}. No call centers, no subcontractors, no runaround. You deal directly with the people doing the work.</p>
    <ul>{trust_checks}</ul>
  </div>
  <div class="why-photo"><img src="{photo_url}" alt="{name}"></div>
</div></section>
<section class="contact" id="contact">
  <div class="c-card"><h3>Phone</h3><p><a href="tel:{phone}">{phone}</a></p></div>
  {f'<div class="c-card"><h3>Address</h3><p>{address}</p></div>' if address else ''}
  <div class="c-card"><h3>Hours</h3><p>Mon&ndash;Sat 8am&ndash;6pm<br>Emergency? Just call.</p></div>
  <div class="c-card"><h3>Service Area</h3><p>{city if city else 'Local Area'} &amp; surrounding communities</p></div>
</section>
<section class="cta"><div class="cta-inner">
  <div><h2>Get Your Free Estimate</h2><p>Fast response. Upfront pricing. Quality guaranteed.</p></div>
  <div style="display:flex;gap:12px;flex-wrap:wrap;">
    <a href="tel:{phone}" class="btn btn-white">Call {phone}</a>
    <a href="sms:{phone}" class="btn btn-line">Text Us</a>
  </div>
</div></section>
{footer_html}
<div class="sticky-bar">
  <a href="tel:{phone}" class="sticky-call">Call Now</a>
  <a href="{claim_sms}" class="sticky-claim">Claim This Site Free</a>
</div>
</body></html>"""


# ── Preview helpers ───────────────────────────────────────────────────────────

def write_preview(lead: dict) -> str:
    """Generate the preview HTML file for a lead, save its path, return the path."""
    slug = re.sub(r"[^a-z0-9]+", "-", lead["name"].lower()).strip("-")
    filename = f"{slug}-{lead['id']}.html"
    with open(os.path.join(PREVIEWS_DIR, filename), "w") as f:
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


@app.post("/api/scrape/stop")
async def scrape_stop():
    global scraper_stop_requested
    scraper_stop_requested = True
    return {"ok": True}


@app.get("/api/leads")
async def get_leads(status: Optional[str] = None, search: Optional[str] = None,
                    page: int = 1, page_size: int = 50):
    conn = get_db()
    base = "FROM leads"
    params, conditions = [], []
    if status and status != "all":
        conditions.append("status = ?")
        params.append(status)
    if search:
        conditions.append("(name LIKE ? OR phone LIKE ? OR address LIKE ? OR category LIKE ?)")
        params += [f"%{search}%"] * 4
    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    total = conn.execute(f"SELECT COUNT(*) {base}{where}", params).fetchone()[0]
    offset = (page - 1) * page_size
    rows = [dict(r) for r in conn.execute(f"SELECT * {base}{where} ORDER BY created_at DESC LIMIT ? OFFSET ?", params + [page_size, offset]).fetchall()]
    conn.close()
    return {"leads": rows, "total": total, "page": page, "pages": max(1, -(-total // page_size))}


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


def send_twilio_sms(account_sid, auth_token, from_number, to, body) -> str:
    """Send SMS and return the Twilio message SID."""
    import base64, json as _json, urllib.parse, urllib.request
    cfg = load_config()
    base = (cfg.get("base_url") or "").rstrip("/")
    params = {"To": to, "From": from_number, "Body": body}
    if base:
        params["StatusCallback"] = f"{base}/api/webhooks/sms/status"
    data = urllib.parse.urlencode(params).encode()
    req  = urllib.request.Request(
        f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
        data=data,
        headers={"Authorization": "Basic " + base64.b64encode(f"{account_sid}:{auth_token}".encode()).decode()}
    )
    resp = urllib.request.urlopen(req, timeout=10)
    return _json.loads(resp.read()).get("sid", "")


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
        msg  = get_sequence()[0].replace("{name}", lead["name"]).replace("{preview_url}", absolute_url(ensure_preview(lead))).replace("{category}", lead.get("category") or "business").replace("{city}", city)
        try:
            await asyncio.sleep(1.1)
            sid = send_twilio_sms(cfg["twilio_account_sid"], cfg["twilio_auth_token"], cfg["twilio_from_number"], lead["phone"], msg)
            follow_up_at = (now + timedelta(days=SEQUENCE_DELAYS[1])).strftime("%Y-%m-%d %H:%M:%S")
            conn = get_db()
            conn.execute("UPDATE leads SET sequence_active=1, sequence_step=1, follow_up_at=?, status=CASE WHEN status='new' THEN 'contacted' ELSE status END, sms_sent=sms_sent+1, first_contacted_at=COALESCE(first_contacted_at, ?) WHERE id=?", (follow_up_at, now.strftime("%Y-%m-%d %H:%M:%S"), lead["id"]))
            conn.execute("INSERT INTO sms_log (lead_id, phone, message, status, twilio_sid, delivery_status) VALUES (?,?,?,?,?,?)", (lead["id"], lead["phone"], msg, "sent", sid, "queued"))
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
    due  = conn.execute("SELECT * FROM leads WHERE sequence_active=1 AND follow_up_at <= ? AND sequence_step < ? AND status NOT IN ('replied','closed','opted_out','has_website')", (now, len(get_sequence()))).fetchall()
    conn.close()

    sent = 0
    for lead in due:
        lead = dict(lead)
        step = lead["sequence_step"]
        seq = get_sequence()
        if step >= len(seq):
            conn = get_db()
            conn.execute("UPDATE leads SET sequence_active=0 WHERE id=?", (lead["id"],))
            conn.commit()
            conn.close()
            continue

        city    = (lead.get("location") or "your area").split(",")[0].split(" IL")[0].strip()
        preview = absolute_url(ensure_preview(lead))
        msg     = seq[step].replace("{name}", lead["name"]).replace("{preview_url}", preview).replace("{category}", lead.get("category") or "business").replace("{city}", city)

        try:
            await asyncio.sleep(1.1)
            sid = send_twilio_sms(cfg["twilio_account_sid"], cfg["twilio_auth_token"], cfg["twilio_from_number"], lead["phone"], msg)
            next_step = step + 1
            if next_step < len(seq):
                follow_up_at = (datetime.now() + timedelta(days=SEQUENCE_DELAYS[next_step] - SEQUENCE_DELAYS[step])).strftime("%Y-%m-%d %H:%M:%S")
                active = 1
            else:
                follow_up_at = None
                active = 0
            conn = get_db()
            conn.execute("UPDATE leads SET sequence_step=?, sequence_active=?, follow_up_at=?, sms_sent=sms_sent+1 WHERE id=?", (next_step, active, follow_up_at, lead["id"]))
            conn.execute("INSERT INTO sms_log (lead_id, phone, message, status, twilio_sid, delivery_status) VALUES (?,?,?,?,?,?)", (lead["id"], lead["phone"], msg, "sent", sid, "queued"))
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
        conn.execute("INSERT INTO sms_log (lead_id, phone, message, status, direction) VALUES (?,?,?,?,?)",
                     (matched["id"], from_, body, "opted_out", "inbound"))
        conn.commit()
        conn.close()
        cfg = load_config()
        if all([cfg.get("twilio_account_sid"), cfg.get("twilio_auth_token"), cfg.get("twilio_from_number")]):
            try:
                conf_msg = "You have been unsubscribed and will receive no further messages from us."
                send_twilio_sms(cfg["twilio_account_sid"], cfg["twilio_auth_token"],
                                cfg["twilio_from_number"], from_, conf_msg)
                conn = get_db()
                conn.execute("INSERT INTO sms_log (lead_id, phone, message, status, direction) VALUES (?,?,?,?,?)",
                             (matched["id"], from_, conf_msg, "sent", "outbound"))
                conn.commit()
                conn.close()
            except Exception:
                pass
        return HTMLResponse(content=TWIML_EMPTY, media_type="application/xml")

    intent = classify_reply(body, current_status=matched.get("status", ""))

    # Determine new status
    if intent == "claim":
        new_status = "claimed"
    elif intent == "has_website":
        new_status = "has_website"
    elif intent == "no_website":
        new_status = "building"
    else:
        new_status = "replied"

    conn = get_db()
    conn.execute("UPDATE leads SET status=?, sequence_active=0 WHERE id=?", (new_status, matched["id"]))
    conn.execute("INSERT INTO sms_log (lead_id, phone, message, status, direction) VALUES (?,?,?,?,?)",
                 (matched["id"], from_, body, "received", "inbound"))
    conn.commit()
    conn.close()

    cfg = load_config()
    has_twilio = all([cfg.get("twilio_account_sid"), cfg.get("twilio_auth_token"), cfg.get("twilio_from_number")])

    if intent == "no_website" and has_twilio:
        # Build preview and send the link
        try:
            preview_path = ensure_preview(matched)
            base = (cfg.get("base_url") or str(request.base_url)).rstrip("/")
            preview_link = shorten_url(f"{base}{preview_path}")

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
            conn.execute("INSERT INTO sms_log (lead_id, phone, message, status, direction) VALUES (?,?,?,?,?)",
                         (matched["id"], from_, reply_msg, "sent", "outbound"))
            conn.commit()
            conn.close()
        except Exception:
            pass

    elif intent == "claim" and has_twilio:
        try:
            reply_msg = "Awesome! Someone from our team will be reaching out shortly to get everything set up for you."
            send_twilio_sms(
                cfg["twilio_account_sid"], cfg["twilio_auth_token"],
                cfg["twilio_from_number"], from_, reply_msg
            )
            conn = get_db()
            conn.execute("INSERT INTO sms_log (lead_id, phone, message, status, direction) VALUES (?,?,?,?,?)",
                         (matched["id"], from_, reply_msg, "sent", "outbound"))
            conn.commit()
            conn.close()
        except Exception:
            pass

    # Notify Josh with intent context
    notify_number = cfg.get("notify_number")
    if notify_number and has_twilio:
        try:
            if intent == "claim":
                notify_msg = (
                    f"HOT LEAD - CALL NOW\n"
                    f"{matched['name']} wants to claim their site.\n"
                    f"Call: {from_}"
                )
            else:
                labels = {
                    "no_website": "NO website - preview sent",
                    "has_website": "HAS a website already",
                    "other": "replied",
                }
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


@app.post("/api/test-notify")
async def test_notify():
    cfg = load_config()
    account_sid  = cfg.get("twilio_account_sid")
    auth_token   = cfg.get("twilio_auth_token")
    from_number  = cfg.get("twilio_from_number")
    notify_number = cfg.get("notify_number")
    if not all([account_sid, auth_token, from_number, notify_number]):
        return JSONResponse({"ok": False, "error": f"Missing config. notify_number={notify_number}, from={from_number}"})
    try:
        sid = send_twilio_sms(account_sid, auth_token, from_number, notify_number, "TEST - Render notify is working")
        return {"ok": True, "sid": sid, "to": notify_number}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.post("/api/sync-twilio-replies")
async def sync_twilio_replies():
    """Pull inbound messages from Twilio API and backfill sms_log for any that are missing."""
    cfg = load_config()
    account_sid = cfg.get("twilio_account_sid")
    auth_token  = cfg.get("twilio_auth_token")
    from_number = cfg.get("twilio_from_number")
    if not all([account_sid, auth_token, from_number]):
        return JSONResponse({"ok": False, "error": "Twilio not configured"})

    import base64
    from email.utils import parsedate_to_datetime

    credentials = base64.b64encode(f"{account_sid}:{auth_token}".encode()).decode()
    url = (
        f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
        f"?To={urllib.parse.quote(from_number)}&PageSize=1000"
    )
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Basic {credentials}")
    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

    messages = data.get("messages", [])
    conn  = get_db()
    leads = {re.sub(r"\D", "", r["phone"] or ""): dict(r)
             for r in conn.execute("SELECT * FROM leads WHERE phone IS NOT NULL").fetchall()}

    imported = 0
    for msg in messages:
        if msg.get("direction") != "inbound":
            continue
        from_digits = re.sub(r"\D", "", msg.get("from", ""))
        if from_digits not in leads:
            continue
        lead = leads[from_digits]
        sid  = msg.get("sid", "")
        if sid and conn.execute("SELECT id FROM sms_log WHERE twilio_sid=?", (sid,)).fetchone():
            continue
        date_raw = msg.get("date_sent") or msg.get("date_created", "")
        try:
            sent_at = parsedate_to_datetime(date_raw).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            sent_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        body = msg.get("body", "")
        conn.execute(
            "INSERT INTO sms_log (lead_id, phone, message, status, direction, twilio_sid, sent_at) VALUES (?,?,?,?,?,?,?)",
            (lead["id"], msg.get("from", ""), body, "received", "inbound", sid, sent_at)
        )
        imported += 1

    conn.commit()
    conn.close()
    return {"ok": True, "imported": imported}


@app.post("/api/webhooks/sms/status")
async def sms_status_webhook(request: Request):
    form = await request.form()
    sid    = form.get("MessageSid", "")
    status = form.get("MessageStatus", "")  # queued, sent, delivered, undelivered, failed
    if sid and status:
        conn = get_db()
        conn.execute("UPDATE sms_log SET delivery_status=? WHERE twilio_sid=?", (status, sid))
        conn.commit()
        conn.close()
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


@app.get("/api/sequence/messages")
async def get_sequence_messages():
    return {"messages": get_sequence(), "defaults": _DEFAULT_SEQUENCE}


@app.post("/api/sequence/messages")
async def save_sequence_messages(request: Request):
    body = await request.json()
    messages = body.get("messages", [])
    if len(messages) != len(_DEFAULT_SEQUENCE):
        return JSONResponse({"ok": False, "error": f"Must provide exactly {len(_DEFAULT_SEQUENCE)} messages"})
    save_config({"sequence_messages": messages})
    return {"ok": True}


@app.post("/api/leads/{lead_id}/log-message")
async def log_manual_message(lead_id: int, request: Request):
    """Log a message you sent from your personal phone without going through Twilio."""
    body = await request.json()
    text = (body.get("message") or "").strip()
    direction = body.get("direction", "outbound")
    if not text:
        return JSONResponse({"ok": False, "error": "No message provided"})
    conn = get_db()
    lead = conn.execute("SELECT phone FROM leads WHERE id=?", (lead_id,)).fetchone()
    if not lead:
        conn.close()
        return JSONResponse({"ok": False, "error": "Lead not found"})
    conn.execute("INSERT INTO sms_log (lead_id, phone, message, status, direction) VALUES (?,?,?,?,?)",
                 (lead_id, lead["phone"], text, "manual", direction))
    conn.commit()
    conn.close()
    return {"ok": True}


@app.get("/api/leads/{lead_id}/messages")
async def get_lead_messages(lead_id: int):
    conn = get_db()
    rows = conn.execute(
        "SELECT direction, message, status, sent_at, delivery_status FROM sms_log WHERE lead_id=? ORDER BY sent_at ASC",
        (lead_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]



@app.get("/", response_class=HTMLResponse)
async def dashboard():
    with open("static/index.html") as f:
        return f.read()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=bool(os.environ.get("DEV")))
