# No-Website Lead Machine

Finds local businesses with no website on Google Maps, generates a free preview website for each one, and runs an automated SMS outreach sequence to sell it to them. FastAPI + SQLite + Playwright + Twilio.

## How it works

1. **Scrape**: Playwright searches Google Maps by category + city, keeps only businesses with a phone number and no website.
2. **Preview**: each lead gets an auto-generated single-page website (3 design templates, 17 industry content packs) served at `/previews/<slug>.html`.
3. **Outreach**: a 4-step SMS drip over 5 days. Follow-ups send automatically every 30 minutes via a background scheduler.
4. **Replies**: a Twilio webhook classifies inbound texts. "No we don't have a site" triggers an instant auto-reply with their preview link. Every reply also notifies your cell. STOP replies are marked opted out and never contacted again.
5. **Track**: dashboard with lead statuses, deal/revenue tracking, CSV export.

## Run locally

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
python app.py
# open http://localhost:8000
```

## Configure

Copy `config.example.json` to `config.json` or use the Settings tab in the dashboard:

| Key | What it is |
|---|---|
| `twilio_account_sid` | From the Twilio console |
| `twilio_auth_token` | From the Twilio console |
| `twilio_from_number` | Your Twilio number, E.164 format |
| `notify_number` | Your cell, gets a text when leads reply |
| `base_url` | Public URL of this app, used in preview links + webhook validation |

Then in the Twilio console, set your number's **SMS webhook** to:
`{base_url}/api/webhooks/sms/reply`

## Deploy

The included Dockerfile runs anywhere that supports Docker (Railway, Fly.io, Render, a VPS). Note: SQLite and generated previews live on disk, so the host needs a **persistent volume** (Railway volumes, Fly volumes, Render disks). Vercel/serverless will not work.

For personal use you can also run it locally and expose it with a tunnel:

```bash
cloudflared tunnel --url http://localhost:8000
```

Then paste the tunnel URL into Settings as the base URL.

## Compliance notes

- Cold SMS in the US falls under TCPA. Use a verified toll-free number or registered A2P 10DLC campaign or carriers will silently filter your messages.
- STOP/UNSUBSCRIBE replies are honored automatically.
