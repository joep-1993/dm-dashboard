# DM Dashboard

SEO tools dashboard for Beslist.nl — content generation, keyword planning, link validation, and more.

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/joep-1993/dm-dashboard.git
cd dm-dashboard

# 2. Run setup (creates venv, installs deps, initializes DB)
chmod +x setup.sh
./setup.sh

# 3. Edit .env with your credentials
nano .env

# 4. Start the server
source venv/bin/activate
uvicorn backend.main:app --host 0.0.0.0 --port 8003 --reload
```

Open **http://localhost:8003** in your browser.

## Requirements

- Python 3.11+
- PostgreSQL database (local or remote)

## Configuration

Copy `.env.example` to `.env` and fill in:

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `OPENAI_API_KEY` | Yes | OpenAI API key for content generation |
| `AI_MODEL` | No | Model to use (default: `gpt-4o-mini`) |
| `GOOGLE_*` | No | Google Ads API credentials (for keyword planner) |
| `REDSHIFT_*` | No | Redshift credentials (for data export) |

## Tools Included

- **Kopteksten** — SEO content generation for category pages
- **FAQ Generator** — FAQ content with Schema.org markup
- **Keyword Planner** — Google Ads search volume lookup
- **Link Validator** — Check for broken links in generated content
- **IndexNow** — Submit URLs to search engines
- **301 Generator** — Redirect mapping tool
- **Thema Ads** — Google Ads thematic campaign management
- **GSD Campaigns** — Google Shopping campaign tools
- **DMA Bidding** — Designated Market Area bid management
- And more...

## Project Structure

```
dm-dashboard/
├── backend/
│   ├── main.py              # FastAPI app entry point
│   ├── database.py          # DB connection & schema init
│   ├── gpt_service.py       # OpenAI content generation
│   ├── scraper_service.py   # Product data scraping
│   ├── *_service.py         # Feature-specific services
│   └── *_router.py          # API route modules
├── frontend/
│   ├── dashboard.html       # Main dashboard
│   ├── index.html           # Kopteksten tool
│   ├── css/style.css
│   └── js/app.js
├── requirements.txt
├── setup.sh
└── .env.example
```
