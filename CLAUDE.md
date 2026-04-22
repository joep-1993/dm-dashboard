# CLAUDE.md

This project is **Content Top** - an SEO content generation system.

## Tech Stack
- **Backend**: FastAPI with auto-reload
- **Frontend**: Static files with Bootstrap CDN (no build tools)
- **Database**: PostgreSQL (local) + AWS Redshift (persistent storage)
- **AI**: OpenAI API for content generation
- **Timezone**: Europe/Amsterdam (CET/CEST)

## Development Workflow
1. Start the backend: `uvicorn backend.main:app --reload --port 8003`
2. Edit files directly - they auto-reload
3. Access frontend at http://localhost:8003/static/index.html (port 8003, not 8001)

## Important Notes
- **No Build Tools**: Edit HTML/CSS/JS directly
- **Simple Scale**: Designed for small teams (1-10 users)
- **Hybrid Database**: Local PostgreSQL for tracking, Redshift for content storage

## File Locations
- API: `backend/main.py`
- AI Service: `backend/gpt_service.py`
- Scraper: `backend/scraper_service.py`
- Database: `backend/database.py`
- Frontend: `frontend/index.html`
- App Logic: `frontend/js/app.js`

## Scraper Configuration
- **User Agent**: `"Beslist script voor SEO"` (set in `backend/scraper_service.py`)
- **Purpose**: Custom identifier for Beslist scraping operations
- **Location**: `USER_AGENT` constant at top of `scraper_service.py`
- **HTML Structure**: Extracts product URLs from `<a class="productLink--zqrcp">` elements (updated 2025-01-22)
- **503 Detection**: Uses specific patterns (`'service unavailable'`, `'503 service'`, `'error 503'`) to avoid false positives from URLs containing "503"

## External API Configuration

### Google Ads API
- **Credentials Location**: `.env` file (see `.env.example` for template)
- **Required Variables**:
  - `GOOGLE_DEVELOPER_TOKEN` - Developer token for API access
  - `GOOGLE_REFRESH_TOKEN` - OAuth2 refresh token
  - `GOOGLE_CLIENT_ID` - OAuth2 client ID
  - `GOOGLE_CLIENT_SECRET` - OAuth2 client secret
  - `GOOGLE_LOGIN_CUSTOMER_ID` - Customer ID for account access
- **Documentation**: https://developers.google.com/google-ads/api/docs/oauth/cloud-project
- **Access**: Environment variables loaded from `.env` via `python-dotenv`

## What It Does
Processes URLs from database, scrapes product information, generates AI-powered SEO content, and saves results.

---
_Project: Content Top | SEO Content Generation_
