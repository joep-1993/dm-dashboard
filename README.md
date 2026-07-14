# SEO Content Generation Workflow

FastAPI + PostgreSQL + Docker application that recreates n8n workflow for automated SEO content generation using OpenAI.

## 🚀 Quick Start

### 1. Environment Setup

Create a `.env` file:
```bash
OPENAI_API_KEY=your_openai_api_key_here
AI_MODEL=gpt-4o-mini
```

### 2. Start Docker Containers

```bash
docker-compose up --build
```

Services start on:
- **Backend API**: http://localhost:8001
- **PostgreSQL**: localhost:5433

### 3. Initialize Database

Open a new terminal:
```bash
docker-compose exec app python -m backend.database
```

Creates schema `pa` with tables:
- `jvs_seo_werkvoorraad` (work queue)
- `jvs_seo_werkvoorraad_kopteksten_check` (tracking)
- `content_urls_joep` (output)

### 4. Add URLs to Process

Connect to database:
```bash
docker-compose exec db psql -U postgres -d myapp
```

Insert URLs:
```sql
INSERT INTO pa.jvs_seo_werkvoorraad (url) VALUES
('https://www.beslist.nl/products/laptops'),
('https://www.beslist.nl/products/smartphones');
```

Exit: `\q`

### 5. Process URLs

**Option A: Web Interface**

Open http://localhost:8001/static/index.html and click **"Process URLs"**

**Option B: API**
```bash
curl -X POST http://localhost:8001/api/process-urls
```

## 📊 How It Works

1. **Fetch**: Gets 2 unprocessed URLs from queue
2. **Scrape**: Downloads HTML (User-Agent: `n8n-bot-jvs`)
3. **Extract**: Parses product titles, descriptions, URLs
4. **Generate**: Creates AI product recommendations via OpenAI
5. **Validate**: Checks for valid product links
6. **Save**: Stores results in database

## 🔍 View Results

Check processed content:
```bash
docker-compose exec db psql -U postgres -d myapp -c \
  "SELECT url, LEFT(content, 100) FROM pa.content_urls_joep ORDER BY created_at DESC LIMIT 5;"
```

Check status:
```bash
curl http://localhost:8001/api/status
```

## 🛠️ Development

- **Backend**: Edit `backend/*.py` - auto-reloads
- **Frontend**: Edit `frontend/*` - refresh browser
- **No build tools**: Just save and refresh!

## 📦 Tech Stack

- **Backend**: FastAPI (Python 3.11)
- **Frontend**: Bootstrap 5 + Vanilla JS
- **Database**: PostgreSQL 15
- **AI**: OpenAI API
- **Scraping**: BeautifulSoup4 + Requests
- **Deploy**: Docker + docker-compose

## 🔧 Common Commands

```bash
# Start everything
docker-compose up --build

# View logs
docker-compose logs -f app

# Restart
docker-compose restart app

# Stop everything
docker-compose down

# Reset database (DELETES ALL DATA)
docker-compose down -v
docker-compose up --build
docker-compose exec app python -m backend.database

# Access database
docker-compose exec db psql -U postgres -d myapp

# Check container status
docker-compose ps
```

## 📋 API Endpoints

- `POST /api/process-urls` - Process 2 URLs from queue
- `GET /api/status` - Get processing statistics
- `GET /api/health` - Health check

## ⚙️ Configuration

- **Batch Size**: 2 URLs per request
- **User-Agent**: `n8n-bot-jvs`
- **Max Products**: 70 per page
- **AI Model**: Configurable via `AI_MODEL` env var

## 📁 Key Files

```
backend/
  ├── main.py            # API endpoints
  ├── database.py        # Database schema
  ├── scraper_service.py # Web scraping logic
  └── gpt_service.py     # AI content generation

frontend/
  ├── index.html         # Dashboard UI
  └── js/app.js          # Frontend logic
```

## Windows Service Deployment (win-htz-006)

The dashboard runs as a Windows service via [NSSM](https://nssm.cc/) on `win-htz-006.colo.beslist.net`.

### Access

- **Dashboard**: https://win-htz-006.colo.beslist.net:3003/static/dashboard.html
- **Logs**: `logs\service.log`

### Service Management

```batch
:: Start / stop / restart
net start dm-dashboard
net stop dm-dashboard
net stop dm-dashboard && net start dm-dashboard
```

### Updating After Code Changes

Run the update script on the desktop:

```batch
C:\Users\l.davidowski\Desktop\update-dashboard.bat
```

This script pulls the latest code, installs any missing Python dependencies from `requirements.txt`, and restarts the service.

### Dependencies

Python dependencies are managed via `requirements.txt` and installed in the local venv (`venv\`). Key notes:

- **`setuptools<81`** is pinned because the vendored SA360 SDK uses `pkg_resources`, which was removed in setuptools 81+.
- **`./backend/vendor/searchads360-py.tar.gz`** is a vendored package — it must exist in the repo for `pip install -r requirements.txt` to succeed.

If the service fails to start, check `logs\service.log` for `ModuleNotFoundError` — this usually means a new dependency was added but not yet installed. Run the update script or manually:

```batch
venv\Scripts\pip install -r requirements.txt
net stop dm-dashboard && net start dm-dashboard
```

## 🐛 Troubleshooting

**Service won't start / port 3003 not listening:**
- Check `logs\service.log` for import errors
- Run: `venv\Scripts\pip install -r requirements.txt`
- Restart: `net stop dm-dashboard && net start dm-dashboard`

**Database connection issues:**
```bash
docker-compose down
docker-compose up --build
```

**Can't connect to OpenAI:**
- Check `.env` file has valid `OPENAI_API_KEY`
- Restart: `docker-compose restart app`

**No URLs to process:**
- Add URLs to `pa.jvs_seo_werkvoorraad` table (see step 4)

**View detailed logs:**
```bash
docker-compose logs -f app
```
