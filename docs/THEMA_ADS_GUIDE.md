# Thema Ads Processing - Quick Start Guide

## Overview

Web-based interface for processing Google Ads themed campaigns with:
- ✅ **CSV Upload** - Upload ad group data via web interface
- ✅ **Progress Tracking** - Real-time monitoring with live updates
- ✅ **Resume Capability** - Automatically resume after crashes
- ✅ **State Persistence** - All progress saved to PostgreSQL

## Quick Start

### 1. Start the System

```bash
./start-thema-ads.sh
```

This will:
- Build Docker containers
- Start FastAPI backend + PostgreSQL
- Initialize database tables
- Mount all code for live editing

### 2. Access the Interface

Open your browser:
- **Thema Ads UI**: http://localhost:8001/static/thema-ads.html
- **API Docs**: http://localhost:8001/docs

### 3. Process Your Data

1. **Prepare CSV File**
   - Must have columns: `customer_id`, `ad_group_id`
   - Example:
     ```csv
     customer_id,ad_group_id
     1234567890,98765432
     1234567890,98765433
     ```

2. **Upload & Start**
   - Click "Choose File" and select your CSV
   - Click "Upload & Create Job"
   - Click "Start" to begin processing

3. **Monitor Progress**
   - Real-time progress bar
   - Success/failure counts
   - Recent error messages
   - Auto-updates every 2 seconds

4. **Resume After Crash**
   - If processing stops, simply click "Resume"
   - Continues from exactly where it left off
   - No data loss!

## How It Works

### Architecture

```
┌─────────────┐
│  Frontend   │  (Bootstrap + JavaScript)
│  thema-ads  │  - CSV upload
│  .html      │  - Progress display
└──────┬──────┘  - Start/Pause/Resume
       │
       ↓ REST API
┌──────────────┐
│   FastAPI    │
│   Backend    │  - Job management
└──────┬───────┘  - State persistence
       │
       ↓
┌──────────────┐
│  PostgreSQL  │  - Job tracking
│   Database   │  - Resume state
└──────┬───────┘
       │
       ↓
┌──────────────────┐
│ Thema Ads Engine │  - Google Ads API
│  (Optimized)     │  - Parallel processing
└──────────────────┘
```

### Database Tables

- **thema_ads_jobs** - Job metadata and statistics
- **thema_ads_job_items** - Individual ad group status
- **thema_ads_input_data** - Original CSV data

## API Endpoints

All endpoints available at http://localhost:8001/docs

### Upload CSV
```bash
POST /api/thema-ads/upload
Content-Type: multipart/form-data

Returns: { job_id, total_items, status }
```

### Start Job
```bash
POST /api/thema-ads/jobs/{job_id}/start
Returns: { status: "started" }
```

### Pause Job
```bash
POST /api/thema-ads/jobs/{job_id}/pause
Returns: { status: "paused" }
```

### Resume Job
```bash
POST /api/thema-ads/jobs/{job_id}/resume
Returns: { status: "resumed" }
```

### Get Job Status
```bash
GET /api/thema-ads/jobs/{job_id}
Returns: { job details, progress, failures }
```

### List All Jobs
```bash
GET /api/thema-ads/jobs?limit=20
Returns: { jobs: [...] }
```

## Configuration

Google Ads API credentials are read from:
```
/thema_ads_optimized/.env
```

Required variables:
- `GOOGLE_DEVELOPER_TOKEN`
- `GOOGLE_REFRESH_TOKEN`
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_LOGIN_CUSTOMER_ID`

## Development

### File Structure
```
/home/jschagen/test2/
├── backend/
│   ├── main.py                 # FastAPI routes
│   ├── thema_ads_service.py    # Job management
│   └── database.py             # DB initialization
├── frontend/
│   ├── thema-ads.html          # UI
│   └── js/thema-ads.js         # Frontend logic
├── thema_ads_project/
│   └── thema_ads_optimized/    # Google Ads engine
└── docker-compose.yml
```

### Live Editing

All code is mounted as volumes in Docker:
- Edit files locally → Changes reflect immediately
- FastAPI auto-reloads on changes
- No rebuild needed!

### View Logs
```bash
docker-compose logs -f app
```

### Stop System
```bash
docker-compose down
```

### Rebuild After Dependencies Change
```bash
docker-compose build
docker-compose up -d
```

## Troubleshooting

### Port Already in Use
```bash
# Check what's using port 8001
sudo lsof -i :8001

# Or change port in docker-compose.yml
ports:
  - "8002:8000"  # Use 8002 instead
```

### Database Connection Issues
```bash
# Restart containers
docker-compose restart

# Or reset database
docker-compose down -v
./start-thema-ads.sh
```

### Job Stuck in "Running"
If a job shows as "running" but isn't progressing:
1. Check logs: `docker-compose logs -f app`
2. Pause the job in the UI
3. Resume to restart processing

### CSV Upload Fails
Ensure CSV has correct columns:
- Must have: `customer_id`, `ad_group_id`
- UTF-8 encoding
- No special characters in column names

## Performance

- **Parallel Processing**: 10 customers simultaneously
- **Batch Operations**: Up to 1000 items per API call
- **Auto-resume**: Zero data loss on crash
- **Progress Updates**: Every 2 seconds

## Security Notes

- `.env` files contain sensitive credentials
- Never commit `.env` to git
- Restrict access to port 8001 in production
- Use proper authentication for production deployment
