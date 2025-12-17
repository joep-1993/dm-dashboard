# 🚀 Quick Start Guide

## The Frontend is Running!

Your Thema Ads system is now integrated into Docker and ready to use.

### 📱 Access the Application

Open your browser and go to:

- **Thema Ads Interface**: http://localhost:8001/static/thema-ads.html
- **Main Dashboard**: http://localhost:8001/static/index.html
- **API Documentation**: http://localhost:8001/docs

### ⚙️ How to Start/Stop

**Start everything:**
```bash
docker-compose up -d
```

**Stop everything:**
```bash
docker-compose down
```

**View logs:**
```bash
docker-compose logs -f app
```

**Restart after code changes:**
```bash
docker-compose restart app
```

### 📊 Using the Thema Ads Interface

1. **Visit**: http://localhost:8001/static/thema-ads.html

2. **Upload CSV**:
   - Click "Choose File"
   - Select a CSV with `customer_id` and `ad_group_id` columns
   - Click "Upload & Create Job"
   - Example CSV is provided: `sample_input.csv`

3. **Start Processing**:
   - Click "Start" button
   - Watch real-time progress updates
   - Monitor success/failure counts

4. **Resume After Crash**:
   - If processing stops, simply click "Resume"
   - All progress is saved to the database
   - Continues exactly where it left off

### 🔧 System Status

Check if everything is running:
```bash
docker-compose ps
```

You should see:
- `test2-app-1` - Running on port 8001
- `test2-db-1` - PostgreSQL on port 5433

### 📁 File Structure

```
/home/jschagen/test2/
├── backend/               # FastAPI backend
│   ├── main.py           # API routes
│   ├── thema_ads_service.py  # Job management
│   └── database.py       # Database setup
├── frontend/             # Web interface
│   ├── thema-ads.html   # Thema Ads UI
│   └── js/thema-ads.js  # Frontend logic
├── thema_ads_project/    # Google Ads engine (mounted)
└── docker-compose.yml    # Docker configuration
```

### ⚡ Features

- ✅ **CSV Upload** - Replace input data via web interface
- ✅ **Real-time Progress** - Live updates every 2 seconds
- ✅ **Auto-resume** - Continue after crashes
- ✅ **State Persistence** - All progress saved
- ✅ **Batch Processing** - Parallel processing of customers
- ✅ **Error Tracking** - See recent failures

### 🛠️ Troubleshooting

**Port already in use?**
```bash
sudo lsof -i :8001
# Or edit docker-compose.yml to use different port
```

**Database issues?**
```bash
docker-compose restart db
docker-compose exec -T app python backend/database.py
```

**Check app logs:**
```bash
docker-compose logs app
```

**Rebuild after dependency changes:**
```bash
docker-compose build
docker-compose up -d
```

### 📖 More Information

See `THEMA_ADS_GUIDE.md` for complete documentation.

---

**Everything is ready! Just open your browser to http://localhost:8001/static/thema-ads.html** 🎉
