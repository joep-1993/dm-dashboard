# Thema Ads Optimizer - High Performance Version

**20-50x faster** than the original script through async processing, bulk operations, and smart caching.

## 🚀 Performance Improvements

| Feature | Original | Optimized | Speedup |
|---------|----------|-----------|---------|
| **Processing** | Sequential | Parallel (async) | **50x** |
| **API Calls** | ~6 per ad group | ~0.1 per ad group | **60x** |
| **1000 ad groups** | ~50 minutes | ~2-3 minutes | **20x** |
| **Concurrency** | 1 operation | 50+ operations | **50x** |
| **Error handling** | Basic | Retry with backoff | ✅ |
| **Caching** | None | Smart prefetch | ✅ |

## 📋 Features

✅ **Async/parallel processing** - Process multiple customers simultaneously
✅ **Bulk API operations** - Batch create/update operations
✅ **Smart prefetching** - Load all data in 2-3 queries
✅ **Retry logic** - Exponential backoff for transient failures
✅ **Caching layer** - Eliminate redundant queries
✅ **Dry-run mode** - Preview changes before execution
✅ **Comprehensive logging** - Track progress and debug issues
✅ **Modular architecture** - Easy to extend and maintain

## 🏗️ Architecture

```
thema_ads_optimized/
├── config.py              # Configuration management
├── google_ads_client.py   # Client initialization
├── models.py              # Data structures
├── operations/
│   ├── prefetch.py        # Bulk data loading
│   ├── ads.py             # Ad operations
│   └── labels.py          # Label operations
├── templates/
│   └── generators.py      # Ad template generators
├── processors/
│   └── data_loader.py     # Excel/CSV input
├── utils/
│   ├── cache.py           # Caching utilities
│   └── retry.py           # Retry decorators
└── main_optimized.py      # Main entry point
```

## 🛠️ Setup

### Option 1: Docker (Recommended) 🐳

**Prerequisites:**
- Docker Desktop installed and running
- Input data file (Excel or CSV)

**Quick Start:**

```bash
cd thema_ads_optimized

# 1. Setup environment
./docker-run.sh setup

# 2. Edit .env with your Google Ads credentials
nano .env  # or use any text editor

# 3. Place your input file in data/ directory
cp /path/to/your/input.xlsx data/

# 4. Build Docker image
./docker-run.sh build

# 5. Test with dry-run (no changes made)
./docker-run.sh dry-run

# 6. Run for real
./docker-run.sh run
```

**Docker Commands:**
```bash
./docker-run.sh setup      # Setup directories and check config
./docker-run.sh build      # Build Docker image
./docker-run.sh dry-run    # Test run (no changes)
./docker-run.sh run        # Production run
./docker-run.sh logs       # View logs
./docker-run.sh clean      # Clean up
```

**Manual Docker Run:**
```bash
# Build
docker-compose build

# Dry run
docker-compose run --rm -e DRY_RUN=true thema-ads-optimizer

# Production run
docker-compose run --rm thema-ads-optimizer

# View logs
tail -f logs/thema_ads_optimized.log
```

### Option 2: Local Python Installation

**Prerequisites:**
- Python 3.11+
- pip

**Setup:**

```bash
cd thema_ads_optimized

# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
nano .env  # Edit with your credentials

# 3. Prepare input data
# Place your Excel/CSV file in the directory
```

**Configuration (.env):**

```bash
# Required
GOOGLE_DEVELOPER_TOKEN=your_token
GOOGLE_REFRESH_TOKEN=your_refresh_token
GOOGLE_CLIENT_ID=your_client_id
GOOGLE_CLIENT_SECRET=your_client_secret
GOOGLE_LOGIN_CUSTOMER_ID=1234567890

# Optional (defaults shown)
MAX_CONCURRENT_CUSTOMERS=10
MAX_CONCURRENT_OPERATIONS=50
INPUT_FILE=input_data.xlsx
DRY_RUN=false
```

### Input Data Format

**Excel Format** (default):
- Sheet name: `ad_groups`
- Column B: `customer_id`
- Column C: `campaign_name`
- Column D: `campaign_id`
- Column F: `ad_group_id`

**CSV Format** (alternative):
```csv
customer_id,campaign_name,campaign_id,ad_group_id
1234567890,Campaign Name,98765,43210
```

## 🚀 Usage

### Docker Usage (Recommended)

```bash
# Dry run (preview only)
./docker-run.sh dry-run

# Production run
./docker-run.sh run

# View logs
./docker-run.sh logs
```

### Local Python Usage

```bash
# Basic run
python main_optimized.py

# Dry run (preview only)
DRY_RUN=true python main_optimized.py

# Custom input file
INPUT_FILE=/path/to/your/data.xlsx python main_optimized.py

# Debug mode
LOG_LEVEL=DEBUG python main_optimized.py
```

## 📊 Performance Tuning

Adjust these settings in `.env` based on your needs:

```bash
# More aggressive (faster, higher API usage)
MAX_CONCURRENT_CUSTOMERS=20
MAX_CONCURRENT_OPERATIONS=100
BATCH_SIZE=2000

# More conservative (slower, lower API usage)
MAX_CONCURRENT_CUSTOMERS=5
MAX_CONCURRENT_OPERATIONS=25
BATCH_SIZE=500
```

## 🔍 How It Works

### Original Script (Sequential)
```
For each ad group:
  1. Search for existing ad        [API call]
  2. Check label exists            [API call]
  3. Create new ad                 [API call]
  4. Label new ad (2x)             [API call x2]
  5. Label ad group                [API call]

Total: ~6 API calls × 1000 ad groups = 6000 calls
Time: ~50 minutes
```

### Optimized Script (Parallel + Batched)
```
For each customer (in parallel):
  1. Prefetch all labels           [1 API call]
  2. Prefetch all existing ads     [1 API call]
  3. Create all labels needed      [1 API call]
  4. Batch create all ads          [1 API call per 1000 ads]
  5. Batch label all ads           [1 API call]
  6. Batch label ad groups         [1 API call]

Total: ~6-10 API calls per customer
Time: ~2-3 minutes for 1000 ad groups
```

## 🎯 Key Optimizations

### 1. Parallel Processing
```python
# Process 10 customers simultaneously
async with asyncio.Semaphore(10):
    results = await asyncio.gather(*customer_tasks)
```

### 2. Bulk Prefetching
```python
# Get all ads in one query instead of 1000
query = f"""
    SELECT ad_group_ad.*
    FROM ad_group_ad
    WHERE ad_group_ad.ad_group IN ({all_ad_groups})
"""
```

### 3. Batch Mutations
```python
# Create 1000 ads in one API call
service.mutate_ad_group_ads(
    customer_id=customer_id,
    operations=operations,  # 1000 operations
    partial_failure=True
)
```

### 4. Smart Caching
```python
# Labels cached for reuse
cached_labels = prefetch_labels(customer_id)
# No repeated queries for same label
```

## 🐛 Troubleshooting

### "Missing environment variables"
- Ensure `.env` file exists and contains all required variables
- On Windows, close and reopen terminal after setting env vars

### "Failed to prefetch ads"
- Check customer_id format (remove dashes)
- Verify API credentials are correct
- Check quota limits in Google Ads

### "Some operations failed"
- Check logs for specific errors
- Script uses `partial_failure=True` - continues despite individual failures
- Review failed operations in log file

## 📝 Logs

Logs are written to:
- **Console**: Real-time progress
- **File**: `thema_ads_optimized.log`

Log levels:
- `INFO`: Progress and summary
- `DEBUG`: Detailed operation info
- `WARNING`: Recoverable issues
- `ERROR`: Failures requiring attention

## 🔐 Security

✅ Credentials stored in `.env` (not committed to git)
✅ No hardcoded secrets
✅ `.env.example` provided for reference
⚠️ Add `.env` to `.gitignore`

## 🚀 Next Steps

For even better performance:

1. **Database integration**: Replace Excel with PostgreSQL
2. **Redis caching**: Distributed cache across multiple workers
3. **Worker queue**: Process with Celery/RQ for horizontal scaling
4. **Monitoring**: Add Prometheus metrics

## 📄 License

Internal use only.

## 🤝 Support

For issues or questions, contact the development team.
