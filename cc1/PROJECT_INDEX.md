# PROJECT INDEX
_Project structure and technical specs. Update when: creating files, adding dependencies, defining schemas._

## Stack
Backend: FastAPI (Python 3.11, ThreadPoolExecutor for parallel processing, psycopg2 connection pooling 2-20 conns) | Frontend: Bootstrap 5 + Vanilla JS | Database: PostgreSQL 15 (seo_tools_db - primary for all data) + N8N Vector DB (copy for n8n workflows) + AWS Redshift (legacy, disabled) | AI: OpenAI API | Deploy: Docker + docker-compose | Google Ads: AsyncIO + Batch API (v28)

## Directory Structure
```
dm-tools/                    # DM Tools - Digital Marketing Tools Platform (Port 8003)
├── .claude/              # Claude Code configuration
├── backend/              # FastAPI app + all services
│   ├── main.py           # FastAPI app (~3250 lines, 58+ API endpoints)
│   ├── database.py       # Database connections (PostgreSQL primary, Redshift legacy)
│   ├── gpt_service.py    # OpenAI API integration
│   ├── scraper_service.py    # Product Search API + web scraping
│   ├── link_validator.py     # Elasticsearch plpUrl link validation
│   ├── faq_service.py        # FAQ generation service
│   ├── content_publisher.py  # Publishes content to website-configuration API
│   ├── ai_titles_service.py  # AI-powered title generation
│   ├── canonical_service.py  # Canonical URL transformation
│   ├── redirect_301_service.py # 301 redirect management
│   ├── rfinder_service.py    # /r/ URL discovery from Redshift
│   ├── seo_content_generator.py # SEO content from Product Search API
│   ├── keyword_planner_service.py # Keyword Planner: Google Ads search volume lookup
│   ├── category_keyword_service.py # Category Keyword Volumes: keyword+category combinations + facet volume processing
│   ├── run_facet_volumes.py    # Batch facet volume processing script (all maincats, resume-capable)
│   ├── category_forms.json     # Pre-computed Dutch singular/plural forms (3,564 entries)
│   ├── categories.xlsx         # Preloaded category data (3,543 rows: maincat/deepest_cat)
│   ├── unique_titles.py      # Unique title generation
│   ├── thema_ads_router.py   # Thema Ads APIRouter
│   ├── thema_ads_service.py  # Thema Ads business logic (150KB)
│   ├── thema_ads_db.py       # Thema Ads database layer
│   ├── maincat_mapping.csv   # Category ID mapping (used by link_validator + seo_content_generator)
│   ├── import_content.py     # Utility: CSV content import
│   ├── sync_werkvoorraad.py  # Utility: Sync werkvoorraad with content
│   ├── sync_redshift_flags.py # Utility: Sync Redshift flags (legacy)
│   └── deduplicate_content.py # Utility: Remove duplicate URLs
├── frontend/
│   ├── dashboard.html    # Entry point - tool overview
│   ├── index.html        # SEO Content Generation (kopteksten)
│   ├── faq.html          # FAQ Generation
│   ├── canonical.html    # Canonical URL Generator
│   ├── rfinder.html      # R-Finder (URL Discovery)
│   ├── redirect-checker.html # Redirect Checker
│   ├── 301-generator.html    # 301 Generator
│   ├── thema-ads.html    # Thema Ads Processing
│   ├── keyword-planner.html # Keyword Planner (search volumes + category volumes)
│   ├── unique-titles.html # Unique Titles Manager
│   ├── css/style.css     # Custom theme (#059CDF blue, #9C3095 purple, #A0D168 green)
│   └── js/
│       ├── app.js        # SEO content frontend logic
│       ├── faq.js        # FAQ frontend logic
│       └── thema-ads.js  # Thema Ads frontend logic
├── cc1/                  # Claude Code documentation
│   ├── TASKS.md          # Task tracking
│   ├── LEARNINGS.md      # Knowledge capture + DB connection reference
│   ├── BACKLOG.md        # Future planning
│   └── PROJECT_INDEX.md  # This file
├── data/                 # Data files
│   └── sample_input.csv  # Example CSV for Thema Ads
├── docs/                 # Documentation + reference files
│   ├── ARCHITECTURE.md   # System architecture
│   ├── PROXY_SETUP.md    # VPN/proxy configuration
│   ├── START_HERE.md     # Quick start guide
│   ├── THEMA_ADS_GUIDE.md # Thema Ads documentation
│   ├── 301-generator_script.js # Google Sheets reference script
│   └── kopteksten_uitrol (1).json # n8n workflow reference
├── logs/                 # Log output (cleared regularly)
├── scripts/              # Standalone CLI tools + utilities
│   ├── redirect_checker.py   # HTTP redirect/canonical checker
│   ├── setup.sh              # Project setup script
│   ├── start-thema-ads.sh    # Thema ads startup
│   ├── csv_utils/            # CSV manipulation scripts
│   └── testing/              # Rate limit test scripts
├── themes/               # Thema Ads templates (black_friday, cyber_monday, etc.)
├── thema_ads_optimized/  # Docker volume mount target (external)
├── docker-compose.yml    # Services: seo_tools_db (PostgreSQL) + dm_tools_app (FastAPI)
├── Dockerfile            # Python 3.11-slim container
├── requirements.txt      # Python dependencies
├── CLAUDE.md             # Claude Code instructions
├── README.md             # Quick start guide
├── .env / .env.example   # Environment configuration
└── .gitignore
```

## Network Configuration

### VPN Bypass for Whitelisted IP
The scraper requires IP 87.212.193.148 (whitelisted by beslist.nl) but company VPN routes through 94.142.210.226. Solution: Windows static route with lower metric to bypass VPN for CloudFront IPs (65.9.0.0/16).

**Setup (one-time, as Administrator):**
```cmd
# Find your gateway: route print 0.0.0.0
# Add persistent route (replace 192.168.1.1 with your gateway, 10 with your Wi-Fi interface)
route delete 65.9.0.0
route add -p 65.9.0.0 mask 255.255.0.0 192.168.1.1 metric 1 if 10
```

**Result**: VPN stays connected (for Redshift access), but beslist.nl traffic uses whitelisted IP. Route persists across reboots. See LEARNINGS.md for detailed explanation and troubleshooting.

## Environment Variables

### Required (FastAPI/OpenAI)
```bash
OPENAI_API_KEY=sk-...  # Your OpenAI API key
DATABASE_URL=postgresql://postgres:postgres@db:5432/myapp
AI_MODEL=gpt-4o-mini  # Or other OpenAI model (max_tokens: 1000 for content with HTML links)
```

### Optional (Redshift - Legacy, disabled by default)
```bash
USE_REDSHIFT_OUTPUT=false  # Disabled - all data in local PostgreSQL
REDSHIFT_HOST=production-redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DB=database_name
REDSHIFT_USER=username
REDSHIFT_PASSWORD=password
REDSHIFT_OUTPUT_SCHEMA=pa  # Schema for output tables
REDSHIFT_OUTPUT_TABLE=content_urls_joep  # Content storage table
```
**Important**: Keep Redshift credentials in separate file (e.g., `redshift`) and add to `.gitignore`

### Required (Google Ads - thema_ads_optimized)
```bash
GOOGLE_DEVELOPER_TOKEN=...           # Google Ads developer token
GOOGLE_REFRESH_TOKEN=1//09...        # OAuth refresh token
GOOGLE_CLIENT_ID=...apps.googleusercontent.com
GOOGLE_CLIENT_SECRET=GOCSPX-...     # OAuth client secret
GOOGLE_LOGIN_CUSTOMER_ID=3011145605  # Manager account ID

# Performance Tuning (optional)
MAX_CONCURRENT_CUSTOMERS=10          # Parallel customer processing
MAX_CONCURRENT_OPERATIONS=50         # Concurrent operations per customer
BATCH_SIZE=1000                      # Operations per batch (max 10000)
API_RETRY_ATTEMPTS=3                 # Retry failed operations
API_RETRY_DELAY=1.0                  # Delay between retries (seconds)
ENABLE_CACHING=true                  # Cache label/campaign lookups

# Application Settings
INPUT_FILE=input_data.xlsx           # Excel/CSV file to process
LOG_LEVEL=INFO                       # DEBUG | INFO | WARNING | ERROR
DRY_RUN=false                        # Set to true for testing
```

### Required (Legacy Script - thema_ads)
```bash
# Google Ads OAuth Credentials (REQUIRED)
GOOGLE_CLIENT_ID=...apps.googleusercontent.com
GOOGLE_CLIENT_SECRET=GOCSPX-...
GOOGLE_REFRESH_TOKEN=1//09...
GOOGLE_DEVELOPER_TOKEN=...
GOOGLE_LOGIN_CUSTOMER_ID=...

# Azure Mail Credentials (OPTIONAL - for email features)
MAIL_TENANT_ID=...
MAIL_CLIENT_ID=...
MAIL_CLIENT_SECRET=...
MAIL_CLIENT_SECRET_ID=...

# File Paths (OPTIONAL - defaults provided)
EXCEL_PATH=C:\Users\YourName\Downloads\Python\your_file.xlsx
SERVICE_ACCOUNT_FILE=C:\Users\YourName\Downloads\Python\service-account.json
```

### Important Notes
- **OAuth Credentials**: refresh_token must match the client_id/client_secret used to generate it
- **API Version**: Requires google-ads>=25.1.0
- **Performance**: For 1M ads, consider running in chunks of 10k-50k
- **Thema Ads Integration**: Google Ads automation features are integrated into the main application (backend/thema_ads_service.py, frontend/js/thema-ads.js) rather than being a separate directory

## Database Schema

### Architecture: Local PostgreSQL Primary (seo_tools_db)
All data lives in the local PostgreSQL container. See LEARNINGS.md for connection details.

**Primary tables (schema `pa`)**:
- `pa.jvs_seo_werkvoorraad` - URL work queue (~243K URLs, kopteksten: 0=pending, 1=has content)
- `pa.jvs_seo_werkvoorraad_kopteksten_check` - Processing status tracking (success/skipped/failed)
- `pa.content_urls_joep` - Generated SEO content (~152K entries)
- `pa.faq_content` - Generated FAQ content
- `pa.faq_tracking` - FAQ processing status
- `pa.unique_titles` - AI-generated titles (~1M entries)
- `pa.link_validation_results` - SEO link validation history
- `pa.faq_validation_results` - FAQ link validation history
- `pa.content_history` - Content backup before resets
- Thema Ads tables (jobs, job_items, input_data)

**Pending URL calculation**: `WHERE werkvoorraad.url NOT IN tracking_table` (LEFT JOIN, see LEARNINGS.md "Stuck Pending URLs")

### Thema Ads Job Tracking
```sql
-- Jobs table: tracks each processing job
CREATE TABLE thema_ads_jobs (
    id SERIAL PRIMARY KEY,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    total_ad_groups INTEGER DEFAULT 0,
    processed_ad_groups INTEGER DEFAULT 0,
    successful_ad_groups INTEGER DEFAULT 0,
    failed_ad_groups INTEGER DEFAULT 0,
    skipped_ad_groups INTEGER DEFAULT 0,
    batch_size INTEGER DEFAULT 7500,            -- User-configurable API batch size (1000-10000)
    input_file VARCHAR(255),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);

-- Job items: tracks each individual ad group
CREATE TABLE thema_ads_job_items (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES thema_ads_jobs(id) ON DELETE CASCADE,
    customer_id VARCHAR(50) NOT NULL,
    campaign_id VARCHAR(50),           -- Optional: from CSV or fetched at runtime
    campaign_name TEXT,                -- Optional: from CSV or fetched at runtime
    ad_group_id VARCHAR(50) NOT NULL,
    ad_group_name TEXT,                -- Optional: for ID resolution (Excel precision loss fix)
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    new_ad_resource VARCHAR(500),
    error_message TEXT,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Input data: stores uploaded CSV data
CREATE TABLE thema_ads_input_data (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES thema_ads_jobs(id) ON DELETE CASCADE,
    customer_id VARCHAR(50) NOT NULL,
    campaign_id VARCHAR(50),           -- Optional: from CSV or fetched at runtime
    campaign_name TEXT,                -- Optional: from CSV or fetched at runtime
    ad_group_id VARCHAR(50) NOT NULL,
    ad_group_name TEXT,                -- Optional: for ID resolution (Excel precision loss fix)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SEO workflow tables (in seo_tools_db, schema pa)
CREATE TABLE pa.jvs_seo_werkvoorraad (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    kopteksten INTEGER DEFAULT 0,  -- 0=pending, 1=has content
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tracking table with status tracking
CREATE TABLE pa.jvs_seo_werkvoorraad_kopteksten_check (
    url VARCHAR(500) PRIMARY KEY,
    status VARCHAR(50) DEFAULT 'pending',  -- 'success', 'skipped', 'failed'
    skip_reason VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_kopteksten_check_status ON pa.jvs_seo_werkvoorraad_kopteksten_check(status);

CREATE TABLE pa.content_urls_joep (
    id SERIAL PRIMARY KEY,
    url VARCHAR(500) NOT NULL,
    content TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Link validation results tracking
CREATE TABLE pa.link_validation_results (
    id SERIAL PRIMARY KEY,
    content_url TEXT NOT NULL,
    total_links INTEGER DEFAULT 0,
    broken_links INTEGER DEFAULT 0,
    valid_links INTEGER DEFAULT 0,
    broken_link_details JSONB,  -- Stores array of broken link objects with url, status_code, status_text
    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- FAQ validation results tracking (prevents re-validation)
CREATE TABLE pa.faq_validation_results (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    total_links INTEGER DEFAULT 0,
    valid_links INTEGER DEFAULT 0,
    gone_links INTEGER DEFAULT 0,
    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_faq_validation_url ON pa.faq_validation_results(url);
```

## Dependencies

### Backend (Python 3.11)
```
# FastAPI & Web
fastapi==0.104.1          # Web framework
uvicorn[standard]==0.24.0 # ASGI server
python-multipart==0.0.6   # File upload support (CSV)

# AI & External APIs
openai==1.35.0            # OpenAI API client
httpx==0.25.2             # HTTP client (pinned for OpenAI compatibility)
requests==2.31.0          # HTTP requests
beautifulsoup4==4.12.3    # Web scraping
lxml==5.1.0               # XML/HTML parsing (BeautifulSoup parser, 2-3x faster)

# Database
psycopg2-binary==2.9.9    # PostgreSQL adapter

# Google Ads
google-ads>=25.1.0        # Google Ads API client (minimum v25.1.0)
pandas==2.2.0             # Data processing and Excel handling
openpyxl==3.1.2           # Excel file reading

# Utilities
python-dotenv==1.0.0      # Environment variable management
```

## Git Repository

- **URL**: https://github.com/joep-1993/dm-tools
- **User**: joep-1993 <joepvanschagen34@gmail.com>
- **Authentication**: SSH (ed25519 key)
- **Protected Files**: .env files, *.xlsx, *.xls, thema_ads_optimized/, themes/ (all in .gitignore)

## API Endpoints

### Core
- `GET /` - System status
- `GET /api/health` - Health check
- `POST /api/generate` - AI text generation
- `GET /static/*` - Frontend files

### SEO Workflow
- `POST /api/process-urls?batch_size=10&parallel_workers=3&conservative_mode=false` - Process URLs with parallel workers (synchronous processing - optimized for database connection pooling, batch_size: min 1 no max, parallel_workers: 1-20, conservative_mode forces 1 worker with 0.5-0.7s delay)
- `GET /api/status` - Get SEO processing status (includes total, processed, skipped, failed, pending counts)
- `POST /api/upload-urls` - Upload text file with URLs (one per line, duplicates skipped)
- `DELETE /api/result/{url}` - Delete result and reset URL to pending
- `GET /api/export/xlsx` - Export all generated content as Excel XLSX (from local PostgreSQL, sanitizes illegal characters)
- `GET /api/export/json` - Export all generated content as JSON
- `POST /api/validate-links?batch_size=1000&parallel_workers=3&conservative_mode=false` - Validate hyperlinks in content (checks for 301/404, auto-resets to pending if broken) (batch_size: min 1, no upper limit, parallel_workers: 1-20, conservative_mode forces 1 worker with 0.5-0.7s delay per link). Only validates URLs not yet validated.
- `POST /api/validate-all-links?parallel_workers=3` - Validate ALL unvalidated URLs in single batch. Uses LEFT JOIN for efficient filtering. Returns: validated count, urls_corrected count, moved_to_pending count.
- `GET /api/validation-history?limit=20` - Get link validation history with broken link details
- `DELETE /api/validation-history/reset` - Reset all validation history to allow re-validation of all URLs

### FAQ Link Validation
- `POST /api/faq/validate-links?batch_size=100&parallel_workers=3` - Validate FAQ hyperlinks via Elasticsearch lookup. Only validates unvalidated FAQs. Resets FAQs with gone products to pending.
- `POST /api/faq/validate-all-links?parallel_workers=3` - Validate ALL unvalidated FAQ links until complete. Records results to tracking table.
- `DELETE /api/faq/validation-history/reset` - Reset FAQ validation history to allow re-validation of all FAQs.

### Content Publishing
- `GET /api/content-publish/stats` - Get publishing stats (content_top count, FAQ count, total URLs)
- `GET /api/content-publish/preview?limit=10` - Preview content items to be published
- `GET /api/content-publish/curl?limit=10&environment=dev` - Generate curl command for testing
- `POST /api/content-publish?dry_run=true&environment=dev` - Publish content (dry_run=true returns stats, false starts background task)
- `GET /api/content-publish/status/{task_id}` - Poll background task status (pending/running/completed/failed)

### Keyword Planner
- `POST /api/keyword-planner/search-volumes` - Get search volumes for keyword list (JSON: `{"keywords": [...]}`, max 50,000)
- `POST /api/keyword-planner/upload-excel` - Upload Excel with keywords in first column
- `POST /api/keyword-planner/test` - Test Google Ads API connection
- `POST /api/keyword-planner/download` - Download results as Excel
- `POST /api/keyword-planner/category-volumes` - Combine keyword with all preloaded categories (JSON: `{"keyword": "nike"}`), returns volumes per deepest_cat and maincat
- `POST /api/keyword-planner/category-volumes/download` - Download category volume results as Excel (JSON: `{"deepest_cat_results": [...]}`)

### Labels Applied by Thema Ads
**Ad Groups get labeled with:**
- `BF_2025` - Black Friday 2025 campaign marker
- `SD_DONE` - Processing complete marker (used to skip already-processed ad groups)
  - **Only applied to successfully processed ad groups**
  - Ad groups without existing ads are NOT labeled (skipped for different reason)

**New Ads get labeled with:**
- `SINGLES_DAY` - Singles Day themed ad
- `THEMA_AD` - Themed ad marker

**Existing Ads get labeled with:**
- `THEMA_ORIGINAL` - Original ad marker

### Job Status Categories
**Completed**: Successfully created new themed ads
**Skipped**: Two types
- Already processed (has SD_DONE label from previous run)
- No existing ads (ad group has 0 ads, can't be processed)

**Failed**: Actual errors (API failures, permission issues, etc.)

### Thema Ads Job Management
- `POST /api/thema-ads/discover` - Auto-discover ad groups from Google Ads MCC account (params: limit, batch_size, see Auto-Discover Mode below)
- `POST /api/thema-ads/upload` - Upload CSV file and auto-start processing (params: file, batch_size, see CSV Format below)
- `POST /api/thema-ads/jobs/{job_id}/start` - Start processing job (deprecated - jobs auto-start on upload)
- `POST /api/thema-ads/jobs/{job_id}/pause` - Pause running job
- `POST /api/thema-ads/jobs/{job_id}/resume` - Resume paused/failed job
- `GET /api/thema-ads/jobs/{job_id}` - Get job status & progress
- `GET /api/thema-ads/jobs` - List all jobs (limit=20)
- `GET /api/thema-ads/jobs/{job_id}/failed-items-csv` - Download failed and skipped items as CSV (includes status and reason columns)
- `DELETE /api/thema-ads/jobs/{job_id}` - Delete job and all associated data (blocks running jobs)

#### Auto-Discover Mode
Frontend has two tabs:
1. **CSV Upload**: Manual upload with customer_id and ad_group_id
2. **Auto-Discover**: Automatically query Google Ads to find ad groups

**Auto-Discover Criteria:**
- MCC Account: 3011145605
- Customer Accounts: Name starts with "Beslist.nl -"
- Campaigns: Name starts with "HS/" AND status = ENABLED
- Ad Groups: Status = ENABLED AND does NOT have SD_DONE label
- Optional limit parameter (recommended: 100-1000 for testing)
- Configurable batch_size (1000-10000, default: 7500)
- Returns discovered ad groups and automatically starts processing

**Performance:**
- Direct ad query with cross-resource filtering: 74% fewer API queries (271→71 for 146k ad groups)
- Batched label checking: ~20 API calls for 146k ad groups (vs 146k individual calls)
- Default batch size: 7,500 ad groups per query (user-configurable)
- Discovery time: ~30-60 seconds for full account scan (optimized from 2+ minutes)

#### CSV Format
**Minimum columns** (campaign info fetched at runtime):
- `customer_id` (required) - dashes automatically removed
- `ad_group_id` (required)

**Recommended columns** (faster, no API calls):
- `customer_id` (required)
- `campaign_id` (optional)
- `campaign_name` (optional)
- `ad_group_id` (required)
- `ad_group_name` (optional, recommended) - resolves correct IDs to fix Excel precision loss

**Frontend Parameters**:
- `batch_size` (optional, default: 7500) - API batch size for processing (1000-10000)

**Notes**:
- Column order doesn't matter (parsed by name, not position)
- Extra columns are ignored (e.g., status, budget)
- Empty rows are automatically skipped
- Delimiter auto-detected (comma or semicolon)
- Maximum file size: 30MB
- Encoding auto-detected (UTF-8, Windows-1252, ISO-8859-1, Latin1)
- Jobs automatically start processing after successful upload
- **Excel Precision Loss**: Include `ad_group_name` column to avoid ID corruption from scientific notation
  - Excel converts large IDs (168066123456) to scientific notation (1.68066E+11)
  - Scientific notation loses precision (becomes 168066000000)
  - System uses ad_group_name to look up correct ID from Google Ads API

#### Downloaded CSV Format (Failed/Skipped Items)
- `customer_id` - Google Ads customer ID
- `campaign_id` - Campaign ID (if available)
- `campaign_name` - Campaign name (if available)
- `ad_group_id` - Ad group ID
- `status` - "failed" or "skipped"
- `reason` - Human-readable explanation:
  - "Ad group has 'SD_DONE' label (already processed)"
  - "Ad group has 0 ads"
  - Original error message for actual failures

---

## Additional Documentation

For detailed architectural decisions, design patterns, and technology rationales, see **ARCHITECTURE.md** in the project root.

---
_Last updated: 2026-02-10_
