# PROJECT INDEX
_Project structure and technical specs. Update when: creating files, adding dependencies, defining schemas._

## Stack
Backend: FastAPI (Python 3.11, ThreadPoolExecutor for parallel processing, psycopg2 connection pooling 2-10 conns) | Frontend: Bootstrap 5 + Vanilla JS | Database: PostgreSQL 15 (local tracking) + AWS Redshift (data storage, COPY command for bulk inserts) | AI: OpenAI API | Deploy: Docker + docker-compose | Google Ads: AsyncIO + Batch API (v28) | Automation: Docker multi-stage builds

## Directory Structure
```
content_top/                 # Unified SEO Tools Platform (Port 8003)
├── .claude/              # Claude Code configuration
│   ├── commands/         # Custom slash commands
│   │   ├── skip-permissions.md     # Toggle bypassPermissions mode
│   │   └── restore-permissions.md  # Restore default permissions
│   └── settings.local.json  # Local settings (git ignored)
├── thema_ads_optimized/  # Google Ads theme management (merged from theme_ads)
│   ├── themes.py         # Theme definitions (black_friday, cyber_monday, etc.)
│   ├── config.py         # Configuration management
│   └── ...               # Google Ads automation utilities
├── themes/               # Theme template data (merged from theme_ads)
├── cc1/                   # CC1 documentation + utilities
│   ├── TASKS.md          # Task tracking
│   ├── LEARNINGS.md      # Knowledge capture
│   ├── BACKLOG.md        # Future planning
│   ├── PROJECT_INDEX.md  # This file
│   ├── lookup_plp_urls.py    # Elasticsearch plpUrl lookup script (batch queries, maincat mapping)
│   ├── input_urls.csv        # Input URLs for lookup
│   └── output_plp_urls.csv   # Output with original URL and plpUrl (or GONE)
├── SEO_koptekst/         # Legacy SEO data directory
├── backend/
│   ├── main.py           # FastAPI app - Unified SEO Tools Platform
│   │                     # Includes: SEO content, FAQ, Thema Ads (via router)
│   │                     # CSV parsing: empty row handling, dash removal, optional columns
│   │                     # SEO content generation uses Product Search API via scrape_product_page_api()
│   ├── thema_ads_router.py  # APIRouter for all /api/thema-ads/* endpoints
│   ├── thema_ads_service.py # Thema Ads business logic (merged from theme_ads)
│   ├── thema_ads_db.py      # Thema Ads database functions (merged from theme_ads)
│   ├── database.py       # Hybrid database connections (PostgreSQL + Redshift)
│   │                     # Functions: get_db_connection(), get_redshift_connection(), get_output_connection()
│   │                     # Schema: campaign_id and campaign_name columns added
│   ├── gpt_service.py    # AI integration with optimized prompts for concise hyperlink text (3-5 words max)
│   ├── link_validator.py # Hyperlink validation via Elasticsearch plpUrl lookup (uses local PostgreSQL only)
│   │                     # Auto-corrects outdated URLs, resets GONE products to pending
│   │                     # Adds gone URLs to werkvoorraad for reprocessing
│   ├── seo_content_generator.py  # SEO content from Product Search API
│   │                     # Parses /products/{maincat}/{category}/c/{filters} URLs
│   │                     # Fetches 30 products, generates GPT content with plpUrl links
│   │                     # Outputs to Excel (url, maincat_id, category, content)
│   ├── import_content.py # CSV import utility for bulk content upload (semicolon delimiter)
│   ├── sync_werkvoorraad.py  # Utility: Synchronize werkvoorraad with content table
│   ├── sync_redshift_flags.py # Utility: Sync Redshift kopteksten flags with local content table (fixes data consistency)
│   ├── deduplicate_content.py # Utility: Remove duplicate URLs from content table
│   ├── thema_ads_service.py  # Thema Ads job management with state persistence
│   │                          # Features: delete job, campaign info fetching at runtime
│   ├── thema_ads_schema.sql  # Database schema for job tracking
│   ├── schema.sql        # SEO workflow database schema
│   └── scraper_service.py    # Web scraping utilities with 0.2-0.3s delay (optimized mode) or 0.5-0.7s delay (conservative mode), custom UA 'Beslist script voor SEO' (bypasses VPN, whitelisted IP has no rate limits), hidden 503 detection (checks HTML body, returns {'error': '503'} for immediate batch stop)
│                             # Key functions: scrape_product_page_api() - Product Search API integration
│                             # parse_beslist_url(), build_api_params(), extract_selected_facets(), build_product_subject()
│                             # Extracts selected facets (detailValue) for product subjects with smart category inclusion
├── openvpn               # OpenVPN client config (with pull-filter for split tunneling)
├── frontend/
│   ├── dashboard.html    # Unified dashboard - entry point
│   ├── index.html        # SEO Content Generation page
│   ├── faq.html          # FAQ Generation page
│   ├── thema-ads.html    # Thema Ads page (merged from theme_ads)
│   ├── css/
│   │   └── style.css     # Custom styles (purple navbar, grey headers, orange buttons)
│   └── js/
│       ├── app.js        # SEO content frontend logic
│       ├── faq.js        # FAQ frontend logic
│       └── thema-ads.js  # Thema Ads frontend logic with polling
│                         # Features: delete job UI with confirmation
├── docker-compose.yml    # Service orchestration (no version attr)
├── Dockerfile           # Python container
├── requirements.txt     # Python dependencies
├── .env.example        # Environment template
├── .env                # Local environment (git ignored)
├── .gitignore          # Version control excludes
│                       # Ignores: .env files, Excel files (*.xlsx, *.xls), old thema_ads_project/
├── ARCHITECTURE.md     # System architecture and design decisions
├── README.md           # Quick start guide
├── CLAUDE.md           # Claude Code instructions
├── THEMA_ADS_GUIDE.md  # Complete Thema Ads documentation
├── START_HERE.md       # Quick start for web interface
├── start-thema-ads.sh  # Automated setup script
├── sample_input.csv    # Example CSV for Thema Ads upload
└── seo_urls            # Input file with URLs to process (75,858 URLs)
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

### Required (Redshift - Output Storage)
```bash
USE_REDSHIFT_OUTPUT=true  # Enable Redshift for output tables
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

### Architecture: Hybrid (PostgreSQL + Redshift)
**Local PostgreSQL** (tracking & temporary):
- `pa.jvs_seo_werkvoorraad_kopteksten_check` - Processing status tracking
- `pa.link_validation_results` - Link validation history
- Thema Ads tables (jobs, job_items, input_data)

**Redshift** (persistent data):
- `pa.jvs_seo_werkvoorraad_shopping_season` - Work queue (72,992 URLs for shopping season, kopteksten: 0=pending, 1=has content, 2=processed without content)
- `pa.content_urls_joep` - Generated content (columns: url, content)

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

-- SEO workflow tables
CREATE TABLE pa.jvs_seo_werkvoorraad_shopping_season (
    url VARCHAR(500) PRIMARY KEY,
    kopteksten INTEGER DEFAULT 0,  -- 0=pending, 1=has content, 2=processed without content
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
_Last updated: 2025-12-21_
