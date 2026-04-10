# ARCHITECTURE.md

**Project:** DM Tools - Digital Marketing Tools Platform
**Last Updated:** 2026-04-10 CET
**Timezone:** Europe/Amsterdam (CET/CEST)

## Table of Contents
1. [System Overview](#system-overview)
2. [Frontend Architecture](#frontend-architecture)
3. [Backend Architecture](#backend-architecture)
4. [Database Architecture](#database-architecture)
5. [Network Architecture](#network-architecture)
6. [Key Design Decisions](#key-design-decisions)
7. [Technology Choices](#technology-choices)

---

## System Overview

### High-Level Architecture
```
┌─────────────────┐         ┌─────────────────┐         ┌──────────────────┐
│   Web Browser   │────────▶│  FastAPI App    │────────▶│   PostgreSQL     │
│  (Bootstrap UI) │         │  (Port 8003)    │         │   (Port 5433)    │
└─────────────────┘         └─────────────────┘         └──────────────────┘
                                    │                             │
                                    │                             │
                                    ▼                             ▼
                            ┌──────────────┐           ┌──────────────────┐
                            │   OpenAI     │           │  AWS Redshift    │
                            │     API      │           │ (Data Storage)   │
                            └──────────────┘           └──────────────────┘
                                    │
                                    ▼
                            ┌──────────────┐
                            │  Beslist.nl  │
                            │  (Scraping)  │
                            └──────────────┘
```

### Core Workflow
1. **Input**: URLs loaded from local PostgreSQL table (`pa.jvs_seo_werkvoorraad` in `seo_tools_db`)
2. **Scraping**: Product Search API fetches product data (or web scraper with custom user agent)
3. **AI Generation**: OpenAI generates SEO-optimized content (100 words)
4. **Storage**: Content saved to local PostgreSQL (`pa.content_urls_joep`), tracking to `pa.jvs_seo_werkvoorraad_kopteksten_check`
5. **Quality Control**: Link validation via Elasticsearch lookup (replaces HTTP checking)

### Recent Fixes (2025-01-22)
1. **Three-State URL Tracking**: Implemented kopteksten=0/1/2 system for better analytics and preventing infinite retry loops
2. **503 Detection**: Scraper returns {'error': '503'} for immediate batch stop (not after 3 consecutive failures)
3. **Batch Size Fix**: Changed local tracking query to filter ALL processed URLs, preventing single-URL batches
4. **Frontend NaN Values**: Added default value handling (|| operator) to prevent undefined/NaN display in batch progress
5. **Database Sync Issue**: Fixed mismatch between local tracking (60,455 success) and Redshift (kopteksten=0). Synced 20,560 URLs.
6. **Scraper HTML Structure**: Updated product URL extraction from JavaScript `plpUrl` pattern to HTML `<a class="productLink--zqrcp">` elements
7. **False 503 Detection**: Changed from broad `'503' in response.text` to specific patterns to avoid false positives from URLs/IDs containing "503"
8. **Database Insert Method**: Switched from Redshift `copy_from()` to universal `executemany()` to fix COPY command syntax errors

### Deployment Model
- **Two modes**: Docker (`docker-compose up`) or Docker-free (`run_local.sh` / venv + uvicorn)
- **No build tools** - direct HTML/CSS/JS editing with auto-reload
- **Single-machine deployment** - designed for 1-10 users
- **Database**: Remote PostgreSQL at 10.1.32.9 (primary for both modes)
- **Windows auto-start**: Task Scheduler task "DM Tools Dashboard" runs `C:\Users\JoepvanSchagen\scripts\start-dm-dashboard.ps1` at logon — starts uvicorn via WSL, health-checks port 8003, closes window on success, stays open on error

---

## Frontend Architecture

### Technology Stack
- **Framework**: None (Vanilla JavaScript)
- **UI Library**: Bootstrap 5 (via CDN)
- **Build Tools**: None
- **File Structure**:
  ```
  frontend/
  ├── index.html           # Kopteksten (AI product recommendations)
  ├── faq.html             # FAQ's (SEO-optimized FAQs with Schema.org)
  ├── canonical.html       # Canonicals (canonical URL generation)
  ├── rfinder.html         # R-Finder (URL Discovery)
  ├── redirect-checker.html # Redirect Checker (HTTP status/redirects/canonicals)
  ├── 301-generator.html   # Redirects (facet sorting & transformations)
  ├── thema-ads.html       # Thema Ads Processing
  ├── gsd-campaigns.html   # GSD Campaigns (Google Shopping campaign management)
  ├── unique-titles.html   # Unique Titles Manager
  ├── keyword-planner.html # Keyword Planner (Google Ads search volumes)
  ├── indexnow.html        # IndexNow (URL submission for indexing)
  ├── index-checker.html   # Index Checker (Google index status via Search Console)
  ├── url-checker.html     # URL Checker (status codes, meta, H1, products)
  ├── dashboard.html       # DM Tools Dashboard (categorized tool overview)
  ├── css/
  │   └── style.css        # Custom styles (dropdown menus, responsive nav, shadows)
  └── js/
      ├── app.js           # Kopteksten application logic
      └── faq.js           # FAQ application logic
  ```

### Design Principles

#### 1. No Build Tools Philosophy
**Decision**: Use vanilla JavaScript + Bootstrap CDN instead of React/Vue/Webpack

**Rationale**:
- Instant changes: Edit → Save → Refresh (no compilation)
- No npm install delays (no node_modules)
- Works identically on any machine with Docker
- Reduces complexity for small teams
- Saves 500MB+ of disk space

**Trade-offs**:
- No JSX/component frameworks
- Manual DOM manipulation
- Limited code reusability

#### 2. Real-Time Progress Tracking
**Pattern**: JavaScript polling instead of WebSockets

**Implementation**:
```javascript
pollInterval = setInterval(updateJobStatus, 2000);  // Poll every 2 seconds
if (status === 'completed') clearInterval(pollInterval);
```

**Rationale**:
- Simpler than WebSocket setup
- No need for persistent connections
- Sufficient for 2-second update intervals
- Easier to debug

#### 3. Direct DOM Manipulation
**Pattern**: Avoid innerHTML for complex content with hyperlinks

**Issue**: Browser auto-links HTML tags when using template literals
```javascript
// ❌ Wrong: Browser parses </div> as URL
html += `<a href="/product">Product</a></div>`;

// ✅ Correct: Create DOM elements separately
const div = document.createElement('div');
div.innerHTML = content;
```

#### 4. Handling Null Timestamps
**Pattern**: Gracefully handle missing created_at from Redshift

**Issue**: Redshift table lacks created_at column, causing "1-1-1970" display

**Solution**:
```javascript
const dateText = item.created_at ? new Date(item.created_at).toLocaleString() : 'N/A';
```

**Rationale**:
- Redshift table schema differences require null handling
- "N/A" is clearer than epoch date (1970-01-01)
- Prevents confusion about when content was created

### UI Theme & Color Scheme

**Design Philosophy**: Custom color palette overriding Bootstrap defaults

#### Color Codes
```css
:root {
    --color-primary: #059CDF;   /* Blue */
    --color-info: #9C3095;      /* Purple/Magenta */
    --color-success: #A0D168;   /* Green */
}
```

#### Color Usage Map

| UI Element | Color | Hex Code | Bootstrap Class |
|------------|-------|----------|-----------------|
| **Navbar** | Blue | #059CDF | `.bg-primary` |
| **Upload URLs Section** | Purple | #9C3095 | `.bg-info` |
| **SEO Content Generation Section** | Green | #A0D168 | `.bg-success` |
| **Link Validation Section** | Grey | Bootstrap default | `.bg-secondary` |
| **Processing Status Numbers** | Blue | #059CDF | `.text-primary` |
| **Primary Buttons** | Blue | #059CDF | `.btn-primary` |
| **Success Buttons** | Green | #A0D168 | `.btn-success` |
| **Progress Bars** | Green | #A0D168 | `.progress-bar` |

#### Implementation Details
- **Location**: `frontend/css/style.css`
- **Method**: CSS custom properties (CSS variables) with `!important` overrides
- **Hover States**: 20% darker shade for better UX
- **Consistency**: All Bootstrap color classes overridden for uniform theme

#### Design Rationale
- Blue (primary): Professional, trustworthy, action-oriented
- Purple (info): Distinctive, creative, stands out from standard Bootstrap
- Green (success): Positive feedback, completion, growth
- Grey (secondary): Neutral, non-critical actions

---

## Backend Architecture

### Technology Stack
- **Framework**: FastAPI 0.104.1
- **Server**: Uvicorn with auto-reload
- **Python Version**: 3.11
- **Parallelization**: ThreadPoolExecutor (1-100 workers, default 50)
- **Session Management**: Persistent HTTP sessions with connection pooling

### Service Layer Architecture

```
main.py (API Endpoints)
    │
    ├──▶ scraper_service.py (Web Scraping)
    │       └──▶ requests + BeautifulSoup (lxml parser)
    │
    ├──▶ gpt_service.py (AI Content Generation)
    │       └──▶ OpenAI API (gpt-4o-mini, 2000 max_tokens)
    │
    ├──▶ batch_api_service.py (OpenAI Batch API for bulk processing)
    │       └──▶ JSONL upload → OpenAI Batch API → result download (50% cheaper)
    │
    ├──▶ link_validator.py (Quality Control)
    │       └──▶ HTTP status checking (301/404 detection)
    │
    ├──▶ ai_titles_service.py (Unique Title Generation)
    │       └──▶ Product Search API + OpenAI (facet classification, met-features, spec values)
    │
    ├──▶ indexnow_service.py (IndexNow URL Submission)
    │       └──▶ IndexNow API + local PostgreSQL dedup (10K daily limit)
    │
    ├──▶ canonical_service.py (URL Canonicalization)
    │       └──▶ Redshift queries + URL transformation rules
    │
    ├──▶ redirect_301_service.py (301 Redirect Generation)
    │       └──▶ Facet sorting + category/facet transformations
    │
    ├──▶ rfinder_service.py (R-URL Discovery)
    │       └──▶ Redshift queries for /r/ URLs
    │
    ├──▶ redirect_checker (in main.py)
    │       └──▶ HTTP status codes, redirects, canonical URLs
    │
    └──▶ database.py (Data Access Layer)
            ├──▶ PostgreSQL (local tracking + IndexNow dedup)
            └──▶ Redshift (persistent storage)
```

### Key Design Decisions

#### 1. Parallel Processing with ThreadPoolExecutor
**Decision**: Use thread-based parallelism (1-100 configurable workers, default 50)

**Rationale**:
- I/O-bound workload (scraping + API calls)
- Threads work well for I/O (no CPU-bound bottleneck)
- Each worker gets own database connection (pool maxconn=60)
- OpenAI rate limits allow 30K RPM / 150M TPM — supports high concurrency

**Performance**: ~2,500 URLs/hour with 50 workers (real-time API)

#### 1b. OpenAI Batch API (Bulk Processing)
**Decision**: Added optional Batch API mode for bulk runs (`backend/batch_api_service.py`)

**Rationale**:
- 50% cheaper than real-time API (batch pricing)
- No rate limit concerns — OpenAI processes asynchronously within 24h window
- Better for large bulk runs (10K+ URLs)

**How it works**:
1. Fetch all pending URLs from DB
2. Call Product Search API for each (50 concurrent threads) to get product data
3. Build prompts, write JSONL file
4. Upload to OpenAI Files API, create batch job
5. Poll every 15s until complete (~15-60 min for typical batches)
6. Download results, parse, save to DB in bulk

**Frontend**: "Bulk API" checkbox on FAQ and Kopteksten pages. When checked, greys out batch size/workers/single-batch button. "Process All URLs" triggers batch pipeline with phase-based progress bar.

#### 2. Batch Database Operations
**Decision**: Batch all Redshift operations after parallel processing

**Problem**: 10 workers × 2 Redshift calls = 20 simultaneous connections

**Solution**:
```python
# Workers collect operations instead of executing
def process_single_url(url):
    redshift_ops = []
    redshift_ops.append(('insert_content', url, content))
    return (result, redshift_ops)

# Execute all operations in single transaction using executemany()
for result, ops in result_tuples:
    all_redshift_ops.extend(ops)
output_cur.executemany("INSERT INTO pa.content_urls_joep (url, content) VALUES (%s, %s)", insert_content_data)
```

**Impact**: 15-20% throughput improvement

**Update (2025-01-22)**: Switched from `copy_from()` to `executemany()` for better compatibility with both PostgreSQL and Redshift. Previous COPY command caused syntax errors with psycopg2.

#### 3. Scraper Configuration
**User Agent**: `"Beslist script voor SEO"`

**Rationale**:
- Clear identification in server logs
- Distinguishes scraper from browser traffic
- Helps with debugging rate limiting issues
- IT team can easily filter/analyze traffic

**HTML Structure** (Updated 2025-01-22):
- **Previous**: Extracted URLs from JavaScript `"plpUrl":"/p/.../40000/.../"` pattern
- **Current**: Extracts from HTML `<a class="productLink--zqrcp" href="/p/.../36000/.../">` elements
- **Reason**: Beslist.nl changed their page structure; JavaScript pattern no longer reliable

**503 Error Detection** (Updated 2025-01-22):
- **Previous**: `'503' in response.text` (too broad)
- **Current**: Specific patterns - `'service unavailable'`, `'503 service'`, `'error 503'`
- **Reason**: Avoided false positives from URLs/product IDs containing "503" (e.g., `/kantoorartikelen_558034_558644/`)

**Rate Limiting**: Two modes available
- **Optimized Mode** (default): 0.2-0.3s delay (~3-5 URLs/sec)
- **Conservative Mode**: 0.5-0.7s delay (~2 URLs/sec) with 1 worker only
- Whitelisted IP (87.212.193.148) bypasses captchas
- Rate limit testing showed no throttling even at 0s delay
- Conservative mode available as safety option for cautious operation

#### 4. Performance Optimizations
**Goal**: Process 131K URLs in 4-9 days (was 18-46 days)

**Optimizations**:
1. Reduced AI max_tokens: 500 → 300 (content is ~100 words)
2. Scraping delay: 0.5-1s → 0.2-0.3s (whitelisted IP)
3. BeautifulSoup parser: html.parser → lxml (2-3x faster)
4. Batch database commits (1 commit per URL instead of 3-5)
5. Use cursor.executemany() for batch inserts

**Result**: 30-50% faster per URL (4-10s → 2.5-7s)

---

## Database Architecture

### Primary Database: Local PostgreSQL (`seo_tools_db`)

**Current state**: All data lives in the local PostgreSQL container. Redshift is legacy/optional (`USE_REDSHIFT_OUTPUT=false`).

```
┌─────────────────────────────────────────────────────────────┐
│              DM Tools App (dm_tools_app:8003)               │
└─────────────────────────────────────────────────────────────┘
                │
                ▼
    ┌───────────────────────┐
    │  seo_tools_db         │      ┌──────────────────────────┐
    │  PostgreSQL (Local)   │      │  N8N Vector DB (Copy)    │
    │  Container: seo_tools_db     │  Host: 10.1.32.9         │
    │  Database: seo_tools  │      │  Database: n8n-vector-db │
    │  Port: 5432 (internal)│      │  User: dbadmin           │
    │  User: postgres       │      │  Schema: pa (copy)       │
    │  Password: postgres   │      └──────────────────────────┘
    │  Schema: pa           │
    │  ALL tables live here │
    └───────────────────────┘
```

### Three Databases (and their roles)

| Database | Role | Used By |
|----------|------|---------|
| **seo_tools_db** (local PostgreSQL) | **PRIMARY** - all werkvoorraad, tracking, content, validation | dm-tools app (frontend/backend) |
| **n8n-vector-db** (10.1.32.9) | **COPY** - synced from seo_tools_db for n8n workflows | n8n workflows only |
| **Redshift** (AWS) | **LEGACY** - optional, disabled by default (`USE_REDSHIFT_OUTPUT=false`) | Not actively used |

**IMPORTANT**: When debugging kopteksten issues, always query **seo_tools_db** (the local Docker PostgreSQL), NOT the n8n vector DB or Redshift. The app reads/writes exclusively to seo_tools_db.

### Quick Access to Primary Database
```bash
# Query from host via Docker
docker exec seo_tools_db psql -U postgres -d seo_tools -c "SELECT ..."

# Connect interactively
docker exec -it seo_tools_db psql -U postgres -d seo_tools
```

### Table Allocation (all in `seo_tools_db`, schema `pa`)

**Work Queue & Content**:
- `pa.jvs_seo_werkvoorraad` - URL work queue (~243K URLs), `kopteksten` flag (0=pending, 1=has content)
- `pa.content_urls_joep` - Generated SEO content (~152K entries)
- `pa.faq_content` - Generated FAQ content
- `pa.unique_titles` - AI-generated titles (~1M entries)

**Tracking**:
- `pa.jvs_seo_werkvoorraad_kopteksten_check` - Processing status (success/skipped/failed/pending)
- `pa.faq_tracking` - FAQ processing status
- `pa.link_validation_results` - Link validation history
- `pa.faq_validation_results` - FAQ link validation history
- `pa.content_history` - Content backup before resets

**Other**:
- `thema_ads_jobs` / `thema_ads_job_items` / `thema_ads_input_data` - Google Ads job tracking

### Pending URL Calculation (Kopteksten)

The frontend's "pending" count uses this query:
```sql
SELECT COUNT(*) FROM pa.jvs_seo_werkvoorraad w
LEFT JOIN pa.jvs_seo_werkvoorraad_kopteksten_check t ON w.url = t.url
WHERE t.url IS NULL
```

**Pending = URLs in werkvoorraad that are NOT in the tracking table.**

This means if ALL URLs end up in the tracking table (even with status='pending'), the frontend shows 0 pending. See LEARNINGS.md "Stuck Pending URLs" for the fix.

### Database Connection Strategy

```python
# Connection pool connects to seo_tools_db via DATABASE_URL env var
def get_db_connection():          # Local PostgreSQL (primary)
def get_redshift_connection():    # Redshift (legacy, rarely used)
def get_output_connection():      # Routes to Redshift or PostgreSQL based on USE_REDSHIFT_OUTPUT
```

### Schema Design Decisions

#### 1. Kopteksten URL Tracking
**Pattern**: Flag-based tracking on werkvoorraad table
```sql
CREATE TABLE pa.jvs_seo_werkvoorraad (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    kopteksten INTEGER DEFAULT 0,  -- 0=pending, 1=has content
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Rationale**:
- **kopteksten = 0**: Not yet processed (pending)
- **kopteksten = 1**: Successfully processed with content in `content_urls_joep`
- **kopteksten = 2**: Processed but no usable content (skipped, failed non-503 errors)
- **503 errors**: Kept at kopteksten=0 for retry, batch stops immediately
- **Benefits**: Better analytics, can query problematic URLs, prevents re-processing empty pages

**Implementation** (2025-10-22):
- Success: `update_werkvoorraad_success` operation sets kopteksten=1
- Processed without content: `update_werkvoorraad_processed` sets kopteksten=2
- Rate limiting (503): No Redshift update, stays kopteksten=0

#### 2. Status Tracking with Separate Table
**Pattern**: Separate tracking table instead of status column
```sql
CREATE TABLE pa.jvs_seo_werkvoorraad_kopteksten_check (
    url VARCHAR(500) PRIMARY KEY,
    status VARCHAR(50) DEFAULT 'pending',  -- 'success', 'skipped', 'failed'
    skip_reason VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Rationale**:
- Work queue remains clean (minimal columns)
- Tracking can be reset without affecting work queue
- Allows multiple processing attempts with history

#### 3. Link Validation with JSONB
**Pattern**: Store validation details as JSONB for flexibility
```sql
CREATE TABLE pa.link_validation_results (
    content_url TEXT NOT NULL,
    broken_link_details JSONB,  -- Array of {url, status_code, status_text}
    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Rationale**:
- Variable number of broken links per URL
- No need for separate broken_links table
- Easy to query and display

### Database Maintenance Utilities

#### Deduplication Strategy
**Problem**: Bulk imports or interrupted processing can create duplicate content records

**Solution**: `backend/deduplicate_content.py`
```sql
CREATE TEMP TABLE content_deduped AS
SELECT url, content
FROM (
    SELECT url, content,
           ROW_NUMBER() OVER (PARTITION BY url ORDER BY content) as rn
    FROM pa.content_urls_joep
)
WHERE rn = 1;

DELETE FROM pa.content_urls_joep;
INSERT INTO pa.content_urls_joep (url, content)
SELECT url, content FROM content_deduped;
```

**Results**: Removed 48,846 duplicates (108,722 → 59,876 unique URLs)

#### Werkvoorraad Synchronization
**Problem**: Content exists but werkvoorraad table not updated (URLs marked pending but have content)

**Solution**: `backend/sync_werkvoorraad.py`
```sql
UPDATE pa.jvs_seo_werkvoorraad w
SET kopteksten = 1
FROM pa.content_urls_joep c
WHERE w.url = c.url AND w.kopteksten = 0;
```

**Results**:
- Initial sync: 17,672 URLs, 0 overlaps remaining
- **2025-01-22 Fix**: Synced additional 20,560 URLs after discovering local tracking had 60,455 "success" entries but Redshift still showed `kopteksten=0`

**Root Cause (2025-01-22)**:
Local PostgreSQL tracking table marked URLs as "success", but Redshift `kopteksten` flag was never updated. This caused the API to filter out all fetched URLs, returning "No URLs to process" despite 55k pending URLs.

**Use Cases**:
- After bulk CSV imports
- After manual content additions
- After interrupted processing sessions
- When content exists outside the work queue
- **When local tracking and Redshift are out of sync**

---

## Network Architecture

### Port Configuration
- **Frontend/API**: Port 8003 (Docker: external → 8000 container; Docker-free: direct on 8003)
- **PostgreSQL**: Port 5433 (external) → 5432 (container) — local DB still runs but remote 10.1.32.9 is primary
- **Reason**: Avoid conflicts with existing services on host

### VPN Bypass for Whitelisted IP

**Problem**: Company VPN routes through 94.142.210.226, but scraper needs whitelisted IP (87.212.193.148)

**Solution**: Windows static route with lower metric

```cmd
# Add persistent route (as Administrator)
route add -p 65.9.0.0 mask 255.255.0.0 192.168.1.1 metric 1 if 10
```

**How It Works**:
1. Windows routing is hierarchical (lower metric = higher priority)
2. VPN routes have metric 25-50
3. Our route (metric 1) takes precedence for CloudFront IPs (65.9.0.0/16)
4. WSL2 and Docker inherit Windows routing table
5. Route persists across reboots (`-p` flag)

**Result**: VPN stays connected (for Redshift), scraper uses whitelisted IP

### Failed Approaches
- OpenVPN client-side routing (server overrides)
- OpenVPN route-nopull (breaks internal routes)
- Privoxy proxy (still routes through VPN)
- Docker network_mode: host (still uses VPN)

**Key Learning**: For corporate VPNs, split tunneling must be configured at OS routing level, not application level

---

## Key Design Decisions

### 1. Single-Page Application vs Multi-Page
**Decision**: Single-page application (SPA)

**Rationale**:
- Simple project scope (one workflow)
- No need for routing complexity
- Faster perceived performance (no page reloads)
- Easier state management with JavaScript

### 2. Synchronous vs Asynchronous Job Processing
**Decision**: Synchronous processing with polling for updates

**Rationale**:
- Simpler than job queues (Celery/RQ)
- Sufficient for single-user workflow
- No need for distributed workers
- Easy to pause/resume jobs

### 3. Quality Control Strategy
**Decision**: Automatic link validation via Elasticsearch lookup with auto-reset to pending

**Workflow**:
1. Extract `<a href="/p/...">` hyperlinks from generated content
2. Look up each product in Elasticsearch by pimId (fast `terms` query)
3. Check shopCount >= 2 (product still has offers)
4. Three outcomes per link:
   - **Valid**: Product found with same plpUrl → no action
   - **Replaced**: Product found but plpUrl changed (slug update) → auto-correct link in content
   - **Gone**: Product not found or shopCount < 2 → back up content to `content_history`, delete, reset to pending for regeneration

**V4 UUID Product Lookup** (two-phase):
1. **Phase 1 (fast)**: Try `terms` query on `pimId` field with V4 UUID values
2. **Phase 2 (skip)**: V4 URLs not found via pimId are skipped (not marked as gone). Wildcard queries (`*V4_xxx*`) on plpUrl were disabled because they always timeout on ES due to leading wildcard full index scans

**Rationale**:
- Automated quality control via ES lookup (no HTTP requests to production)
- No manual intervention required
- Historical tracking via `pa.link_validation_results`
- Incremental validation (only unvalidated URLs via LEFT JOIN)
- Parallel processing with ThreadPoolExecutor (configurable workers)

### 4. Content Generation Constraints
**Decision**: GPT-4o-mini with 300 max_tokens

**Rationale**:
- Target: 100 words (~130 tokens)
- 300 tokens provides buffer for variation
- Reduced from 500 tokens (saves 10-15% processing time)
- Still generates quality content

**Prompt Engineering**:
- Explicit constraints: "KORTE, heldere omschrijving (max 3-5 woorden)" for hyperlinks
- Prevents long anchor text (e.g., full product names)
- Example-driven (show desired format in prompt)

---

## Technology Choices

### Why These Technologies?

#### FastAPI
**Chosen Over**: Flask, Django

**Reasons**:
- Automatic OpenAPI documentation
- Built-in async support (future-ready)
- Type hints for better IDE support
- Fast performance (on par with Node.js)
- Modern Python (3.11 features)

#### PostgreSQL + Redshift
**Chosen Over**: MySQL, MongoDB, Redshift-only

**Reasons**:
- PostgreSQL: Fast, reliable, excellent JSON support (JSONB)
- Redshift: Shared data warehouse (accessible to other teams)
- Hybrid: Best of both worlds (speed + scalability)

#### ThreadPoolExecutor
**Chosen Over**: AsyncIO, Celery, multiprocessing

**Reasons**:
- I/O-bound workload (perfect for threads)
- Simpler than async/await for requests + BeautifulSoup
- No need for separate worker process (Celery)
- Linear speedup up to 7 workers

#### Bootstrap
**Chosen Over**: Tailwind, Material-UI, custom CSS

**Reasons**:
- CDN-based (no build step)
- Familiar to most developers
- Comprehensive component library
- Responsive out of the box

#### OpenAI API
**Chosen Over**: Open-source LLMs (Llama, Mistral)

**Reasons**:
- Superior quality for Dutch content
- No infrastructure for self-hosting
- Predictable costs (per-token pricing)
- Fast inference (no GPU required)
- Batch API available for 50% cost reduction on bulk runs
- Rate limits (30K RPM, 150M TPM) support high parallelism

#### Docker + Docker Compose
**Chosen Over**: Bare metal, Kubernetes

**Reasons**:
- Consistent environment across machines
- Simple orchestration with docker-compose
- No need for Kubernetes complexity (small scale)
- Easy to version control (docker-compose.yml)

---

## External APIs

### Beslist Product Search API

**Endpoint**: `https://productsearch-v2.api.beslist.nl/search/products`

**Purpose**: Fetch product data for SEO content generation based on category/facet URLs.

#### Required Parameters
| Parameter | Example | Description |
|-----------|---------|-------------|
| `query` | `""` | Search query (can be empty for category browsing) |
| `mainCategory` | `kantoorartikelen` | Main category name (not ID) |
| `category` | `kantoorartikelen_558052_558970` | Category URL name |
| `filters[{facet}][0]` | `filters[merk][0]=2829915` | Facet filters (URL encoded) |
| `limit` | `76` | Max products to return |
| `offset` | `0` | Pagination offset |
| `isBot` | `false` | **REQUIRED** - API returns 400 without this |
| `countryLanguage` | `nl-nl` | **REQUIRED** - API returns 500 without this |

#### Optional Parameters
| Parameter | Example | Description |
|-----------|---------|-------------|
| `experiment` | `topProducts` | Ranking experiment |
| `trackTotalHits` | `false` | Include total hit count |

#### Request Headers
```
Accept: application/json
User-Agent: Beslist script voor SEO
```

#### Error Responses
| Error | Cause | Response |
|-------|-------|----------|
| HTTP 400 | Missing `isBot` | `{"errors":"isBot is a required parameter."}` |
| HTTP 500 | Missing `countryLanguage` | `findCategoryIdByCategoryUrlAndCountryLanguage(): Argument #2 ($countryLanguage) must be of type string, null given` |

#### Product Response Fields
| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Product ID |
| `title` | string | Product title |
| `description` | string | Product description |
| `brandName` | string | Brand name |
| `plpUrl` | string | Product listing page URL |
| `shopCount` | integer | Number of shops offering this product |
| `popularity` | integer | Popularity score (higher = more popular) |
| `type` | string | **Match type** - `result` or `orResult` |
| `minPrice` | float | Minimum price |
| `images` | array | Product images |

#### Product Type Field (Critical for Quality)
The `type` field indicates how well the product matches the search filters:

| Type | Meaning | Use Case |
|------|---------|----------|
| `result` | **Exact match** - Product matches ALL filters (correct brand, category, etc.) | **Include** in content |
| `orResult` | **Partial/related match** - Product is related but doesn't match all filters | **Exclude** from content |

**Example**: For URL `/products/.../c/merk~2829915` (brand filter):
- `type=result`: Product is from the specified brand ✓
- `type=orResult`: Product is from a different brand, included as fallback ✗

#### Product Filtering Rules (as of 2026-01-28)
```python
# Skip orResult products - only include exact matches
if product.get("type") == "orResult":
    continue

# Only include products with reliable availability
if shop_count >= 2:
    products.append(product)
```

**Filtering Logic**:
1. Skip all `orResult` products (only include `type="result"`)
2. Only include products with `shopCount >= 2`
3. If no products remain after filtering, URL is skipped

#### API Response Ordering
Products are returned sorted by `popularity` (descending). Higher popularity = shown first.

#### Code Locations
- **SEO Content**: `backend/scraper_service.py` (lines 533-557)
- **FAQ Content**: `backend/faq_service.py` (lines 466-500)
- **API Parameters**: `backend/scraper_service.py` (lines 325-335)

### Beslist Taxonomy API v2

**Base URL**: `http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl`
**Swagger**: `http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl/swagger/index.html`
**Auth**: JWT Bearer token required per spec, but **no auth needed from internal network**
**Spec file**: `scripts/swagger_taxv2.json`

**Purpose**: Manage product taxonomy — categories, facets, facet values, and their relationships. Used for updating SEO-relevant fields like `noIndexNoFollow` and `seoPriority`.

#### Key Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/Categories?rootCategoriesOnly=false` | List all 3,575 categories |
| `GET` | `/api/Categories/{id}` | Get category detail (includes subcategories, linked facets) |
| `GET` | `/api/Facets?searchTerm=...` | Search facets by name |
| `GET` | `/api/Facets/{id}` | Get facet detail (includes `noIndexNoFollow`) |
| `PUT` | `/api/Facets/{id}` | Update facet (`noIndexNoFollow`, `isEnabled`, etc.) |
| `GET` | `/api/Facets/{facetId}/values` | Get facet values (with `seoPriority`) |
| `PUT` | `/api/Facets/values/{facetValueId}` | Update facet value (`seoPriority`) |
| `GET` | `/api/CategoryFacets?categoryId=...` | Get linked facets for a category (with inheritance) |
| `GET` | `/api/CategoryFacetSettings?categoryId=...` | Get explicit settings per category |
| `GET` | `/api/CategoryFacetSettings/{categoryId}/{facetId}` | Get setting for specific combo |
| `PUT` | `/api/CategoryFacetSettings` | Upsert category-facet setting (`seoPriority`, `isHidden`, etc.) |

#### SEO-Relevant Fields

- **`noIndexNoFollow`** (boolean) — on `FacetDto`. Facet-wide: all values of this facet become noindex/nofollow.
  - Read: `GET /api/Facets/{id}` → `noIndexNoFollow`
  - Write: `PUT /api/Facets/{id}` with `UpdateFacetRequest`

- **`seoPriority`** (boolean, nullable) — exists at two levels:
  1. **Category-Facet level**: `CategoryFacetSettingDto` — priority for a facet within a specific category
     - Read: `GET /api/CategoryFacetSettings/{categoryId}/{facetId}`
     - Write: `PUT /api/CategoryFacetSettings` with `UpsertCategoryFacetSettingRequest` (`categoryId`, `facetId`, `seoPriority`)
  2. **Facet Value level**: `FacetValueDto` — priority per individual value
     - Read: `GET /api/Facets/{facetId}/values`
     - Write: `PUT /api/Facets/values/{facetValueId}` with `UpdateFacetValueRequest`
  - `null` means "inherit from parent", explicit `true`/`false` overrides

#### Data Model Notes
- Categories have nl-NL labels with `name` and `urlSlug` (e.g., name="Tuintafels", urlSlug="meubilair_389373_393687")
- Facets also have nl-NL labels (e.g., name="Kleur", urlSlug="kleur")
- As of 2026-03-17: `seoPriority` is not set anywhere in production (all `null`/inherit)
- `noIndexNoFollow` is set on some facets (e.g., facet 2906 "Kleur" has `noIndexNoFollow: false`)

---

## Future Architectural Considerations

### If Scale Increases (100+ users, 1M+ URLs):

1. **Job Queue**: Add Celery + Redis for distributed workers
2. **Caching**: Redis for rate limiting and result caching
3. **Load Balancing**: Multiple FastAPI instances behind nginx
4. **Database Sharding**: Split Redshift by date/category
5. **CDN**: Cloudflare for frontend static assets
6. **Monitoring**: Prometheus + Grafana for metrics
7. **Logging**: ELK stack for centralized logs

### Current Scale Targets (2025-01-22):
- **Users**: 1-10 concurrent
- **URLs**: 74,933 total (54,337 pending after sync)
- **Processed**: 59,763 URLs with content generated
- **Processing Speed**: 350-840 URLs/hour (3 workers)
- **Success Rate**: ~90% (9/10 URLs in recent batches)
- **Total Time**: 4-9 days for full dataset

---

## References

- **CC1 Documentation**: See `cc1/` directory for detailed learnings and patterns
- **Project Index**: See `cc1/PROJECT_INDEX.md` for file structure and endpoints
- **Learnings**: See `cc1/LEARNINGS.md` for troubleshooting and patterns
- **Main Instructions**: See `CLAUDE.md` for development workflow

---

_This architecture document is living documentation. Update when making significant architectural changes._
