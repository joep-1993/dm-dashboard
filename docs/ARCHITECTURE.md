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
1. **Input**: URLs loaded from `pa.kopteksten_jobs` (status='pending'), joined to the URL catalog `pa.urls` for the actual URL string
2. **Scraping**: Product Search API fetches product data (or web scraper with custom user agent)
3. **AI Generation**: OpenAI generates SEO-optimized content (100 words)
4. **Storage**: Content saved to `pa.kopteksten_content` (keyed on `url_id`), job state to `pa.kopteksten_jobs`
5. **Quality Control**: Link validation via Elasticsearch lookup, results in `pa.kopteksten_link_validation`

> **Schema note (2026-05-07)**: the per-tool URL-keyed tables (`pa.jvs_seo_werkvoorraad`, `pa.content_urls_joep`, `pa.faq_tracking`, etc.) were collapsed into a single `pa.urls` catalog plus per-tool `*_jobs` / `*_content` tables (FK on `url_id`). See [Database Architecture](#database-architecture) for the mapping and `cc1/LEARNINGS.md` ("Big Bang DB refactor") for the migration trail.

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
- **Single codebase, two deploys**: one FastAPI app runs both on the developer laptop (localhost:8003) and on the networked box (`win-htz-006.colo.beslist.net:3003`). Behavioral differences are env-driven — see "Environment-Gated Features" below
- **Two modes**: Docker (`docker-compose up`) or Docker-free (`run_local.sh` / venv + uvicorn). Docker-free is the standard path now
- **No build tools** - direct HTML/CSS/JS editing with auto-reload
- **Single-machine deployment** - designed for 1-10 users
- **Database**: Remote PostgreSQL at 10.1.32.9 (primary for both modes)
- **Windows auto-start**: Task Scheduler task "DM Tools Dashboard" runs `C:\Users\JoepvanSchagen\scripts\start-dm-dashboard.ps1` at logon — starts uvicorn via WSL, health-checks port 8003, closes window on success, stays open on error

### Repository Management (as of 2026-04-15)
- **Canonical repo**: `github.com/joep-1993/dm-dashboard` (git remote alias: `origin`)
- **Legacy repo**: `github.com/joep-1993/dm-tools` (alias `dm-tools-old`) — kept for reference until archived. Do NOT push to it
- **Working copy**: `/home/joepvanschagen/projects/dm-dashboard` (folder renamed from `dm-tools` on 2026-07-21 when the two local checkouts were consolidated into one; live :8003 also runs from here)
- **History of the consolidation**: dm-tools and dm-dashboard were two parallel repos with drifting copies. On 2026-04-15 they were unified: dm-tools absorbed all dashboard features behind env flags, then its history was force-pushed to dm-dashboard. Both remotes now hold identical history at commit `2414674`
- **Workflow**: always `git pull --rebase` before pushing; a second Claude instance on the networked box also has repo access

### Environment-Gated Features
Features that should only run in specific deploys are toggled via `.env`. All default to "off" so the local dev experience is minimal:
- `DASHBOARD_PASSWORD` — set to enable login middleware; unset disables auth entirely. Session cookie signed with `DASHBOARD_SECRET` (any stable random hex)
- `CORS_ORIGINS` — comma-separated allow-list; unset or `*` for permissive local dev
- `ENABLE_TASK_SCHEDULER=true` — mounts `task_scheduler_router` (Windows `schtasks`-based) and reveals the "Automation → Taakplanner" card on the dashboard frontpage via a `/api/config` feature-flag fetch
- `UNIQUE_TITLES_API_KEY` — was hardcoded, now env-driven (key still the same value)
- `BASE_URL`, `DISABLE_SSL_VERIFY`, `DASHBOARD_PASSWORD` are also read by `backend/daily_automation.py` so the same script can target localhost HTTP and networked self-signed HTTPS

Frontend reads `/api/config` on page load to decide which feature-flagged UI to reveal — no rebuild required since the frontend is vanilla JS + CDN Bootstrap

---

## Frontend Architecture

### Technology Stack
- **Framework**: None (Vanilla JavaScript)
- **UI Library**: Bootstrap 5 (via CDN)
- **Build Tools**: None
- **Layout**: All pages use `col-md-10 mx-auto` for consistent width. Input fields use `input-group` with inline label prefix. Buttons right-aligned via `d-flex justify-content-between`
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
    │       └──▶ Elasticsearch lookup on pimId / V4 id; "gone" verdict covers
    │           (1) not-in-ES, (2) shopCount < min_offers (default 2),
    │           (3) unparseable URL format, (4) V4 id miss
    │
    ├──▶ ai_titles_service.py (Unique Title Generation)
    │       ├──▶ DEFAULT pipeline (v1, generate_title_from_api):
    │       │     Product Search API → 5 dedup passes → strip brand/color/size →
    │       │     OpenAI full rewrite (11-rule prompt) → hallucination guard →
    │       │     reassemble → 5 dedup passes → save
    │       └──▶ EXPERIMENTAL (v3, generate_title_v3) — opt-in via
    │             AI_TITLES_PIPELINE=v3, currently shelved at ~76% acceptable:
    │             facets → deterministic builder (slot order: colour/merk/serie/
    │             productlijn/materials/adj/doelgroep/category/met/voor/combo/size) →
    │             OpenAI polish (5-rule prompt, no rewrite) → content+brand
    │             preservation guards → casing restore from composed → save.
    │             Pick-up notes in cc1/LEARNINGS.md.
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
    ├──▶ seo_rulings_service.py (SEO Sanity Checks)
    │       ├──▶ taxv2 isEnabled + 404 filter → cat sampler (main / sub / deepest)
    │       ├──▶ taxv2 CategoryFacetSettings/CategoryFacets → priority-facet probe
    │       ├──▶ pa.unique_titles_content → !!DISCOUNT!! / !!NR!! / !!JAAR!!
    │       │     placeholder-substitution probe (paths absolutized to www.beslist.nl)
    │       ├──▶ Slack chat.postMessage DM (reuses SLACK_BOT_TOKEN / SLACK_USER_ID)
    │       └──▶ Persists every run to pa.seo_rulings_runs (JSONB result) for
    │             page-load rehydration via GET /api/seo-rulings/last
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

#### 1c. Hyperlink Rules in FAQ + Kopteksten Prompts

**Decision**: Anchor text in generated content MUST be a product name or logical search term — never a vague demonstrative phrase.

**Why**: vague anchors ("klik hier", "deze link", "hier", "deze", "lees meer", "meer info", "kijk hier", "bekijk hier", "via deze link", "deze pagina", "deze gids", "ga naar") are bad for SEO (search engines weight anchor text as a relevance signal) and bad for UX (no preview of destination). Real example the user flagged: *"Dark Grey variant kun je hier klikken, en voor de 360 ml Dark Grey variant is er deze link."*

**Enforcement layers** (needed all three — plain prompt instructions alone don't hold):

1. **Explicit VERBODEN LINKTEKSTEN block-list** in every prompt — spells out each forbidden phrase so the model can pattern-match against the ban rather than interpret a vague rule.
2. **FOUT/GOED example pair** in each prompt — shows a literal wrong version and its rewrite. Anchors the model on what "good" looks like.
3. **Positive rule with escape hatch**: "linktekst MOET de productnaam of een logische zoekterm zijn — als dat niet natuurlijk past, maak dan GEEN hyperlink, herschrijf liever de zin zonder link." Without the escape hatch, the model forces a link and falls back to "hier" as the least-bad option.

**Prompt sites** (all 4 updated 2026-04-20):
- `backend/faq_service.py` — single-URL FAQ prompt.
- `backend/batch_api_service.py` — FAQ batch prompt + Kopteksten batch system message.
- `backend/gpt_service.py` — Kopteksten subcategory prompt + system message, main-category prompt + system message.

**Post-processing guard** (`backend/faq_service.py:clean_urls_in_answer`): after the model returns, every `<a>` tag's anchor text is normalised (lowercased, punctuation stripped) and matched against the `VAGUE_ANCHOR_TEXTS` set. Matches are unwrapped in place — tag removed, text kept. Belt-and-suspenders for the model's off-days. Currently only on the FAQ single-URL path; worth adding to batch FAQ + kopteksten paths if the problem recurs.

**Pending regeneration snapshot (2026-04-20)**: a DB scan turned up 1,280 FAQ rows + 274 kopteksten rows with vague anchors. All reset to pending so the next batch (post-prompt-fix) regenerates them with compliant anchors.

#### 1d. link_validator.py "gone" Classification

**What triggers a `gone` verdict on a product URL inside generated content**:

1. URL's `pimId` (or V4 `id`) is not found in Elasticsearch — the true "product was removed" case.
2. URL is found but `shopCount < min_offers` (default 2) — product exists with too few offers to be commercially useful.
3. URL's path can't be parsed into `(maincat_id, pimId)` via `extract_from_url` — e.g. truncated `/p/slug/` without maincat/pimId segments. Intentional: reprocessing will emit a fresh valid link.
4. V4 UUID URL where phase-1 `id`-based lookup misses.

**What does NOT trigger gone**: ES query exceptions (timeout, 500). The code explicitly drops the link from the result dict and logs `- skipping batch (not marking as gone)` so ES blips can't mass-false-positive-trigger reprocessing.

**Diagnostic pattern**: when interpreting "validator flagged N URLs as gone," always decompose N by classification rather than treating the total as monolithic. Sample the gone URLs back through ES (size=1 terms query on pimId/id, read `shopCount` + `plpUrl`) and bucket: `TRULY_GONE_not_in_ES`, `LOW_OFFERS_sc=X`, `UNRECOGNIZED_FORMAT`, `NO_PLPURL`. A 200-row sample on current production found 13/13 gone verdicts dominated by buckets 2+3, not 1 — useful context for "is the validator behaving" conversations.

#### 1e. URL Validator — Suggested URL Rebuild Rules

`backend/url_validator_service.py` validates beslist.nl category/facet URLs and, when it finds fixable issues, returns a `suggested_url` rebuilt from parsed components.

**URL blueprint** (order matters):
```
/products/{maincat}/{subcat}/r/{query}/c/{facet1_slug}~{value_id}~~{facet2_slug}~{value_id}
                              ^^^^^^^^^ ^^^^^^^^^^^^^^
                              optional  optional
```

**What `parse_beslist_url` captures into `ParsedUrl`**:
- `maincat_slug`, `subcat_slug` — the `/products/{maincat}/{subcat}/` segments.
- `r_query` — the `/r/{query}` bucket/search-term segment if present. **Preserved, not dropped** (earlier versions stripped this destructively, which broke suggestion rebuilding for any URL that had a `/r/` segment).
- `facets` — list of `(slug, value_id)` tuples from the `/c/` portion.
- `scheme`, `netloc`, `query_params`, `fragment` — preserved for the output.

**What `build_suggested_url` emits**:
1. Rebuilds path in blueprint order: `/products/` → maincat → subcat → `/r/{r_query}` (if present) → `/c/{facet1}~{v1}~~{facet2}~{v2}` (if facets).
2. Trailing slash: yes when `/c/` is absent (category-only or `/r/`-only URL), no when `/c/` is present (URL ends at the last facet value).
3. Deduplicates facets — keeps first occurrence of each slug (fixes `DUPLICATE_FACET`).
4. **Forces the whole output to lowercase** — scheme, netloc, path. Not per-segment: a single `path.lower()` + `scheme.lower() / netloc.lower()` pass after assembly. Per-segment lowercasing is a magnet for "forgot this field" bugs every time you add a component.
5. Returns `""` when the rebuilt URL exactly equals the input (no suggestion needed) or when any `_BLOCKER_CODE` issue fires (e.g. `CATEGORY_NOT_FOUND`, `FACET_NOT_LINKED` — the URL can't be safely fixed).

**Design rule for the parser**: when a parser serves both validation (wants normalised/simplified structure) and reconstruction (wants lossless input), capture every stripped segment into a named field rather than discarding it. Destructive normalisation is a one-way trip that breaks rebuilders downstream.

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

### Big Bang Refactor (2026-05-07) — single URL catalog

The SEO content tools used to each have their own URL-keyed tables. As of 2026-05-07 they share a single canonicalized URL catalog (`pa.urls`, ~980k rows) plus per-tool `*_jobs` / `*_content` tables that FK on `url_id` (BIGSERIAL). If you're debugging anything table-related, **start here** — see also `cc1/LEARNINGS.md` ("Big Bang DB refactor") for the full debugging guide.

#### Old → new table mapping

| Old table (renamed to `*_old_2026_05_07`) | New table | Notes |
|---|---|---|
| `pa.jvs_seo_werkvoorraad` | gone — see `pa.urls` + per-tool `*_jobs` | Universe concept now lives in `pa.urls`; eligibility per tool = "row in pa.<tool>_jobs" |
| `pa.jvs_seo_werkvoorraad_kopteksten_check` | `pa.kopteksten_jobs` | `(url_id, status, last_error, attempts, ...)` |
| `pa.content_urls_joep` | `pa.kopteksten_content` | `(url_id, content, ...)` |
| `pa.faq_tracking` | `pa.faq_jobs` | `(url_id, status, skip_reason, last_error, ...)` |
| `pa.faq_content` | `pa.faq_content_v2` | `_v2` suffix temporary; rename in step 5. faq_json/schema_org are TEXT (legacy literal newlines break strict JSONB) |
| `pa.unique_titles` (wide table) | `pa.unique_titles_jobs` + `pa.unique_titles_content` | URL-probe columns (status_code/final_url/checked_at) live on jobs as `http_status / final_url / last_checked_at`; `title_score` + `title_score_issue` on content |
| `pa.url_validation_tracking` | `pa.url_validation` | `is_valid=FALSE` means "skipped" (no products found / unreachable) |
| `pa.link_validation_results` | `pa.kopteksten_link_validation` | Same JSONB `broken_link_details`, now FK |
| `pa.faq_validation_results` | `pa.faq_link_validation` | New per-tool table |
| `pa.content_history` | UNCHANGED | Append-only audit log, still keyed on URL string |
| `pa.publish_log` | UNCHANGED | No URL column |
| `pa.jvs_seo_werkvoorraad_shopping_season` | UNCHANGED | Lives in Redshift, separate concern |

#### Catalog and helpers

```sql
-- Single source of truth, ~980k rows
CREATE TABLE pa.urls (
    url_id              BIGSERIAL PRIMARY KEY,
    url                 TEXT NOT NULL UNIQUE,    -- already canonicalized
    main_cat_name       TEXT,
    deepest_subcat_name TEXT,
    first_seen_at       TIMESTAMP NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMP,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    notes               TEXT
);
```

URLs are canonicalized at insert time using shared rules (strip protocol+host for Beslist, reject other hosts; strip query/fragment; trailing-slash rule by structure: `/c/` → no trailing slash, otherwise add one). Two implementations, both must agree:
- **Python**: `backend/url_catalog.py::canonicalize_url(s)` — used by every write path. Plus `get_url_id(cur, url, *, create=True)` for single-URL lookup with auto-insert, and `bulk_upsert_urls(cur, urls)` for batch inserts (returns `{canonical_url: url_id}`).
- **PL/pgSQL**: `pa.canonicalize_url(text)` — used in WHERE clauses (`WHERE u.url = pa.canonicalize_url(%s)` lookup pattern for endpoints that accept user-supplied URL variants).

#### Per-tool schema (typical shape)

```sql
CREATE TABLE pa.<tool>_jobs (
    url_id     BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    status     TEXT NOT NULL,           -- 'pending' / 'success' / 'failed' / 'skipped'
    last_error TEXT,
    attempts   INTEGER NOT NULL DEFAULT 0,
    -- (FAQ adds skip_reason; unique_titles adds http_status/final_url/last_checked_at)
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE pa.<tool>_content (
    url_id     BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    -- tool-specific columns (content / faq_json+schema_org / h1+title+description+...)
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE pa.url_validation (
    url_id          BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    last_checked_at TIMESTAMP NOT NULL DEFAULT now(),
    http_status     INTEGER,
    is_valid        BOOLEAN,             -- FALSE = skipped/unreachable
    reason          TEXT
);
```

Indexes: status/skip_reason on jobs tables (frequent filter), `created_at DESC` and `updated_at DESC` on jobs + content (for "recent results" panels), `is_valid` on url_validation. Every URL FK is the PK — joins are PK→PK lookups, fast.

#### Pending URL Calculation

```sql
-- Pending = "tool job is pending AND URL hasn't been validation-skipped"
SELECT u.url
FROM pa.kopteksten_jobs j
JOIN pa.urls u ON j.url_id = u.url_id
LEFT JOIN pa.url_validation v ON v.url_id = u.url_id
WHERE j.status = 'pending'
  AND (v.is_valid IS NULL OR v.is_valid = TRUE)
LIMIT %s
```

The `LEFT JOIN url_validation … WHERE is_valid IS NULL OR is_valid = TRUE` filter is critical — without it the FAQ batch picks up URLs known to have no products. Same pattern used for `/api/status`, `/api/faq/status`, the batch worker's URL-fetch.

**FAQ pending=0 quirk**: with this filter, FAQ commonly shows pending=0 even though `pa.faq_jobs` has many `status='pending'` rows. Reason: those URLs all have `is_valid=FALSE` rows from past `no_products_found` runs. This matches the OLD query's semantics (which excluded URLs in `url_validation_tracking`). Don't "fix" it.

#### Eligibility-backfill subtlety

The old `pa.jvs_seo_werkvoorraad` was the universe of URLs eligible for both Kopteksten AND FAQ. The new model requires explicit per-tool job rows. During step 2, every werkvoorraad URL not already in `pa.kopteksten_jobs` / `pa.faq_jobs` got a `status='pending'` row inserted — both job tables are exactly 390,022 rows after backfill (the canonical werkvoorraad universe preserved).

Going forward:
- `link_validator.add_urls_to_werkvoorraad(urls)` writes ONLY to `pa.kopteksten_jobs` (was previously the implicit shared eligibility marker via werkvoorraad).
- If a URL should also be picked up by FAQ, the caller must explicitly insert into `pa.faq_jobs` too.

#### Performance gotchas

`ORDER BY ... DESC LIMIT N` queries that join to `pa.urls`: **always rewrite as subquery-LIMIT-then-JOIN**. Sort the smaller content/jobs table first, then PK-lookup against `pa.urls`. The naive plan does a parallel hash join + top-N heapsort over 980k rows. ~25× speedup on `/api/status`, `/api/faq/status`, `get_recent_results`, `/api/validation-history`.

```sql
-- Slow:  parallel hash join + sort over 980k rows
SELECT u.url, c.content
FROM pa.kopteksten_content c
JOIN pa.urls u ON c.url_id = u.url_id
ORDER BY c.created_at DESC LIMIT 5;

-- Fast: sort 250k rows first, then 5 PK lookups
SELECT u.url, c.content
FROM (
    SELECT url_id, content, created_at
    FROM pa.kopteksten_content
    ORDER BY created_at DESC LIMIT 5
) c
JOIN pa.urls u ON c.url_id = u.url_id;
```

Same lesson for cross-table COUNTs: `COUNT(*) FROM pa.urls LEFT JOIN both content tables WHERE content IS NOT NULL` was 5.9s; `COUNT(DISTINCT url_id) FROM (UNION ALL of two content tables)` is 0.5s. ~12× speedup on `/api/content-publish/stats`.

After the migration always run `ANALYZE pa.<table>` on the new tables — without it the planner picks bad plans because it has no row-count statistics.

#### Migration trail

The full migration trail is in `dm-tools/migrations/2026-05-07-bigbang-step*.{sql,md}`:
- `step1-create-new-tables.sql` — additive, zero-risk
- `step2-backfill.sql` — UNION DISTINCT of all old URL columns into `pa.urls`, plus per-tool data backfill
- `step3a-unique-titles.md` — Unique Titles code refactor
- `step3a-fix-csv-imported-content.sql` — backfilled content for ~400k CSV-imported rows that had `ai_processed=FALSE` but content populated (legacy quirk)
- `step3b-faq-kopteksten.md` — FAQ + Kopteksten + content_publisher (bundled because of the cross-tool join)
- `step3c-perf-indexes.sql` — btree indexes on created_at/updated_at + ANALYZE
- `step4-rename-old-tables.sql` — rename to `*_old_2026_05_07` (the forcing function)
- step 5 (DROP TABLE) — not yet run, scheduled ~2026-05-14 after a one-week safety window

Rollback: step 4's SQL has a commented reverse (rename `*_old_2026_05_07` back). New tables stay populated independently; no data is lost.

### Other tables

- `thema_ads_jobs` / `thema_ads_job_items` / `thema_ads_input_data` — Google Ads job tracking (untouched by Big Bang)
- `pa.content_history` — content audit log (still URL-keyed; append-only)
- `pa.publish_log` — environment / payload tracking for publishes

### Database Connection Strategy

```python
# Connection pool connects to seo_tools_db via DATABASE_URL env var
def get_db_connection():          # Local PostgreSQL (primary)
def get_redshift_connection():    # Redshift (legacy, rarely used)
def get_output_connection():      # Routes to Redshift or PostgreSQL based on USE_REDSHIFT_OUTPUT
```

### Database Maintenance Utilities

**After Big Bang** most legacy maintenance scripts are obsolete:
- `backend/deduplicate_content.py` — no-op stub. New schema's `pa.kopteksten_content` is keyed on `url_id` (PK); duplicates are structurally impossible.
- `backend/migrate_shared_validation.py` — no-op stub. Replaced by step 2's URL catalog.
- `backend/sync_werkvoorraad.py` — still active, but for Redshift sync only. Local-side reads from `pa.kopteksten_content` and writes to `pa.kopteksten_jobs.status='success'`.

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
| `isBot` | `true` | **REQUIRED** - API returns 400 without this. Standardized to `true` across all callers to skip A/B experiments + personalisation for stable results. |
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
