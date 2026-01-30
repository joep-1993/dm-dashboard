# TASKS
_Active task tracking. Update when: starting work, completing tasks, finding blockers._

## Current Sprint
_Active tasks for immediate work_

## In Progress
_Tasks currently being worked on_

## Completed
_Finished tasks (move here when done)_

- [x] Run canonical REMOVEBUCKET transformation for 780 rules (30 facets, 13 categories) - transformed 7,778 URLs #claude-session:2026-01-30
- [x] Create R-finder tool - new frontend page + API endpoints to find /r/ redirect URLs from Redshift visits data #claude-session:2026-01-29
- [x] Standardize navigation headers across all frontend tools (consistent order, Dashboard button inverted at end) #claude-session:2026-01-29
- [x] Reset all failed/skipped URLs to pending (6,788 URLs) #claude-session:2026-01-28
- [x] Reset all merk URLs to pending for orResult filtering (45,600 archived, 51,401 removed from tracking) #claude-session:2026-01-28
- [x] Add orResult product filtering - skip type="orResult" products, only include type="result" exact matches #claude-session:2026-01-28
- [x] Change shopCount minimum from 3 to 2 for all products in scraper_service.py and faq_service.py #claude-session:2026-01-28
- [x] Add Product Search API documentation to docs/ARCHITECTURE.md (required params, type field, filtering) #claude-session:2026-01-28
- [x] Detect and reset 55,330 merk URLs with brand mismatches (19,888 missing brand name + 35,442 wrong brand links) #claude-session:2026-01-28
- [x] Create pa.merk_lookup table with 97,363 brand ID→name mappings from Excel #claude-session:2026-01-28
- [x] Change alert-info to alert-warning in app.js for consistent yellow styling across SEO Content Generator #claude-session:2026-01-20
- [x] Update publishing section in SEO Content Generator to match FAQ Generator (remove dry run, add content type selection, remove dev environment) #claude-session:2026-01-20
- [x] Remove conservative mode from Link Validation section in SEO Content Generator (HTML + JS) #claude-session:2026-01-20
- [x] Update publish function: remove dry run option, add content type selection (all/seo_only/faq_only), remove dev environment option #claude-session:2026-01-20
- [x] Add minimum 2 offers validation to link validator - PLPs with shopCount < 2 now treated as "gone" #claude-session:2026-01-20
- [x] Change content_faq format from HTML divs to JSON-LD schema with script tag wrapper (schema_org_to_script_tag function) #claude-session:2026-01-20
- [x] Fix production publish failures caused by case-insensitive duplicate URLs - added deduplication and removed 11 duplicate entries #claude-session:2026-01-20
- [x] Successfully publish 164,286 URLs to production in single 1GB payload #claude-session:2026-01-20
- [x] Configure OpenAI API key and Google Ads credentials in .env file #claude-session:2026-01-19
- [x] Reset 10,545 failed/skipped content URLs and 2,451 failed/skipped FAQ URLs to pending for reprocessing #claude-session:2026-01-19
- [x] Fix docker-compose mount path for thema_ads_optimized (../theme_ads/thema_ads_optimized) #claude-session:2026-01-19
- [x] Add content_bottom field to publishing - extracts FAQ Q&As with internal beslist.nl links, format: `<br /><strong>Question</strong><br>Answer<br>` with `<br />` between Q&A pairs for blank lines #claude-session:2026-01-19
- [x] Add batched publishing support to content_publisher.py - tested API limits on staging (max ~14,000 items / ~57MB per request), discovered Beslist API replaces table on each request (batching won't work without API changes) #claude-session:2026-01-19
- [x] Remove 1 URL containing /l/ from content_urls_joep table #claude-session:2026-01-19
- [x] Add content publishing feature with background task pattern - supports dev/staging/production environments, single-payload publishing (no batching), SQL sanitization for apostrophes ('' → ' → &#39;), 10-minute timeout for large payloads (~512MB) #claude-session:2026-01-15
- [x] Deduplicate content_urls_joep table (33,759 duplicates removed), add unique constraint on url column, copy 6,039 URLs from Redshift, reset 2,577 truncated content URLs to pending #claude-session:2026-01-15
- [x] Add facet_not_available error type to FAQ processor - distinguishes invalid facet/value API errors (400) from generic failures, includes invalid_facet details in response #claude-session:2025-12-26
- [x] Add PostgreSQL database service to docker-compose.yml with healthcheck - app now auto-starts db container with depends_on condition #claude-session:2025-12-24
- [x] Reset 11 FAQs with improper /p/ URLs (missing pim_id) to pending for regeneration #claude-session:2025-12-24
- [x] Fix FAQ processor to include URLs reset to pending (was only fetching URLs with no tracking entry, now also includes status='pending') #claude-session:2025-12-23
- [x] Fix FAQ status pending count to include URLs reset after validation (was only counting URLs with no tracking entry, now also includes status='pending') #claude-session:2025-12-23
- [x] Add FAQ link validator with Elasticsearch lookup, validation tracking table (pa.faq_validation_results), and frontend UI (Validate Links, Validate All, Reset Validation buttons) #claude-session:2025-12-23
- [x] Remove Redshift sync calls from main.py - system now uses PostgreSQL only for all operations #claude-session:2025-12-23
- [x] Remove 1,329 URLs containing /r/ from all database tables (faq_tracking, content_urls_joep, werkvoorraad, kopteksten_check) #claude-session:2025-12-23
- [x] Fix FAQ URL validation - remove fabricated URLs, only keep valid /p/ URLs from provided list, updated prompt examples #claude-session:2025-12-21
- [x] Filter product links to only include products with ≥2 offers (shopCount) in both FAQ and SEO content generators #claude-session:2025-12-21
- [x] Change FAQ hyperlinks to use product URLs (/p/) instead of category URLs (/c/) - deleted all 100K FAQs, reset to pending for regeneration #claude-session:2025-12-21
- [x] Change combined export to include ALL URLs (FULL OUTER JOIN) - URLs without content_top or content_faq now included with empty cells #claude-session:2025-12-21
- [x] Optimize FAQ generator performance - reuse OpenAI client, increase HTTP pool (1→10/20), remove sleep delay, increase max workers (10→20), batch DB inserts #claude-session:2025-12-18
- [x] Add content_bottom column to FAQ exports (XLSX and combined) - HTML formatted FAQs with bold questions and regular answers with hyperlinks #claude-session:2025-12-18
- [x] Fix FAQ hyperlinks to use full beslist.nl URLs instead of relative/localhost URLs - added post-processing and fixed 379 existing records #claude-session:2025-12-18
- [x] Standardize alert colors to yellow (alert-warning) across both tools #claude-session:2025-12-17
- [x] Update FAQ prompt to use informal Dutch tone ("jij"/"je" instead of "u"/"uw") #claude-session:2025-12-17
- [x] Fix content preview HTML truncation bug - strip HTML tags before truncating to prevent broken links in results list #claude-session:2025-12-17
- [x] Fix Product Search API to support URLs without /c/ filters - updated parse_beslist_url in both scraper_service.py and faq_service.py #claude-session:2025-12-17
- [x] Fix FAQ prompt to prevent fake URLs and generic link texts - added strict instructions to only use provided URLs, removed 32 problematic FAQ records #claude-session:2025-12-17
- [x] Standardize UI colors across tools - inline styles for badges (success=#198754, warning=#ffc107, danger=#dc3545), consistent alert-warning backgrounds #claude-session:2025-12-17
- [x] Switch link validator to PostgreSQL only - removed Redshift dependency from link_validator.py, all validation now uses local PostgreSQL #claude-session:2025-12-15
- [x] Add single-paragraph constraint to GPT prompt - updated gpt_service.py to require single continuous paragraph, reset 12,779 URLs with multiple paragraphs for regeneration #claude-session:2025-12-15
- [x] Fix validation 'moved to pending' not tracking URLs - URLs with gone products now properly added to werkvoorraad table for reprocessing #claude-session:2025-12-15
- [x] Recover orphaned URLs and fix data consistency - recovered 8,972 URLs from validation results + 56,666 content URLs not in werkvoorraad, total now 163,250 unique URLs #claude-session:2025-12-15
- [x] Fix export endpoint errors and switch to XLSX format - fixed created_at column missing, connection pool mismatch, changed CSV to XLSX with illegal character sanitization #claude-session:2025-12-15
- [x] Fix GPT content truncation at &amp entities - increased max_tokens from 500 to 1000, added truncation warning logging #claude-session:2025-12-15
- [x] Fix export data source mismatch - changed export to read from local PostgreSQL instead of Redshift (94K→177K rows) #claude-session:2025-12-15
- [x] Update MAIN_CATEGORY_IDS from maincat_ids_new.xlsx - replaced all mappings with correct values from authoritative source file #claude-session:2025-12-12
- [x] Optimize content generation speed - reduced API delay (0.1-0.2s → 0.02-0.05s), increased default workers (3 → 6), batch size (10 → 50) #claude-session:2025-12-12
- [x] Fix Total URLs count to show all unique URLs across werkvoorraad + content tables (not just werkvoorraad) #claude-session:2025-12-12
- [x] Switch to local PostgreSQL only - remove all Redshift dependencies from process-urls and status endpoints, content saved directly to local DB #claude-session:2025-12-11
- [x] Add "meubilair" (ID: 10) to MAIN_CATEGORY_IDS mapping in scraper_service.py - fixes API 400 errors for furniture URLs #claude-session:2025-12-11
- [x] Create import_missing_content.py script - imports CSV content to local PostgreSQL, converts relative URLs to absolute, updates tracking table #claude-session:2025-12-11
- [x] Fix double single quotes in content ('') → single quote (') - updated 3,594 records #claude-session:2025-12-11
- [x] Normalize URL formats across all tables - convert relative /products/ URLs to absolute https://www.beslist.nl/products/, remove /l/ format URLs #claude-session:2025-12-11
- [x] Sync tracking table with content table - add tracking entries for 25K+ URLs that had content but weren't tracked #claude-session:2025-12-11
- [x] Integrate Product Search API-based content generation into frontend SEO Content Generation - extracts selected facets (detailValue) to build product subjects (e.g., "Gele iPhone 15", "Nike Heren voetbalschoenen"), smart category name inclusion based on facet types #claude-session:2025-12-11
- [x] Add "Validate All" button to frontend link validation - validates ALL unvalidated URLs in single batch, uses LEFT JOIN with WHERE IS NULL for efficient filtering #claude-session:2025-12-11
- [x] Add urls_corrected count to link validation results display - shows how many URLs were auto-corrected vs moved to pending #claude-session:2025-12-11
- [x] Create seo_content_generator.py to generate SEO content from Product Search API using URL filters (parses /products/{maincat}/{category}/c/{filters}, fetches 30 products, generates GPT content with plpUrl links), outputs to Excel #claude-session:2025-12-10
- [x] Rewrite link_validator.py to use Elasticsearch plpUrl lookup instead of HTTP status checks, auto-correct outdated URLs in content, reset URLs with GONE products to pending (kopteksten=0), validate via local PostgreSQL #claude-session:2025-12-10
- [x] Create lookup_plp_urls.py script to query Elasticsearch API for plpUrl using pimId, supports both old URL format (/p/maincat_url/pimId/) and new format (/p/product-name/maincat_id/pimId/), batches of 10K, maincat mapping from CSV #claude-session:2025-12-09
- [x] Fix Redshift serialization conflict error (Error 1023) by replacing individual UPDATE loops with batch UPDATE operations using IN clauses #claude-session:2025-10-28
- [x] Fix async/threading deadlock causing batch processing to hang after first batch (converted endpoint to synchronous, replaced executemany with individual executes) #claude-session:2025-10-23
- [x] Fix URL filtering logic to use content table instead of tracking table (changed from pa.jvs_seo_werkvoorraad_kopteksten_check to pa.content_urls_joep for accurate filtering) #claude-session:2025-10-22
- [x] Fix data consistency issue between local content and Redshift flags (created sync_redshift_flags.py, synced 9,567 URLs with kopteksten=1) #claude-session:2025-10-22
- [x] Implement 503 detection with immediate batch stop (changed from 3 consecutive failures to immediate stop on first 503) #claude-session:2025-10-22
- [x] Fix batch size issue causing single-URL processing (changed local tracking query to filter ALL processed URLs, not just successful ones) #claude-session:2025-10-22
- [x] Implement three-state URL tracking system: kopteksten=0 (pending), =1 (has content), =2 (processed without content) for better analytics #claude-session:2025-10-22
- [x] Fix frontend batch processing showing NaN/undefined values (added default value handling with || operator in JavaScript) #claude-session:2025-10-22
- [x] Implement hidden 503 detection and auto-stop after 3 consecutive scraping failures (rate limit protection) #claude-session:2025-10-21
- [x] Reset 33,970 failed/skipped URLs back to pending state in batches (fixing false "no_products_found" from rate limiting) #claude-session:2025-10-21
- [x] Fix URL upload handling CSV format with relative URLs (convert /products/... to https://www.beslist.nl/products/..., Redshift-compatible batch checking) #claude-session:2025-10-21
- [x] Fix scraping failure handling: network errors (503, timeout, access denied) now keep URLs in pending for retry instead of marking as processed #claude-session:2025-10-21
- [x] Improve scraping error messages with specific HTTP status codes (403 Forbidden, 503 Service Unavailable, etc.) #claude-session:2025-10-21
- [x] Diagnose Docker network connectivity issue after restart (all external connections timing out, including ping/DNS) #claude-session:2025-10-21
- [x] Run one-time Redshift sync to fix already-processed URLs (synced 1,051 URLs, remaining: 52,779 truly unprocessed) #claude-session:2025-10-20
- [x] Fix critical bug: pending count not decreasing because skipped/failed URLs not updating Redshift kopteksten flag (causing infinite fetch loop) #claude-session:2025-10-20
- [x] Implement performance optimizations: connection pooling (30-50% faster), Redshift COPY command (20-30% faster), reduced OpenAI max_tokens (300→200), optimized URL fetching (3x→2x batch multiplier) #claude-session:2025-10-20
- [x] Fix Recent Results timestamps showing N/A by querying local PostgreSQL and conditionally hiding timestamps in frontend when unavailable #claude-session:2025-10-20
- [x] Add conservative mode to link validator (0.5-0.7s delay per link check, forced 1 worker, checkbox UI) #claude-session:2025-10-17
- [x] Create deduplication utility script removing 48,846 duplicate records (108,722→59,876 unique URLs) #claude-session:2025-10-17
- [x] Create werkvoorraad synchronization utility script updating 17,672 URLs from pending to processed #claude-session:2025-10-17
- [x] Fix date display showing "1-1-1970, 01:00:00" to show "N/A" when created_at is null #claude-session:2025-10-17
- [x] Update ARCHITECTURE.md with UI theme documentation (color codes, usage map, conservative mode) #claude-session:2025-10-17
- [x] Customize UI theme with brand colors (#059CDF blue, #9C3095 purple, #A0D168 green) using CSS custom properties #claude-session:2025-10-17
- [x] Add conservative mode option for cautious scraping (0.5-0.7s delay, forced 1 worker, checkbox UI) #claude-session:2025-10-17
- [x] Optimize scraper delay from 0.5-0.7s to 0.2-0.3s based on rate limit testing (2-3x speed improvement) #claude-session:2025-10-17
- [x] Conduct comprehensive rate limit testing showing NO rate limiting even at 0s delay with whitelisted IP (87.212.193.148) #claude-session:2025-10-17
- [x] Create comprehensive ARCHITECTURE.md documenting system design, technology choices, and architectural decisions for future reference #claude-session:2025-10-16
- [x] Update scraper user agent from generic Chrome UA to 'Beslist script voor SEO' for better traffic identification in server logs #claude-session:2025-10-16
- [x] Create /skip-permissions and /restore-permissions slash commands for quick permission mode toggling #claude-session:2025-10-16
- [x] Switch input table to pa.jvs_seo_werkvoorraad_shopping_season (updated all 6 references in backend/main.py, reset tracking table with 72,992 URLs ready for processing) #claude-session:2025-10-15
- [x] Optimize content generation performance (30-50% faster: 0.2-0.3s delay, lxml parser, 300 max_tokens, batched commits, executemany) #claude-session:2025-10-10
- [x] Fix URL filtering to allow failed/skipped URL retries (filter only successful, add ON CONFLICT handling) #claude-session:2025-10-10
- [x] Fix Recent Results font size issue (replace Bootstrap .small with explicit font-size) #claude-session:2025-10-10
- [x] Add manual URL input field to Upload URLs (textarea with uploadManualUrls function) #claude-session:2025-10-10
- [x] Configure VPN split tunneling to bypass scraper traffic to whitelisted IP (87.212.193.148) #claude-session:2025-10-10
- [x] Integrate Redshift for output tables (pa.jvs_seo_werkvoorraad, pa.content_urls_joep) with hybrid architecture #claude-session:2025-10-08
- [x] Clean up 1,903 URLs with numeric-only link text from Redshift, reset to pending #claude-session:2025-10-08
- [x] Remove batch size upper limit for link validation (batch_size: min 1, no max) #claude-session:2025-10-07
- [x] Remove batch size upper limit for SEO content generation (now unlimited) #claude-session:2025-10-07
- [x] Implement hyperlink validation feature with parallel processing (301/404 detection, auto-reset to pending) #claude-session:2025-10-07
- [x] Create CSV import script for pre-generated content (19,791 items imported) #claude-session:2025-10-07
- [x] Change frontend port from 8001 to 8003 (avoid port conflicts) #claude-session:2025-10-07
- [x] Reorganize frontend UI (Link Validation moved between SEO Generation and Status) #claude-session:2025-10-07
- [x] Optimize slow database queries in status endpoint (NOT IN → LEFT JOIN, add status index) #claude-session:2025-10-04
- [x] Fix CSV export formatting (UTF-8 encoding, newline removal, proper quoting) #claude-session:2025-10-04
- [x] Fix HTML rendering bug causing browser to auto-link HTML tags #claude-session:2025-10-04
- [x] Fix AI prompt to generate shorter hyperlink text #claude-session:2025-10-04
- [x] Display full URLs in frontend Recent Results #claude-session:2025-10-04
- [x] Add contract/collapse button for expanded content #claude-session:2025-10-04
- [x] Add parallel processing with configurable workers (1-10) #claude-session:2025-10-03
- [x] Add upload URLs functionality with duplicate detection #claude-session:2025-10-03
- [x] Add export functionality (CSV/JSON) #claude-session:2025-10-03
- [x] Add delete result and reset to pending functionality #claude-session:2025-10-03
- [x] Track skipped/failed URLs separately from pending #claude-session:2025-10-03
- [x] Add expandable full content view in Recent Results #claude-session:2025-10-03
- [x] Separate content_top and theme_ads into independent repositories #claude-session:2025-10-03
- [x] Create frontend interface on http://localhost:8001/static/index.html with batch processing #claude-session:2025-10-03
- [x] Add "Process All URLs" button with progress tracking and stop functionality #claude-session:2025-10-03
- [x] Clean backend/main.py to only include SEO content generation endpoints #claude-session:2025-10-03
- [x] Update docker-compose.yml to remove theme_ads dependencies #claude-session:2025-10-03
- [x] Update CLAUDE.md to reflect content_top as SEO-only project #claude-session:2025-10-03
- [x] Initialize project from template #claude-session:2025-09-30

## Blocked
_Tasks waiting on dependencies_

---

## Task Tags Guide
- `#priority:` high | medium | low
- `#estimate:` estimated time (5m, 1h, 2d)
- `#blocked-by:` what's blocking this task
- `#claude-session:` date when Claude worked on this
