# LEARNINGS
_Capture mistakes, solutions, and patterns. Update when: errors occur, bugs are fixed, patterns emerge._

## Database Connection Quick Reference

### Primary Database (used by dm-tools app)
- **Container**: `seo_tools_db`
- **Database**: `seo_tools`
- **User**: `postgres` / **Password**: `postgres`
- **Schema**: `pa`
- **Access**: `docker exec seo_tools_db psql -U postgres -d seo_tools -c "SELECT ..."`

### N8N Vector DB (COPY of data, used by n8n workflows only)
- **Host**: `10.1.32.9` (internal: `n8n-vector-db-rw.n8n.svc.cluster.local`)
- **Database**: `n8n-vector-db`
- **User**: `dbadmin` / **Password**: `Q9fGRKtUdvdtxsiCM12HeFe0Nki0PvmjZRFLZ9ArmlWdMnDQXX8SdxKnPniqGmq6`
- **Access**: `docker exec -e PGPASSWORD='...' seo_tools_db psql -h 10.1.32.9 -U dbadmin -d n8n-vector-db -c "SELECT ..."`

### Redshift (LEGACY - not actively used, USE_REDSHIFT_OUTPUT=false)
- **Credentials**: See `.env` file in dm-tools project

**IMPORTANT**: The dm-tools frontend/backend queries `seo_tools_db` ONLY. When debugging kopteksten issues, always check `seo_tools_db` first. The n8n vector DB is a copy and may be out of sync.

## AI Title Generation: Code-Level Facet Classification
- **Problem**: OpenAI (gpt-4o-mini) persistently adds "met" before sizes ("met Maat L", "met Grote maten") despite extensive prompt rules forbidding it. Prompt-only fixes failed after 5+ iterations.
- **Solution**: Moved facet handling from prompt rules to Python code preprocessing in `generate_title_from_api()`:
  1. **Size values stripped before AI**: Facets where `facet_name` starts with "maat", or `detail_value` is "Maat X"/"Grote maten"/"Kleine maten" are removed from the H1 and facet list before sending to the AI. Appended in code after AI response.
  2. **Met-feature pre-combination**: Feature values are pre-combined into a ready-made clause (e.g., "met korte mouwen, print en borstzak") and passed as an exact string for the AI to use.
  3. **Conditional met rule**: When no features exist, prompt says "Voeg NOOIT 'met' toe". When features exist, provides exact clause to copy.
  4. **Value-based classification** (not facet-name-based):
     - API `detail_value` starting with "met "/"zonder " → automatic met_values
     - Small hardcoded set of feature values needing "met" added: mouwen, capuchon, hals, rits, knopen, veters
     - Everything else → regular (adjective before product name)
  5. **Brand deduplication**: If Merk value appears inside another facet (e.g., Merk="Epson" + Productlijn="Epson EcoTank"), standalone brand facet is dropped
  6. **Color shade deduplication**: If both Kleur and Kleurtint* are present, base color dropped in favor of specific shade
  7. **Audience deduplication**: If general audience (Kinder/Baby) + specific (Meisjes/Jongens) both present, general is dropped
  8. **Hallucination removal**: Post-processing strips Heren/Dames/Kinderen/Nieuwe etc. from output if not present in input facets/title
  9. **Trailing "met" safety net**: Strips dangling " met" from AI output before size appending
- **Key lesson**: When LLM prompt rules fail repeatedly for a specific pattern, move that logic to deterministic code. Code-level preprocessing is 100% reliable vs prompt rules being probabilistic.
- **File**: `backend/ai_titles_service.py` — function `generate_title_from_api()`
- **Date**: 2026-02-11

## AI Title Prompt Engineering: Earlier Iterative Fixes
- **Issues fixed via prompt rules** (before code-level approach):
  1. **"met" for features**: FOUT/GOED examples + exception to allow "met"/"zonder"
  2. **"voor" with audiences**: Audiences before product name, "voor" forbidden
  3. **Hallucinated sizes**: Temperature 0.7→0.3, anti-hallucination wording
  4. **Conflicting rules**: Scoped "NOOIT" to specific prepositions
- **Lesson**: LLM prompts need FOUT/GOED examples for every edge case. Rules saying "NOOIT X" get over-generalized unless precisely scoped.
- **File**: `backend/ai_titles_service.py` — both `generate_ai_title()` (prompt 1) and `generate_title_from_api()` (prompt 2)
- **Date**: 2026-02-11

## Database Cleanup: German URLs and Garbage Data
- **Problem**: Databases contained ~210 German URLs (from beslist.de), ~112 garbage URLs (empty facet values, truncated names), and ~66 landing/theme pages
- **Detection methods**:
  1. German category paths: `möbel`, `schuhe`, `essen_getränke`, `haus_garten`, etc. (mojibake encoding: `Ã¶`=ö, `Ã¤`=ä)
  2. German facet names: `farbe`, `marke`, `zielgruppe`, `materialien`, `sportbekleidung`
  3. Cross-reference: Loaded 2,719 Dutch facet url_names from `facets_20260204.csv`, compared against facet names in pending URLs — remaining 84 mismatches were all Dutch variants/typos, no more German
  4. Garbage: URLs with empty facet values (`url ~ '/c/.*~($|~~|/)'`), `no-text` strings, leading spaces, `pricemax`/`pricemin`
- **Cleanup**: Deleted from all 5 tables (unique_titles, werkvoorraad, werkvoorraad_kopteksten_check, faq_tracking, faq_content)
- **Date**: 2026-02-11

## AI Titles: Stop Button and Scraping Fallback Removal
- **Stop button fix**: Changed `_run_processing()` from submitting all URLs to ThreadPoolExecutor at once to chunked processing (chunk_size = num_workers * 2). Stop flag checked between chunks for responsive stopping.
- **Scraping fallback removed**: `process_single_url()` now only uses productsearch API method. Returns `api_failed` error when API returns None, instead of falling back to scraping.
- **Error message improvement**: Last error now includes the failing URL: `f"{reason} ({url})"`
- **File**: `backend/ai_titles_service.py`
- **Date**: 2026-02-11

## Canonical Generator: Facet Sorting Bug with ~ Separator
- **Problem**: When two facets share a prefix (e.g., `kleur` and `kleurtint`), the URL sorted the longer facet first: `/c/kleurtint~17171868~~kleur~393175`
- **Root Cause**: `facets.sort()` sorted the full `facet~value` string. Since `~` (ASCII 126) > `t` (ASCII 116), `kleurtint~...` sorted before `kleur~...`
- **Fix**: Sort by facet name only (part before `~`): `facets.sort(key=lambda f: f.split("~")[0] if "~" in f else f)`
- **Result**: `kleur~393175~~kleurtint~17171868` (correct alphabetical order)
- **File**: `backend/canonical_service.py` — function `_sort_facets()`
- **Date**: 2026-02-10

## Canonical Generator: Filter No-Index URLs with "+"
- **Problem**: URLs containing `+` are no-index and should not appear in canonical generator results
- **Fix**: Added `AND dv.url NOT LIKE '%%+%%'` to the Redshift query in `fetch_urls_from_redshift()`
- **File**: `backend/canonical_service.py` — function `fetch_urls_from_redshift()`
- **Date**: 2026-02-10

## Facet Volume Processing: Batch Search Volumes for All Facet Values
- **What**: Process 140K+ facet values across 31 maincats, combining each facet with all deepest category names within its maincat, looking up search volumes via Google Ads API
- **SIC/SOD handling**: Facet values containing `<!-- SIC: X --><!-- SOD: Y -->` use SOD before category ("zwarte schoenen") and SIC after category ("schoenen zwart"). Plain facet values use the same text in both positions
- **Facet cleaning**: `clean_facet_value()` strips HTML comments (`<!-- ... -->`), normalizes whitespace. `parse_sic_sod()` extracts SIC/SOD/plain text
- **Keyword-to-row tracking**: Uses `keyword_to_rows` dict mapping each keyword combo to a set of facet row indices, allowing volume distribution back to source rows
- **Output**: Same columns as input CSV + `search_volume` column (grand total of all keyword combos for that facet across all deepest cats)
- **Resume capability**: `facets_progress.txt` tracks completed maincats; script skips them on restart
- **Scale**: ~81M keyword combinations, ~8,128 API batches, 35 customer IDs for quota rotation
- **Files**: `backend/category_keyword_service.py` (functions: `clean_facet_value`, `parse_sic_sod`, `process_facet_volumes`, `_normalize_keyword`), `backend/run_facet_volumes.py` (batch runner script)
- **Date**: 2026-02-10

## Google Ads API: gRPC ResourceExhausted vs GoogleAdsException
- **Problem**: Quota rotation wasn't working — rate limit errors (429) crashed entire maincats instead of rotating to next customer_id
- **Root Cause**: The code only caught `GoogleAdsException`, but 429 rate limits throw `google.api_core.exceptions.ResourceExhausted` (a gRPC-level exception), which is a different exception class
- **Fix**: Import `ResourceExhausted` from `google.api_core.exceptions` and catch it separately before `GoogleAdsException`. Also added catch-all `Exception` handler that checks for "resource exhausted" in message string
- **Key lesson**: Google Ads API errors come in two flavors: (1) API-level `GoogleAdsException` with `.failure.errors` list, and (2) gRPC-level exceptions like `ResourceExhausted` which bypass the `GoogleAdsException` handler entirely
- **File**: `backend/keyword_planner_service.py` — function `_query_search_volumes()`
- **Date**: 2026-02-10

## Redshift Connection Pool: SSL SYSCALL Error Fix
- **Problem**: "SSL SYSCALL error: EOF detected" in canonical generator when querying Redshift
- **Root Cause**: Stale connections in psycopg2 ThreadedConnectionPool — connections go idle, Redshift drops them, but the pool doesn't know
- **Fix**: (1) Added TCP keepalive settings to pool (keepalives=1, keepalives_idle=60, keepalives_interval=10, keepalives_count=5), (2) Added health check in `get_redshift_connection()` — runs `SELECT 1` before returning pooled connection, gets fresh one if stale
- **File**: `backend/database.py` — functions `_get_redshift_pool()`, `get_redshift_connection()`
- **Date**: 2026-02-10

## Category Keyword Volumes: Keyword + Category Combination Tool
- **What**: Combines a keyword (e.g., "nike") with all 3,535 deepest category names in both singular/plural forms and both word orders (4 combos per category: "nike schoenen", "schoenen nike", "nike schoen", "schoen nike")
- **Singular/plural forms**: Pre-computed and stored in `backend/category_forms.json` (3,564 entries including maincat names). Generated with Dutch heuristics: remove -en (with doubled consonant fix: "brillen"→"bril"), remove -s, handle -'s. Falls back to appending -en for assumed-singular words
- **Maincat entries**: Each unique maincat name is also combined with the keyword, stored as a deepest_cat row where `deepest_cat = maincat` and `cat_id = maincat_id`
- **Categories preloaded**: `backend/categories.xlsx` loaded at startup into `PRELOADED_CATEGORIES` (no file upload needed). 4 columns: maincat (A), maincat_id (B), deepest_cat (C), cat_id (D)
- **Output Excel**: Same as input + column E (search_volume_deepest_cat) + column F (search_volume_maincat)
- **API batch behavior**: ~14,200 keyword combinations → 2 batches of 10,000. Google Ads `GenerateKeywordHistoricalMetrics` may return slightly different rounded volumes when batch payload changes (adding/removing keywords from same request). This is NOT API variance (consecutive identical requests return identical results)
- **Files**: `backend/category_keyword_service.py` (service), `backend/category_forms.json` (pre-computed forms), `backend/categories.xlsx` (preloaded data), `frontend/keyword-planner.html` (UI section)
- **Endpoints**: `POST /api/keyword-planner/category-volumes` (JSON: `{"keyword": "nike"}`), `POST /api/keyword-planner/category-volumes/download` (JSON: `{"deepest_cat_results": [...]}`)
- **Date**: 2026-02-10

## Link Validator V4 UUID Lookup: Wildcard Queries Kill ES Performance
- **Problem**: V4 UUID plpUrl lookups used `wildcard` queries (`*V4_xxx*`) which caused constant 60s timeouts on Elasticsearch, making the "Validate All" feature extremely slow (~180K URLs taking hours)
- **Root Cause**: Leading wildcards (`*V4_xxx*`) force a full index scan in ES. With 20 parallel workers, each content item having 2-4 V4 links, hundreds of 60s timeouts cascade across batches
- **Fix (two-phase lookup)**:
  1. **Phase 1**: Fast `terms` query on `pimId` field with V4 UUIDs (instant, uses ES index)
  2. **Phase 2**: Wildcard disabled entirely — V4 URLs not found via pimId are skipped (not marked as gone). Wildcard queries on these ES indices always timeout and never return results anyway
- **Key lesson**: `result.get(key)` returns `None` for missing keys, which was then stored as `lookup_to_plp_url[key] = None` and interpreted as "product is GONE". Fix: only store results for keys actually present in the result dict (`if key in result: lookup_to_plp_url[key] = result[key]`), so unfound V4 URLs are skipped instead of falsely marked as gone
- **Impact**: Validator went from timing out/appearing stuck to completing 139K URLs in reasonable time
- **File**: `backend/link_validator.py` — function `query_elasticsearch_by_plpurl()` + callers in `lookup_plp_urls_for_content()` and `validate_faq_links()`
- **Date**: 2026-02-09 (supersedes 2026-02-08 wildcard fix)

## Detecting Cut-Off Content in Database
- **Problem**: Some generated content was cut off mid-sentence/mid-word (e.g., "om je h"), likely due to OpenAI token limits truncating the response
- **Detection query**: Strip HTML tags and check if text ends without sentence-ending punctuation:
  ```sql
  SELECT url FROM pa.content_urls_joep
  WHERE TRIM(regexp_replace(content, '<[^>]+>', '', 'g')) !~ '[.!?")'']$'
  ```
- **Scale**: Found 349 out of 179,949 content items (~0.2%) with cut-off content
- **Fix**: Back up to `content_history` (reason: `cut_off_content`), delete from `content_urls_joep`, add to `werkvoorraad` with `kopteksten=0` for regeneration
- **Date**: 2026-02-09

## Restoring Falsely Reset URLs: Don't Forget kopteksten_check
- **Problem**: After restoring content from `content_history` back to `content_urls_joep`, frontend status numbers didn't add up (processed + pending + skipped + failed > total)
- **Root Cause**: The validator deletes `kopteksten_check` entries when moving URLs to pending. Restoring content without re-adding `kopteksten_check` entries causes double-counting: URLs appear as both "processed" (have content) and "pending" (in werkvoorraad, no kopteksten_check entry)
- **Full restore checklist**:
  1. Restore content from `content_history` → `content_urls_joep`
  2. Re-add `kopteksten_check` entries with status `success`
  3. Clear `link_validation_results` for re-validation
  4. Optionally clean up `content_history` backup entries
- **Date**: 2026-02-09

## Keyword Planner Integration with Google Ads API
- **Purpose**: Look up Google Ads search volumes for keywords, with normalization that preserves traceability back to original keyword
- **Key pattern**: Build `cleaned_to_originals` mapping (e.g., `{"e bike": ["e-bike", "E-Bike"]}`) → query API with deduplicated cleaned keywords → map results back to originals
- **Normalization**: Replace `-` and `_` with spaces, remove special chars, lowercase, collapse whitespace (via `clean_keyword()`)
- **Quota management**: 35 hardcoded customer_ids with rotation on `RESOURCE_EXHAUSTED` + exponential backoff
- **API**: `GenerateKeywordHistoricalMetricsRequest` with geo=2528 (NL), language=1010 (Dutch), batch_size=10000
- **Google Ads credentials**: Already in `.env` / Docker container env vars (`GOOGLE_DEVELOPER_TOKEN`, `GOOGLE_REFRESH_TOKEN`, etc.)
- **Package**: `google-ads` v29.0.0 already installed in Docker container
- **Files**: `backend/keyword_planner_service.py` (service), `backend/main.py` (4 endpoints), `frontend/keyword-planner.html` (UI)
- **Date**: 2026-02-09

## Content Lookup URL Format Mismatch
- **Problem**: URL lookup function in dm-tools frontend returned "URL not found in content database" for URLs that existed
- **Root Cause**: `lookup_content()` in `main.py` normalized input to relative path (`/products/...`) but DB could store full URLs (`https://www.beslist.nl/products/...`) or vice versa
- **Fix**: Build both relative path and full URL variants, query with `WHERE url = %s OR url = %s`
- **File**: `backend/main.py` — endpoint `/api/content/lookup`
- **Date**: 2026-02-08

## Link Validator False Positives from ES Failures
- **Problem**: Validation run flagged 28,999 URLs as "gone products", but many products were actually valid (shopCount >= 2 in ES)
- **Root Cause**: Exception handlers in `lookup_plp_urls_for_content()` (line ~260) and FAQ validator (line ~570) set ALL products to `None` (gone) when an ES query fails:
  ```python
  except Exception as e:
      for pim_id in pim_ids:
          lookup_to_plp_url[pim_id] = None  # BUG: marks all as gone!
  ```
- **Fix**: On ES failure, skip the batch entirely instead of marking as gone. In the result builder, only include links whose `lookup_value` exists in `lookup_to_plp_url` — missing entries are simply omitted (not treated as gone)
- **Impact**: Re-validation after fix: 13,133 URLs (45%) were false positives and kept their content; 15,866 had genuinely gone products
- **Restore process**: Content backed up in `pa.content_history` → re-insert into `content_urls_joep`, set `kopteksten_check` to 'completed', set `werkvoorraad.kopteksten = 1`, clear `link_validation_results` entries, re-run validation
- **File**: `backend/link_validator.py` — both content validator and FAQ validator had the same bug
- **Date**: 2026-02-06

## Canonical Generator FACET+FACET Logic
- **Purpose**: Canonicalize URLs with multiple facets to URLs with fewer facets (remove redundant facet)
- **How it works**: Given old_facet (e.g. `merk~nike`) and new_facet (e.g. `productlijn~air-max`):
  1. Fetch URLs containing BOTH old_facet AND new_facet from Redshift (`contains_all` parameter)
  2. Remove the old_facet from the URL, keeping the new_facet
  3. Example: `beslist.nl/c/merk~nike~~productlijn~air-max` → `beslist.nl/c/productlijn~air-max`
- **Key**: Uses `contains_all` (multiple AND LIKE conditions) in SQL, not just a single `contains`
- **File**: `backend/canonical_service.py` — functions: `_apply_facet_facet()`, `fetch_urls_from_redshift()`, `fetch_urls_for_rules()`
- **Date**: 2026-02-06

## Stuck Pending URLs - Tracking Table Covers All Werkvoorraad URLs
- **Problem**: Frontend shows 0 pending URLs despite ~32K URLs not having content
- **Cause**: ALL URLs in `pa.jvs_seo_werkvoorraad` also exist in `pa.jvs_seo_werkvoorraad_kopteksten_check` (tracking table), even those with status='pending' that were never actually processed
- **Root Cause**: The pending calculation uses:
  ```sql
  SELECT COUNT(*) FROM pa.jvs_seo_werkvoorraad w
  LEFT JOIN pa.jvs_seo_werkvoorraad_kopteksten_check t ON w.url = t.url
  WHERE t.url IS NULL
  ```
  This returns 0 when every werkvoorraad URL has a corresponding tracking entry, regardless of status.
- **Symptoms**:
  - Frontend kopteksten status shows "Pending: 0"
  - Tracking table has entries with status='pending' that block the LEFT JOIN
  - Total URLs and content counts don't add up
- **Diagnosis**:
  ```sql
  -- Check if tracking table covers all werkvoorraad
  SELECT COUNT(*) FROM pa.jvs_seo_werkvoorraad;          -- e.g., 243,702
  SELECT COUNT(*) FROM pa.jvs_seo_werkvoorraad_kopteksten_check;  -- same number = problem

  -- Check tracking status breakdown
  SELECT status, COUNT(*) FROM pa.jvs_seo_werkvoorraad_kopteksten_check GROUP BY status;
  -- Look for 'pending' entries - these are blocking the pending count
  ```
- **Solution**: Delete the 'pending' (and optionally 'failed') entries from tracking table:
  ```sql
  DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check WHERE status = 'pending';
  -- Optionally also: DELETE ... WHERE status = 'failed';
  ```
- **Result**: URLs removed from tracking become truly pending and show up in frontend
- **Prevention**: This happens when URLs are bulk-loaded into both werkvoorraad AND tracking simultaneously. Only load URLs into werkvoorraad; the tracking table should only be populated by the processing workflow.
- **Date**: 2026-02-06

## DMA Script Tree Structure Reference

### Listing Group Tree Variants (campaign_processor.py)

**V2 tree (build_listing_tree_for_inclusion_v2)** — no CL1:
```
ROOT → CL3=shop_name(subdiv) → CL4=maincat_id(unit, positive) + CL4 OTHERS(negative)
     → CL3 OTHERS(negative)
```

**V1+CL1 tree (build_listing_tree_with_cl1)** — with CL1:
```
ROOT → CL3=shop_name(subdiv) → CL4=maincat_id(subdiv) → CL1=cl1(unit, positive) + CL1 OTHERS(negative)
                               → CL4 OTHERS(negative)
     → CL3 OTHERS(negative)
```

### Key Constraints
- **No UPDATE on listing groups** — only CREATE and REMOVE. To change a value (e.g. CL3 shop name), remove entire tree and rebuild.
- **SUBDIVISION requires OTHERS** — when creating a subdivision node, its OTHERS case MUST be in the same mutate operation.
- **Temporary resource names** — use `next_id()` to link nodes within the same mutate, then extract actual names from response for subsequent mutates.
- **Response index formula** — for `build_listing_tree_with_cl1` MUTATE 1: CL4 subdivision for maincat at index `i` is at `resp1.results[4 + i*2]`.

### Sheet Processing Functions
| Function | Sheet | Input | Purpose |
|----------|-------|-------|---------|
| `process_check_sheet` | "check" | shop_name, maincat_id, cl1 | Replace pipe-version CL3 exclusions via cat_ids lookup |
| `process_check_cl1_sheet` | "toevoegen" | shop_name, maincat, maincat_id, cl1 | Check and rebuild trees missing CL1 targeting |
| `process_check_new_sheet` | "check_new" | shop_name, ad_group_name, campaign_name | Replace pipe-version CL3 subdivision targeting directly |

- **Date**: 2026-02-06

## DMA Script Batch Processing Optimization
- **Purpose**: Optimize Google Ads campaign processing functions to reduce API calls by 90%+
- **Pattern**: Group shops by (maincat_id, custom_label_1) and process together instead of individually
- **Key Functions Optimized**:
  1. `process_reverse_exclusion_sheet()` - Removes shop exclusions (CL3) in batches
  2. `process_exclusion_sheet_v2()` - Adds shop exclusions (CL3) in batches
  3. `process_uitbreiding_sheet()` - Creates campaigns/ad groups, now finds campaign once per group
- **New Batch Functions**:
  - `reverse_exclusion_batch(client, customer_id, ad_group_id, ad_group_name, shop_names)` - Removes multiple shop exclusions in one API call
  - `add_shop_exclusions_batch(client, customer_id, ad_group_id, ad_group_name, shop_names)` - Adds multiple shop exclusions in one API call
- **How It Works**:
  1. Read sheet and group rows by (maincat_id, cl1) key
  2. For each group, look up deepest_cats from cat_ids sheet ONCE
  3. For each campaign/ad group, read listing tree ONCE for all shops
  4. Add/remove all exclusions in single batch mutate operation
- **Efficiency Gain**: ~318,468 API calls → ~28,152 API calls (91% reduction) for typical workload
- **File**: `/home/joepvanschagen/projects/dma_script/campaign_processor.py`
- **Test Files**: `test_reverse_exclusion_optimized.py`, `test_reverse_exclusion_integration.py`
- **Date**: 2026-02-04

## Excel UTF-8 Encoding Fix
- **Problem**: Excel shows garbled text like "KÃ¼ppersbusch" instead of "Küppersbusch"
- **Cause**: UTF-8 text was incorrectly decoded as Latin-1/Windows-1252
- **Solution**: Re-encode as Latin-1 then decode as UTF-8:
  ```python
  def fix_encoding(text):
      return str(text).encode('latin-1').decode('utf-8')
  ```
- **Use Case**: Fixed 635 entries in symbols.xlsx
- **Date**: 2026-02-04

## Efficient Bucket Performance Query Pattern
- **Problem**: Need to aggregate visits/revenue for 628K+ buckets without 628K queries
- **Solution**: Single query approach:
  1. Query ALL URLs with visits/revenue from Redshift (no bucket filtering)
  2. Extract bucket patterns from URLs using regex: `r'([a-zA-Z0-9_]+~\d+)'`
  3. Match to bucket set in Python (O(1) lookup per bucket)
  4. Aggregate results per bucket
- **Performance**: ~2-3 minutes for 628K buckets vs days with individual queries
- **Example Query**:
  ```sql
  SELECT SPLIT_PART(dv.url, '?', 1) as url, count(*) as visits,
         sum(fcv.cpc_revenue) + sum(fcv.ww_revenue) as revenue
  FROM datamart.fct_visits fcv
  JOIN datamart.dim_visit dv ON fcv.dim_visit_key = dv.dim_visit_key
  WHERE dv.url LIKE '%beslist.nl/products/%/c/%'
  GROUP BY 1
  ```
- **Date**: 2026-02-04

## User Preferences
- **Default Project**: When user says "the frontend" or "start the frontend" without specifying a project, always assume **dm-tools**
- **Frontend URL**: Always use http://localhost:8003/static/index.html (served by the backend via docker-compose, not a separate server)
- **Skip Permissions**: User can say "skip-permissions" mid-conversation to skip permission prompts (configured in `~/.claude/settings.json`)
- **Date**: 2026-01-27

## Recheck Skipped URLs Feature
- **Purpose**: Re-check URLs that were skipped during content/FAQ generation to see if products are now available
- **Reason for Skip**: URLs get status='skipped' with reason 'no_products_found' when scraper finds no products
- **How It Works**:
  1. Fetches URLs with `status='skipped'` that haven't been rechecked yet
  2. Re-scrapes each URL via Product Search API to check if products are now available
  3. If products found: removes URL from tracking table → gets picked up for content generation
  4. If still no products: marks as "rechecked" to avoid infinite loops
- **Tracking Tables**:
  - SEO: `pa.jvs_seo_werkvoorraad_kopteksten_check` (status='skipped', skip_reason NOT LIKE '%rechecked%')
  - FAQ: `pa.faq_tracking` (status='skipped', skip_reason NOT LIKE '%rechecked%')
- **API Endpoints**:
  - `POST /api/recheck-skipped-urls` - Recheck SEO skipped URLs
  - `POST /api/faq/recheck-skipped-urls` - Recheck FAQ skipped URLs
  - `DELETE /api/recheck-skipped-urls/reset` - Reset recheck markers to allow rechecking again
  - `DELETE /api/faq/recheck-skipped-urls/reset` - Reset FAQ recheck markers
- **Parameters**: `parallel_workers` (1-20), `batch_size` (configurable via UI)
- **UI**: "Recheck Skipped" button next to "Validate All" on both SEO and FAQ pages
- **Date**: 2026-02-01

## N8N Integration Setup
- **N8N Skills**: Installed 7 skills at `~/.claude/skills/` from [n8n-skills](https://github.com/czlonkowski/n8n-skills)
  - n8n-expression-syntax, n8n-mcp-tools-expert, n8n-workflow-patterns
  - n8n-validation-expert, n8n-node-configuration, n8n-code-javascript, n8n-code-python
- **N8N MCP Server**: Configured at `~/.claude/mcp.json`
  - URL: `https://n8n.aks.mgmt.beslist.nl`
  - Requires Node.js 22+ (upgraded via nvm)
  - Full path to npx: `/home/joepvanschagen/.nvm/versions/node/v22.22.0/bin/npx`
- **Date**: 2026-01-27

## Database Sync to N8N Vector DB
- **Purpose**: Copied all dm-tools tables to N8N's PostgreSQL for use in n8n workflows
- **Target Database**:
  - Host: `10.1.32.9` (internal: `n8n-vector-db-rw.n8n.svc.cluster.local`)
  - Database: `n8n-vector-db`
  - User: `dbadmin`
- **Tables Copied** (15 tables, ~2.3M rows):
  - `pa.content_urls_joep` (220K rows) - SEO content
  - `pa.faq_content` (241K rows) - FAQ content
  - `pa.unique_titles` (1M rows) - AI-generated titles
  - `pa.jvs_seo_werkvoorraad` - Work queue
  - `pa.link_validation_results`, `pa.faq_validation_results` - Validation results
  - `pa.content_history` - Content backup
  - Plus tracking tables and thema_ads tables
- **Script Pattern**: Used `psycopg2.extras.execute_values()` with JSONB handling via `Json()` wrapper
- **Date**: 2026-01-27

## CSV Encoding Fix for Excel
- **Problem**: Excel shows garbled characters like "CafetiÃ¨res" instead of "Cafetières"
- **Cause**: Excel defaults to Latin-1 encoding when opening CSV files without BOM
- **Solution**: Add UTF-8 BOM (Byte Order Mark) at start of file:
  ```bash
  printf '\xEF\xBB\xBF' > fixed.csv && cat original.csv >> fixed.csv
  ```
- **Verification**: `file` command should show "UTF-8 (with BOM) text"
- **Date**: 2026-01-27

## Canonical URL Generator
- **Purpose**: Replaces Google Apps Script + Google Sheets workflow for generating canonical URLs
- **Data Source**: Redshift (`datamart.fct_visits` + `datamart.dim_visit`) instead of GA4
- **Transformation Types**:
  - CAT-CAT: Replace category slug (e.g., `schoenen_430884` → `schoenen_430885`)
  - FACET-FACET: Replace facet value (e.g., `merk` → `populaire_serie`)
  - CAT+FACET: Change category for faceted URLs, keep facet
  - CAT+FACET1: Change category for faceted URLs, remove facet
  - BUCKET+BUCKET: Replace bucket value (e.g., `merk~23597985` → `populaire_serie~2590809`)
  - REMOVEBUCKET: Remove bucket from URL
- **Files**:
  - Backend: `backend/canonical_service.py`
  - Frontend: `frontend/canonical.html`
  - API endpoints in `backend/main.py`
- **API Endpoints**:
  - `POST /api/canonical/generate` - Generate canonicals
  - `POST /api/canonical/preview` - Preview affected URLs
  - `GET /api/canonical/fetch-urls` - Search Redshift URLs
  - `POST /api/canonical/transform` - Test single URL
- **Bug Fix**: Regex `re.sub(r'/+', '/', url)` also replaced `://` in URLs; fixed with `re.sub(r'(?<!:)//+', '/', url)`
- **Date**: 2026-01-27

## Redirect Checker Tool
- **Purpose**: Check HTTP status codes, redirect URLs, and canonical URLs for input URLs
- **User Agent**: `"Beslist script voor SEO"` (same as other scraper tools)
- **Base URL**: `https://www.beslist.nl` (for relative URL normalization)
- **Features**:
  - Check status codes (200, 301, 302, 404, etc.)
  - Detect redirect URLs (Location header from first request without following redirects)
  - Extract canonical URLs from HTML using regex
  - Parallel workers with rate limiting (token bucket algorithm)
  - Configurable timeout, workers (default: 20), and rate limit (default: 2 req/sec)
  - Click-to-copy for URL cells in results table
  - CSV and Excel export
  - Streaming progress updates via NDJSON
- **Files**:
  - CLI Script: `redirect_checker.py` (standalone, can process Excel/CSV files)
  - Frontend: `frontend/redirect-checker.html`
  - API endpoints in `backend/main.py`
- **API Endpoints**:
  - `POST /api/redirect-checker/check` - Check URLs (streaming NDJSON response)
  - `POST /api/redirect-checker/download` - Download results as Excel
- **Status Code Logic**: Shows initial status code (301/302) not final status (200) - captures the redirect before following it
- **URL Normalization**: Relative URLs starting with `/` are prefixed with `https://www.beslist.nl`
- **Date**: 2026-01-30

## R-Finder Tool
- **Purpose**: Find /r/ URLs from Redshift visits data (replaces Google Apps Script that queried GA4)
- **Data Source**: Redshift (`datamart.fct_visits` + `datamart.dim_visit`) - same as Canonical Generator
- **Features**:
  - Filter by multiple keywords (AND logic - URL must contain ALL filters)
  - Date range filtering (default: 2015-01-01 to today)
  - Minimum visits threshold
  - Copy-to-clipboard with tab-separated output for Excel
  - Relative URLs in output for easier copying
- **Exclusions** (same as original GA4 script):
  - `device=`, `/sitemap/`, `sortby=`, `/filters/`, `/page_`, `shop_id=`, `+`
  - Mismatched category combinations (e.g., `/cadeaus_gadgets_culinair/meubilair_`)
- **Files**:
  - Backend: `backend/rfinder_service.py`
  - Frontend: `frontend/rfinder.html`
  - API endpoints in `backend/main.py`
- **API Endpoints**:
  - `POST /api/rfinder/search` - Search for /r/ URLs with filters
  - `GET /api/rfinder/stats` - Get total URL/visits statistics
- **Troubleshooting**: URLs with all filter terms may have very few visits - check all-time data if recent date range returns 0 results
- **Date**: 2026-01-29

## Link Validator V4 UUID Support
- **Problem**: Product URLs with V4 UUID format were incorrectly marked as "gone" during link validation
- **URL Formats Supported**:
  1. Old: `/p/gezond_mooi/nl-nl-gold-6150802976981/`
  2. New numeric: `/p/product-name/286/6150802976981/`
  3. V4 UUID: `/p/product-name/137/V4_2f09146b-402b-48d0-b966-655e1416a43d/`
- **Cause**: `extract_from_url()` only checked `potential_pim_id.isdigit()`, which returned False for V4 UUIDs
- **Solution**: Added explicit check for `potential_pim_id.startswith('V4_')` before the numeric check
- **Impact**: Both SEO content validation and FAQ validation use the same `extract_from_url()` function
- **Location**: `backend/link_validator.py` - `extract_from_url()`
- **Date**: 2026-01-24

## Performance Optimizations
- **Connection Pools**:
  - PostgreSQL: `maxconn` increased from 10 → 20
  - Redshift: `maxconn` increased from 5 → 10
  - Scraper HTTP pool: `pool_connections` and `pool_maxsize` increased from 1 → 10
- **Verbose Logging Removed**: Connection pool logging was causing I/O overhead on every connection
- **Combined Status Queries**: Status endpoint reduced from 5 separate COUNT queries to 1 combined query
- **Batched DB Updates**: Link validation now uses `executemany()` instead of individual UPDATE loops
- **Database Indexes Added**:
  - `idx_content_urls_url` (UNIQUE) on `pa.content_urls_joep(url)`
  - `idx_werkvoorraad_check_url` on `pa.jvs_seo_werkvoorraad_kopteksten_check(url)`
  - `idx_werkvoorraad_check_status` on `pa.jvs_seo_werkvoorraad_kopteksten_check(status)`
  - `idx_link_validation_content_url` on `pa.link_validation_results(content_url)`
- **Location**: `backend/database.py`, `backend/main.py`, `backend/scraper_service.py`
- **Date**: 2026-01-24

## GPT URL Truncation Fix
- **Problem**: GPT sometimes truncates product URLs in generated content
  - Full URL: `https://www.beslist.nl/p/product-name/452/8718969401258/`
  - Truncated: `https://www.beslist.nl/p/product-name/` (missing maincat_id and pimId)
- **Impact**: Found 6,486 content items with 22,317 truncated/broken links
- **Solution**: Added `fix_truncated_urls()` function in `backend/gpt_service.py`
  - Builds mapping of product-name slugs to full URLs from original product list
  - Finds truncated URLs in GPT output using regex
  - Replaces them with correct full URLs
  - Logs how many URLs were fixed
- **When Applied**: Automatically runs after GPT generates content in `generate_product_content()`
- **Location**: `backend/gpt_service.py` - `fix_truncated_urls()`
- **Date**: 2026-01-25

## Content History Backup Table
- **Purpose**: Backup content before deletion during link validation or resets
- **Table**: `pa.content_history`
- **Columns**:
  - `url` (TEXT) - The category URL
  - `content` (TEXT) - The backed up content
  - `reset_reason` (TEXT) - Why it was reset (e.g., 'gone_products', 'truncated_urls')
  - `reset_details` (JSONB) - Additional details (e.g., list of gone URLs)
  - `original_created_at` (TIMESTAMP) - When the content was originally created
  - `reset_at` (TIMESTAMP) - When the backup was made
- **Used By**: Link validator, manual resets
- **Location**: `backend/database.py`, `backend/main.py`
- **Date**: 2026-01-25

## Minimum Offer Count for Content Generation
- **Change**: Increased minimum shopCount from 2 to 3 for including product links
- **Applies To**:
  - SEO Content: `backend/scraper_service.py:548` - `shop_count >= 3`
  - FAQ Content: `backend/faq_service.py:485` - `shop_count >= 3`
- **Does NOT Apply To**:
  - Link Validator: Still uses `min_offers=2` (existing content not affected)
- **Reason**: Ensures better quality PLPs are linked in generated content
- **Date**: 2026-01-25

## AI Title Generation Service
- **Purpose**: Generates SEO-optimized titles using productsearch API + OpenAI
- **Location**: `backend/ai_titles_service.py`
- **Frontend**: Unique Titles Manager (`/static/unique-titles.html`)
- **API Endpoints**:
  - `GET /api/ai-titles/status` - Get processing status and stats
  - `POST /api/ai-titles/start?batch_size=100&num_workers=15` - Start AI title generation
  - `POST /api/ai-titles/stop` - Stop processing
  - `GET /api/ai-titles/recent` - Get recently processed titles
- **Processing Method**:
  - **Faceted URLs** (containing `~~` or `/c/`): Uses productsearch API to get H1 and facet data, then OpenAI improves it
  - **Non-faceted URLs**: Falls back to scraping + OpenAI
  - **Workers**: Configurable parallel workers (default 15), each with 0.5s delay (max 2 URLs/worker/sec)
- **Database columns added to `pa.unique_titles`**:
  - `ai_processed` (BOOLEAN) - Whether URL has been processed
  - `ai_processed_at` (TIMESTAMP) - When it was processed
  - `ai_error` (TEXT) - Error message if failed
  - `original_h1` (TEXT) - Original H1 before AI rewrite
- **Generated Content**:
  - **H1**: AI-improved title from API (e.g., "FRESK groene RVS BPA vrij waterflessen")
  - **Title**: `{H1} kopen? ✔️ Tot !!DISCOUNT!! korting! | beslist.nl`
  - **Description**: `Zoek je {H1}? &#10062; Vergelijk !!NR!! aanbiedingen en bespaar op je aankoop &#10062; Shop {H1} met !!DISCOUNT!! korting online! &#10062; beslist.nl`
- **OpenAI Prompt Rules**:
  - Facet values must stay intact (e.g., "Rode Duivels" is one theme, not split)
  - Brand always first (e.g., "Apple iPhones" not "iPhones van Apple")
  - ALWAYS use adjectives for colors/materials (e.g., "Houten bank" not "bank van hout")
  - NEVER use "in" or "van" for colors/materials
- **Post-processing**:
  - `format_dimensions()`: "31 cm 115 cm" → "31 cm x 115 cm"
  - `normalize_preposition_case()`: Lowercase prepositions unless at start of sentence
  - **Lowercase words**: met, in, zonder, van, voor, tot, op, aan, uit, bij, naar, over, onder, tegen, tussen, door, om, en, of
- **Date**: 2026-01-23

## Docker Commands
```bash
# Development
docker-compose up              # Run with logs
docker-compose up -d           # Run in background
docker-compose logs -f app     # View app logs
docker-compose down            # Stop everything
docker-compose down -v         # Stop and remove volumes

# Debugging
docker-compose ps              # Check status
docker exec -it <container> bash  # Enter container

# Import data from local file into container
docker cp /path/to/file container_name:/tmp/file
docker-compose exec -T db psql -U postgres -d dbname -c "COPY table (column) FROM '/tmp/file';"

# CSV import from Windows to container
docker cp /path/to/file.csv content_top_app:/app/file.csv
docker-compose exec app python -m backend.import_content

# Access Frontend
# Navigate to http://localhost:8003/static/index.html
```

## Common Issues & Solutions

### Beslist Product Search API Facet Validation Errors
- **Problem**: FAQ processor API calls fail with HTTP 400 for certain URLs
- **Cause**: URLs contain facet names or value IDs that are no longer valid for that category
- **Error Response Types**:
  - Invalid facet name: `{"context": "facet", "errorInfo": "The given facet is not valid.", "value": "personage"}`
  - Invalid facet value: `{"context": "merk", "errorInfo": "The given facet value is not valid.", "value": 19957206}`
- **Solution**: Detect 400 errors with "not valid" in errorInfo, return `facet_not_available` error type instead of generic `api_failed`
- **FAQ Processor Error Reasons Reference**:
  - `facet_not_available` - URL contains invalid facet name or value ID for category
  - `api_failed` - Generic API failure (non-400 or unparseable error)
  - `no_products_found` - API returned 0 products (skipped)
  - `faq_generation_failed` - OpenAI generation failed
- **Location**: backend/faq_service.py - `fetch_products_api()` and `process_single_url_faq()`
- **Date**: 2025-12-26

### External API SQL Escaping Errors
- **Problem**: Website-configuration API returns MySQL INSERT exception when content contains apostrophes
- **Error**: `An exception occurred while executing 'INSERT INTO ... VALUES ('...DVD's...')`
- **Cause**: External API's MySQL INSERT not properly escaping single quotes in content
- **Additional Issue**: Legacy content contains double single quotes (`''`) which need normalization
- **Solution**: Sanitize content before sending to external API:
```python
def sanitize_for_api(text: str) -> str:
    if not text:
        return ""
    # First normalize double single quotes to single (legacy data issue)
    text = text.replace("''", "'")
    # Then replace single quotes with HTML entity
    return text.replace("'", "&#39;")
```
- **Location**: backend/content_publisher.py - `sanitize_for_api()`
- **Date**: 2026-01-15

### Background Task Pattern for Long-Running Operations
- **Problem**: Browser times out when API operations take >30 seconds (e.g., publishing 162K+ URLs)
- **Solution**: Use threading with task_id polling pattern:
  1. Start background thread with unique task_id
  2. Return task_id immediately to client
  3. Client polls status endpoint every 2 seconds
  4. Background thread updates shared dict with progress/result
```python
_tasks = {}
_task_lock = threading.Lock()

def start_task(params) -> str:
    task_id = str(uuid.uuid4())[:8]
    with _task_lock:
        _tasks[task_id] = {"status": "pending", ...}
    thread = threading.Thread(target=_run_task, args=(task_id, params))
    thread.daemon = True
    thread.start()
    return task_id

def get_task_status(task_id: str) -> Dict:
    with _task_lock:
        return _tasks.get(task_id, {"error": "Task not found"})
```
- **Location**: backend/content_publisher.py - `start_publish_task()`, `get_publish_task_status()`
- **Date**: 2026-01-15

### Orphaned URLs After Content Deletion
- **Problem**: URLs become "lost" when content is deleted (validation/regeneration) but URL not in werkvoorraad
- **Symptoms**: Total URL count drops, URLs cannot be reprocessed
- **Cause**: Content deleted from `content_urls_joep` but URL never existed in `jvs_seo_werkvoorraad`
- **Solution**: When deleting content, always ensure URLs are added to werkvoorraad:
```python
# After deleting content, add URLs to werkvoorraad for reprocessing
for url in deleted_urls:
    cur.execute("""
        INSERT INTO pa.jvs_seo_werkvoorraad (url, kopteksten)
        VALUES (%s, 0)
        ON CONFLICT (url) DO UPDATE SET kopteksten = 0
    """, (url,))
```
- **Recovery**: Check for orphaned URLs in validation_results or tracking tables and add to werkvoorraad
- **Location**: backend/main.py - validate-links and validate-all-links endpoints
- **Date**: 2025-12-15

### GPT Generating Multiple Paragraphs
- **Problem**: Generated content contains `\n\n` (double newlines) creating multiple paragraphs
- **Solution**: Add explicit instruction to BOTH system message and user prompt:
  - System: `"Schrijf ALTIJD als één doorlopende alinea zonder witregels of meerdere paragrafen."`
  - User: `"Schrijf de tekst als EEN doorlopende alinea, GEEN meerdere paragrafen of witregels."`
- **Location**: backend/gpt_service.py
- **Date**: 2025-12-15

### Connection Pool Mismatch in Export Functions
- **Error**: `trying to put unkeyed connection`
- **Cause**: Using `get_output_connection()` but returning with `return_db_connection()` - wrong pool
- **Solution**: Always use matching return function for connection type:
  - `get_db_connection()` → `return_db_connection()`
  - `get_output_connection()` → `return_output_connection()`
  - `get_redshift_connection()` → `return_redshift_connection()`
- **Location**: backend/main.py - export endpoints
- **Date**: 2025-12-15

### Illegal Characters in Excel Export (openpyxl)
- **Error**: `\u0011 cannot be used in worksheets`
- **Cause**: Content contains control characters that Excel doesn't allow
- **Solution**: Sanitize content before writing to Excel worksheet:
```python
import re
# Remove control characters except tab, newline, carriage return
illegal_chars = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')
content = illegal_chars.sub('', content)
```
- **Location**: backend/main.py - `/api/export/xlsx` endpoint
- **Date**: 2025-12-15

### GPT Content Truncation Mid-Entity
- **Problem**: Generated content cut off mid-HTML entity (e.g., `&amp` without `;`)
- **Cause**: OpenAI `max_tokens` limit reached before response completed
- **Symptoms**: Content ends with incomplete entities like `&amp`, `&quot`, `&#123`
- **Solution**:
  1. Increase `max_tokens` (500 → 1000 for ~100 word content with HTML)
  2. Check `finish_reason` to detect truncation:
```python
response = client.chat.completions.create(...)
if response.choices[0].finish_reason == "length":
    print(f"Warning: Response was truncated")
```
- **Location**: backend/gpt_service.py (line 89)
- **Date**: 2025-12-15

### Redshift Serializable Isolation Violation (Error 1023)
- **Error**: `Error: 1023 DETAIL: Serializable isolation violation on table - 37521601, transactions forming the cycle are: 573354047, 573354048, 573354046 (pid:1073775083)`
- **Cause**: Multiple concurrent batch jobs updating the same Redshift table (`pa.jvs_seo_werkvoorraad_shopping_season`) with individual UPDATE statements in loops
- **Symptoms**:
  - Frontend shows "Error: 1023 DETAIL: Serializable isolation violation"
  - Occurs when running multiple concurrent batches
  - Transaction cycles formed by N individual UPDATEs competing for same rows
- **Root Cause**: Individual UPDATE per URL in loop creates many small transactions:
  ```python
  # ❌ Wrong: Individual UPDATEs cause serialization conflicts
  for (url,) in update_werkvoorraad_success_urls:
      output_cur.execute("""
          UPDATE pa.jvs_seo_werkvoorraad_shopping_season
          SET kopteksten = 1
          WHERE url = %s
      """, (url,))
  ```
- **Solution**: Replace with batch UPDATE operations using IN clauses:
  ```python
  # ✅ Correct: Single batch UPDATE prevents conflicts
  if update_werkvoorraad_success_urls:
      url_list = [url for (url,) in update_werkvoorraad_success_urls]
      placeholders = ','.join(['%s'] * len(url_list))
      output_cur.execute(f"""
          UPDATE pa.jvs_seo_werkvoorraad_shopping_season
          SET kopteksten = 1
          WHERE url IN ({placeholders})
      """, url_list)
  ```
- **Impact**:
  - Eliminates serialization conflicts in concurrent batch processing
  - Reduces transaction count from N individual UPDATEs to 1 batch UPDATE
  - Shorter transaction time reduces collision window
  - 15-20% throughput improvement (fewer round-trips to Redshift)
- **Locations Fixed**:
  - backend/main.py:295-317 (batch processing endpoint - success and processed updates)
  - backend/main.py:770-791 (link validation endpoint - broken link resets)
- **Date**: 2025-10-28

### Docker/WSL Integration
- **Error**: `docker-compose: command not found` in WSL 2
- **Cause**: Docker Desktop WSL integration not enabled
- **Solution**: Enable WSL integration in Docker Desktop settings
  - Open Docker Desktop → Settings → Resources → WSL Integration
  - Enable integration for your WSL distro
  - Restart WSL terminal
- **Documentation**: https://docs.docker.com/go/wsl2/

### FastAPI Async Endpoints with psycopg2 ThreadedConnectionPool
- **Problem**: API endpoint hangs indefinitely at `get_output_connection()` call. The async event loop was blocked by synchronous `getconn()` from ThreadedConnectionPool
- **Symptoms**:
  - First batch processes successfully
  - Second batch hangs forever at database connection
  - Logs show "[ENDPOINT] Getting output connection..." but never reach "[POOL] get_output_connection() called"
  - No errors, no timeouts - just infinite hang
- **Root Cause**: FastAPI async endpoint calling synchronous blocking psycopg2 pool operations
  - `async def` endpoint uses asyncio event loop
  - `pool.getconn()` is synchronous and blocks
  - Blocking the event loop prevents any async operations from completing
  - Even `await loop.run_in_executor()` doesn't fully solve it due to connection pool thread safety
- **Solution**: Convert endpoint from `async def` to `def` (synchronous)
```python
# ❌ Wrong: Async endpoint with sync database pool
@app.post("/api/process-urls")
async def process_urls():
    conn = get_output_connection()  # Blocks event loop!

# ✅ Correct: Synchronous endpoint
@app.post("/api/process-urls")
def process_urls():
    conn = get_output_connection()  # No event loop blocking
```
- **Alternative**: Use async-compatible driver (asyncpg) if async is required, but adds complexity
- **Impact**: Immediate fix - endpoint processes multiple batches successfully
- **Location**: backend/main.py (line 181), backend/database.py (connection pool functions)
- **Date**: 2025-10-23

### Redshift executemany() Blocking Indefinitely
- **Problem**: Second API request hangs at "[POOL] Getting Redshift connection..." - connection pool exhaustion
- **Symptoms**:
  - First request succeeds and completes
  - Second request waits forever for a Redshift connection
  - Logs show "Got Redshift connection" but never "Returned Redshift connection"
  - Connection pool exhausted (maxconn=5, all connections stuck)
- **Root Cause**: Redshift doesn't handle `executemany()` well with INSERT/UPDATE statements
  - Batch operations block indefinitely
  - Connection never completes transaction
  - Connection never returned to pool
  - Subsequent requests wait forever for available connection
- **Solution**: Replace all `executemany()` calls with individual `execute()` loops
```python
# ❌ Wrong: Blocks indefinitely on Redshift
if insert_content_data:
    output_cur.executemany("""
        INSERT INTO pa.content_urls_joep (url, content)
        VALUES (%s, %s)
    """, insert_content_data)

# ✅ Correct: Individual executes
if insert_content_data:
    print(f"[ENDPOINT] Inserting {len(insert_content_data)} content records...")
    for url, content in insert_content_data:
        output_cur.execute("""
            INSERT INTO pa.content_urls_joep (url, content)
            VALUES (%s, %s)
        """, (url, content))
    print(f"[ENDPOINT] Content inserts complete")
```
- **Performance**: Slightly slower than executemany() but actually completes (vs hanging forever)
- **Note**: executemany() works fine on PostgreSQL, only Redshift has this issue
- **Testing**: Verified 3 sequential requests complete successfully after fix
- **Location**: backend/main.py (lines 286-315)
- **Date**: 2025-10-23

### Redshift SQL Differences - ON CONFLICT Not Supported
- **Problem**: URL upload fails with syntax error: "syntax error at or near 'ON'"
- **Cause**: PostgreSQL's `ON CONFLICT DO NOTHING` syntax not supported by Redshift
- **Impact**: Cannot use INSERT ... ON CONFLICT for duplicate handling in Redshift
- **Solution**: Use batch checking strategy instead:
  1. Query existing URLs with WHERE IN (batches of 500)
  2. Filter duplicates in Python using set difference
  3. Batch insert only new URLs with executemany()
- **Example**:
```python
# Get existing URLs in batches
existing_urls = set()
batch_size = 500
for i in range(0, len(urls), batch_size):
    batch = urls[i:i + batch_size]
    placeholders = ','.join(['%s'] * len(batch))
    cur.execute(f"SELECT url FROM table WHERE url IN ({placeholders})", batch)
    existing_urls.update(row['url'] for row in cur.fetchall())

# Filter and insert new URLs
new_urls = [(url,) for url in urls if url not in existing_urls]
cur.executemany("INSERT INTO table (url) VALUES (%s)", new_urls)
```
- **Performance**: Batching queries (500 URLs per query) keeps Redshift queries fast
- **Location**: backend/main.py - `/api/upload-urls` endpoint (lines 463-542)
- **Date**: 2025-10-21

### Data Consistency Issue: Local Content Not Synced to Redshift
- **Problem**: 69,391 URLs had content locally, but only 60,000 had kopteksten=1 in Redshift (9,567 URLs out of sync)
- **Cause**: Batch processing completed locally but Redshift updates were lost or incomplete due to:
  - Network interruptions during batch commits
  - Interrupted processing sessions before Redshift sync
  - Failed Redshift UPDATE operations (silent failures)
- **Symptoms**:
  - System shows 50k+ pending URLs but only processes 24 per batch
  - Filtering logic excludes URLs that have local content but kopteksten=0 in Redshift
  - Progress stalls despite thousands of "pending" URLs
  - Status counts don't match actual content count
- **Impact**: URLs with completed content stuck in pending state, wasting processing cycles
- **Solution**: Created `backend/sync_redshift_flags.py` script to sync local content with Redshift
  - Queries `pa.content_urls_joep` (local content table - source of truth)
  - Updates Redshift `kopteksten=1` for all URLs with content
  - Batch updates (1000 URLs per query) for performance
  - Safe to run anytime (idempotent, only updates kopteksten=0 → kopteksten=1)
- **Script Usage**:
```bash
docker-compose exec -T app python -m backend.sync_redshift_flags
```
- **Result**: Synced 9,567 URLs, pending count dropped from 50,345 to 40,754 (accurate)
- **Prevention**: Run sync script after interrupted sessions or if progress stalls
- **Location**: backend/sync_redshift_flags.py, backend/main.py (filtering logic updated)
- **Date**: 2025-10-22

### Frontend Showing NaN/undefined in Batch Progress
- **Problem**: Frontend displays "Batch 1 Complete: undefined successful, NaN failed/skipped" during batch processing
- **Cause**: JavaScript directly using `data.processed` and `data.total_attempted` without null/undefined checks
- **Symptoms**:
  - Progress text shows "undefined" and "NaN" instead of numbers
  - Happens when API response has missing or undefined fields
  - Calculations like `total_attempted - processed` produce NaN
- **Solution**: Add default values using || operator:
```javascript
const batchProcessed = data.processed || 0;
const batchTotal = data.total_attempted || 0;
const batchFailed = batchTotal - batchProcessed;
```
- **Benefits**: Safe handling of undefined/null values, always displays valid numbers
- **Location**: frontend/js/app.js (lines 219-242)
- **Date**: 2025-10-22

### Beslist.nl Hidden 503 Errors in HTML Body
- **Problem**: Scraper marks URLs as "no_products_found" when actually rate limited
- **Cause**: Beslist.nl returns HTTP 200 status with "503 Service Unavailable" in HTML body when rate limited
- **Impact**: 33,946 URLs incorrectly marked as failed/skipped due to undetected rate limiting
- **Detection**:
```python
if response.status_code == 200:
    # Check for hidden 503 in HTML body
    if '503' in response.text or 'Service Unavailable' in response.text:
        print(f"Scraping failed: Hidden 503 (rate limited) for {url}")
        return None  # Keep URL in pending for retry
```
- **Behavior**: Returning None from scraper keeps URL in pending state (not marked as processed)
- **Location**: backend/scraper_service.py (lines 119-123)
- **Date**: 2025-10-21

### Docker Network Connectivity Loss After Restart
- **Problem**: After restarting Docker, all network connections from container timeout (ping, DNS, HTTP requests)
- **Symptoms**:
  - `docker-compose exec -T app python3 -c "requests.get('https://beslist.nl')"` hangs/times out
  - Even basic commands fail: `ping 8.8.8.8` times out
  - DNS lookups fail: `nslookup beslist.nl` times out
  - Scraper returns "scraping_failed" for all URLs
- **Root Cause Options**:
  1. **Proxy environment variables**: `HTTP_PROXY`/`HTTPS_PROXY` in docker-compose.yml pointing to invalid/inaccessible proxy
  2. **VPN routing issues**: VPN split tunneling configuration broken after Docker restart
  3. **WSL2 network bridge**: WSL2 network adapter needs refresh after Docker restart
- **Diagnostic Commands**:
```bash
# Test from host (should work)
curl -A "Beslist script voor SEO" https://www.beslist.nl/

# Test from container (fails if network broken)
docker-compose exec -T app sh -c "ping -c 2 8.8.8.8"
docker-compose exec -T app sh -c "nslookup beslist.nl"
```
- **Solutions** (try in order):
  1. **WSL restart** (fixes most issues): `wsl --shutdown` from Windows PowerShell, then restart WSL terminal
  2. **Check/unset proxy variables**:
```bash
echo $HTTP_PROXY $HTTPS_PROXY  # Check if set
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy
docker-compose down && docker-compose up -d
```
  3. **Remove proxy from docker-compose.yml**: Change `- HTTP_PROXY=${HTTP_PROXY:-}` to `- HTTP_PROXY=`
  4. **Check VPN configuration**: Verify VPN split tunneling still routes beslist.nl traffic correctly
- **Prevention**: After restarting Docker, always test basic connectivity before processing URLs
- **Location**: docker-compose.yml (lines 23-24), network configuration
- **Date**: 2025-10-21

### Port Conflicts
- FastAPI on 8003 (external) → 8000 (internal container port)
- PostgreSQL on 5433 (not 5432) for same reason
- Frontend accessible at http://localhost:8003/static/index.html

### CORS Errors
- Check `allow_origins` in main.py
- For dev: use `["*"]`
- For production: specify exact frontend URL

### Database Connection
- Wait for PostgreSQL to fully start
- Check DATABASE_URL in .env
- Run `docker-compose logs db` to debug

### Database Schema Column Missing
- **Error**: `column "status" does not exist`
- **Cause**: Schema changes applied to wrong database (postgres vs content_top)
- **Solution**: Check DATABASE_URL in docker-compose.yml, apply schema to correct database
- **Command**: `docker-compose exec -T db psql -U postgres -d content_top < backend/schema.sql`

### Pending Count Not Decreasing After Processing (UPDATED 2025-10-22)
- **Problem**: Pending count stays static at 11,756 even after processing 100 URLs, system shows "No URLs to process"
- **Root Cause**: Skipped and failed URLs were:
  1. ✅ Written to local PostgreSQL tracking table (pa.jvs_seo_werkvoorraad_kopteksten_check)
  2. ❌ **NOT updating the Redshift kopteksten flag** (pa.jvs_seo_werkvoorraad_shopping_season)
- **Symptoms**:
  - Redshift kept showing URLs as unprocessed (kopteksten=0)
  - System fetched same URLs repeatedly
  - Immediately filtered them out (already in local tracking)
  - Result: "No URLs to process" despite 11,756 pending
  - Pending count calculation: total_urls - tracked = constant (never decreases)
- **Impact**: URLs stuck in infinite loop, no progress possible
- **Solution (2025-10-20)**: Add `redshift_ops.append(('update_werkvoorraad', url))` for permanent failures:
  - no_products_found (line 86) - Page loads but has no products
  - no_valid_links (line 111) - AI generates content without valid links
  - ai_generation_error (line 127) - AI service error
- **Solution (2025-10-21)**: **REMOVED** Redshift update for scraping failures:
  - scraping_failed (line 79) - Network errors, 503, timeouts, access denied
  - **Reason**: Temporary network/access issues should be retried, not marked as permanently processed
  - **Behavior**: URLs with scraping failures stay in pending, can be retried on next run
  - **Status**: Local tracking still records 'failed' status for monitoring
- **Solution (2025-10-22)**: **Three-state tracking system** + **503-specific handling**:
  - kopteksten=0: Pending (not yet processed)
  - kopteksten=1: Has content (successfully processed)
  - kopteksten=2: Processed without content (skipped/failed non-503 errors)
  - **503 errors (rate_limited_503)**: NOT marked in Redshift, kept pending for retry, batch stops immediately
  - **Local tracking query changed**: Now filters ALL processed URLs (not just successful), preventing infinite retry loop
- **Result**:
  - Permanent failures (no products, bad content) → kopteksten=2 in Redshift
  - Successful content → kopteksten=1 in Redshift
  - 503 rate limiting → kopteksten=0 (stays pending), batch stops immediately
  - Non-503 scraping failures → kopteksten=2 (won't retry)
- **Location**: backend/main.py (lines 73-135, 247-260), backend/scraper_service.py (returns {'error': '503'})

### Frontend Showing N/A for Timestamps from Redshift
- **Problem**: Recent Results section showed "N/A" timestamps because Redshift output table (pa.content_urls_joep) lacks created_at column
- **Cause**: Redshift table schema doesn't include timestamp columns, but frontend expected created_at field
- **Symptoms**:
  - API returns `"created_at": null` for all recent results
  - Frontend displays "N/A" next to every URL
  - Local PostgreSQL has timestamps but Redshift doesn't
- **Solution Options**:
  1. **Query local PostgreSQL for timestamps** (implemented): Use separate connection to local database for recent results with timestamps
  2. **Hide timestamps in UI** (implemented): Conditionally render timestamp element only when data available
  3. **Add created_at to Redshift** (not implemented): Requires Redshift schema change and backfill
- **Implementation**:
```python
# Backend: Query local PostgreSQL for timestamps
try:
    local_conn = get_db_connection()  # Local PostgreSQL
    local_cur = local_conn.cursor()
    local_cur.execute("SELECT url, content, created_at FROM pa.content_urls_joep ORDER BY created_at DESC LIMIT 5")
    recent = [{'url': r['url'], 'content': r['content'], 'created_at': r['created_at'].isoformat() if r['created_at'] else None} for r in local_cur.fetchall()]
except:
    # Fallback: Query Redshift without timestamps
    recent = [{'url': r['url'], 'content': r['content'], 'created_at': None} for r in output_cur.fetchall()]
```
```javascript
// Frontend: Hide timestamp when null
const dateText = item.created_at ? new Date(item.created_at).toLocaleString() : '';
itemDiv.innerHTML = `
    <h6>${item.url}</h6>
    ${dateText ? `<small>${dateText}</small>` : ''}  // Only render if available
`;
```
- **Location**: backend/main.py (lines 333-361), frontend/js/app.js (lines 312-322)

### OpenAI httpx Compatibility
- **Error**: `TypeError: Client.__init__() got an unexpected keyword argument 'proxies'`
- **Cause**: OpenAI 1.35.0 incompatible with httpx >= 0.26.0
- **Solution**: Pin httpx==0.25.2 in requirements.txt

### Beslist.nl AWS WAF Challenge and User Agent Whitelisting
- **Problem**: Scraper returns "scraping_failed" for all URLs, but pages load fine in browser
- **Symptoms**:
  - `curl https://www.beslist.nl/...` returns AWS WAF "Human Verification" challenge page
  - Same URL with user agent `"Beslist script voor SEO"` returns actual HTML content
  - Without correct user agent: `<title>Human Verification</title>` and AWS WAF JavaScript challenge
  - With correct user agent: `<title>Ellen boren goedkoop kopen? | Beste aanbiedingen | beslist.nl</title>`
- **Root Cause**: Beslist.nl uses AWS WAF (Web Application Firewall) with:
  1. **User agent whitelisting**: Only allows specific user agents to bypass bot protection
  2. **JavaScript challenge**: Presents CAPTCHA/challenge for unrecognized bots
- **Whitelist Details**:
  - **User Agent**: `"Beslist script voor SEO"` (whitelisted)
  - **IP Address**: 87.212.193.148 (whitelisted, but user agent is primary authentication)
- **Testing**:
```bash
# Without user agent (gets WAF challenge)
curl https://www.beslist.nl/products/... | head -c 200
# Returns: <!DOCTYPE html><html lang="en"><head><title>Human Verification</title>

# With whitelisted user agent (gets actual page)
curl -A "Beslist script voor SEO" https://www.beslist.nl/products/... | head -c 200
# Returns: <!DOCTYPE html><html lang=nl-NL><head><title>Ellen boren goedkoop kopen?
```
- **Solution**: Ensure scraper uses correct user agent `"Beslist script voor SEO"`
- **Verification**: Check `USER_AGENT` constant in backend/scraper_service.py (line 11)
- **Important**: User agent authentication works regardless of IP address (confirmed working from 94.142.210.226, not just 87.212.193.148)
- **Location**: backend/scraper_service.py (line 11, 87)
- **Date**: 2025-10-21

### AI Generating Long Hyperlink Text
- **Problem**: AI generates very long anchor text (e.g., full product names with specifications like "Beeztees kattentuigje Hearts zwart 120 x 1 cm")
- **Cause**: Prompt instructions were vague about link text length
- **Solution**: Update GPT prompt with explicit constraints: "KORTE, heldere omschrijving (max 3-5 woorden)" with concrete example
- **Example**: "Beeztees kattentuigje Hearts zwart 120 x 1 cm" → "Beeztees kattentuigje Hearts"
- **Location**: backend/gpt_service.py - both system message and user prompt

### Browser Cache Not Showing Updated JavaScript
- **Issue**: JavaScript changes not visible in browser after editing
- **Cause**: Browser caches static files (CSS/JS) aggressively
- **Solution**: Hard refresh to bypass cache
  - Windows/Linux: Ctrl + Shift + R or Ctrl + F5
  - Mac: Cmd + Shift + R

### Browser Auto-Linking HTML Tags in Template Literals
- **Problem**: When inserting HTML content via template literals, browser auto-links HTML tags (e.g., `</div>` becomes a clickable link)
- **Cause**: Inserting raw HTML with `<a href>` tags directly into template literals like `${content}` causes browser to parse ALL text including the subsequent HTML tags as potential URLs
- **Solution**: Create DOM structure first with empty placeholders, then insert HTML content separately via `innerHTML`
- **Example**:
```javascript
// ❌ Wrong: Browser auto-links HTML tags
html += `<div>${content}</div>`;

// ✅ Correct: Insert HTML separately
const div = document.createElement('div');
div.innerHTML = content;
```
- **Location**: frontend/js/app.js - refreshStatus() function

### Windows File Paths Not Accessible from Docker Container
- **Problem**: Docker container cannot access Windows file paths like `C:/Users/...` or `/mnt/c/Users/...`
- **Cause**: Container has isolated filesystem, Windows paths are not mounted by default
- **Solution**: Copy file into container using `docker cp`, then run script
- **Example**:
```bash
# Copy CSV from Windows to container
docker cp /mnt/c/Users/JoepvanSchagen/Downloads/file.csv content_top_app:/app/file.csv

# Run Python script in container
docker-compose exec app python -m backend.import_content
```
- **Location**: CSV import workflow for bulk content upload

### Cloudflare Rate Limit Testing with Whitelisted IP
- **Problem**: Need to determine optimal scraping rate for whitelisted IP (87.212.193.148)
- **Testing Methodology**: Progressive speed testing from conservative (1.0-1.3s) to aggressive (0s burst mode)
- **Test Results**:
  - 1.0-1.3s delay: 100% success (10 URLs)
  - 0.5-0.7s delay: 100% success (10 URLs)
  - 0.3-0.5s delay: 100% success (10 URLs)
  - 0.1-0.3s delay: 100% success (10 URLs)
  - 0.05s delay: 100% success (15 URLs)
  - 0.02s delay: 100% success (15 URLs)
  - 0.01s delay: 100% success (15 URLs)
  - 0s delay (burst mode): 100% success (15 URLs)
- **Key Finding**: Whitelisted IP has NO rate limiting from Cloudflare, even at burst mode
- **Recommended Delays**:
  - **Optimized Mode** (default): 0.2-0.3s delay (~3-5 URLs/sec) - balanced speed with minimal risk
  - **Conservative Mode**: 0.5-0.7s delay (~2 URLs/sec) with 1 worker only - maximum safety for cautious operation
- **Implementation**: Two modes available via `conservative_mode` parameter and frontend checkbox
- **Location**: backend/scraper_service.py (lines 70-82), backend/main.py (conservative_mode enforcement)

### Custom User Agent for Scraper Identification
- **Problem**: Need to identify scraper traffic in server logs for debugging and traffic analysis
- **Solution**: Set custom user agent string that describes the scraper purpose
- **Implementation**: Define `USER_AGENT` constant at top of scraper service with descriptive string
- **Example**: `USER_AGENT = "Beslist script voor SEO"` instead of generic browser user agent
- **Benefits**:
  - Easier to filter and analyze scraper traffic in server logs
  - Clear identification for IT/operations teams
  - Distinguishes scraper from regular browser traffic
  - Helps with debugging rate limiting or blocking issues
- **Location**: backend/scraper_service.py (line 11)

## Git Commands
```bash
# SSH Setup
ssh-keygen -t ed25519 -C "your@email.com"  # Generate SSH key
cat ~/.ssh/id_ed25519.pub                   # Display public key (add to GitHub)
ssh -T git@github.com                       # Test GitHub connection

# Repository Setup
git init                                    # Initialize repository
git remote add origin git@github.com:user/repo.git
git branch -M main                          # Rename branch to main
git push -u origin main                     # Push to GitHub

# Configuration
git config user.name "username"
git config user.email "email@example.com"
```

## Project Patterns

### No Build Tools Benefits
- Edit HTML/CSS/JS → Save → Refresh browser
- No npm install delays
- No webpack configuration
- No node_modules folder (saves 500MB+)
- Works identically on any machine with Docker

### Real-time Progress Tracking with Polling
- **Pattern**: JavaScript polls API endpoint every 2 seconds for status updates
- **Benefit**: Live progress updates without WebSockets complexity
- **Example**: Poll `/api/status`, update progress bar, auto-stop when complete
```javascript
pollInterval = setInterval(updateJobStatus, 2000);
if (status === 'completed') clearInterval(pollInterval);
```

### Multi-Stage Docker Builds
- **Pattern**: Separate builder stage (with gcc, build tools) from runtime stage
- **Benefit**: Smaller final image (build dependencies not included)
- **Example**: Builder installs Python packages, final stage only copies venv
```dockerfile
FROM python:3.11-slim as builder
RUN apt-get install gcc && pip install -r requirements.txt

FROM python:3.11-slim
COPY --from=builder /opt/venv /opt/venv
```

### Environment Variable Management
- **Pattern**: Use python-dotenv for configuration
- **Benefit**: Secure secrets, reusable configuration, safe for version control
- **Example**: Load from .env file at startup
```python
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

# Use environment variables
api_key = os.getenv("OPENAI_API_KEY")
```

### Choosing Synchronous vs Async Endpoints in FastAPI
- **Pattern**: Use synchronous endpoints when working with synchronous database drivers
- **Use Case**: Endpoints that perform database operations with psycopg2 (synchronous driver)
- **Rule of Thumb**:
  - **Use `def` (sync)**: When using synchronous libraries (psycopg2, most database drivers)
  - **Use `async def`**: When using async-compatible libraries (httpx, aiofiles, asyncpg)
- **Why It Matters**: Async endpoints with sync operations block the event loop, causing hangs and deadlocks
- **Implementation**:
```python
# ✅ Correct: Sync endpoint with sync database driver
@app.post("/api/process-urls")
def process_urls(batch_size: int = 10):
    conn = get_db_connection()  # psycopg2 - synchronous
    # ... database operations ...
    return_db_connection(conn)
    return {"status": "success"}

# ✅ Also correct: Async endpoint with async operations
@app.get("/api/external-data")
async def fetch_external():
    async with httpx.AsyncClient() as client:
        response = await client.get("https://api.example.com")
    return response.json()

# ❌ Wrong: Async endpoint with sync database
@app.post("/api/process-urls")
async def process_urls():
    conn = get_db_connection()  # Blocks event loop!
```
- **Migration Path**: If you need async with databases:
  1. Switch to async driver (asyncpg for PostgreSQL)
  2. Update all database calls to use `await`
  3. Update connection pool to async pool
- **Performance Note**: Sync endpoints are perfectly fine for most use cases and often simpler to reason about
- **Location**: backend/main.py - all endpoints (converted to sync on 2025-10-23)
- **Date**: 2025-10-23

### Debugging Connection Pool Issues with Detailed Logging
- **Pattern**: Add detailed logging at each connection lifecycle step to identify pool exhaustion or blocking
- **Use Case**: Debugging why database connections hang, aren't returned, or pool is exhausted
- **Implementation**:
```python
def get_db_connection():
    """Get connection from pool"""
    pool = _get_pg_pool()
    print(f"[POOL] Getting PG connection...")
    conn = pool.getconn()
    print(f"[POOL] Got PG connection")
    return conn

def return_db_connection(conn):
    """Return connection to pool"""
    if conn:
        pool = _get_pg_pool()
        print(f"[POOL] Returning PG connection...")
        pool.putconn(conn)
        print(f"[POOL] Returned PG connection")
```
- **Benefits**:
  - Quickly identify where connections get stuck (e.g., "Getting..." but never "Got...")
  - See if connections are being returned (look for "Returned" logs)
  - Track connection lifecycle across requests
  - Diagnose pool exhaustion (multiple "Getting..." with no "Got...")
- **Debugging Workflow**:
  1. Add detailed logs to all connection get/return functions
  2. Run failing request
  3. Check logs for incomplete lifecycles
  4. Identify where connection is stuck or not returned
- **Example Debug Output**:
```
[POOL] Getting PG connection...
[POOL] Got PG connection
[ENDPOINT] Processing 2 URLs...
[POOL] Getting Redshift connection...
[POOL] Got Redshift connection
[ENDPOINT] Inserting 2 content records...
[ENDPOINT] Content inserts complete
[POOL] Returned Redshift connection
[POOL] Returned PG connection
```
- **Location**: backend/database.py (lines 44-98)
- **Date**: 2025-10-23

### Custom Slash Commands for Permission Management
- **Pattern**: Create markdown files in .claude/commands/ for frequently used operations
- **Use Case**: Quick toggles for Claude Code settings without manual file editing
- **Benefit**: Simple one-command access to complex configuration changes
- **Implementation**:
  1. Create `.claude/commands/` directory
  2. Add markdown files with plain text instructions (e.g., `skip-permissions.md`)
  3. Claude Code executes the instructions when command is invoked
- **Example Commands**:
  - `/skip-permissions`: Set `defaultMode` to `bypassPermissions` in `.claude/settings.local.json`
  - `/restore-permissions`: Set `defaultMode` back to `default`
- **Benefits**:
  - No need to remember file paths or JSON syntax
  - Consistent execution across team members
  - Self-documenting through command names
- **Location**: `.claude/commands/*.md`

### Project Separation Strategy
- **Pattern**: Separate distinct projects into independent repositories
- **Benefit**: Clean git history, independent versioning, easier management
- **Example**: content_top (SEO) and theme_ads (Google Ads) as separate repos
- **Implementation**:
  1. Identify files by project domain
  2. Clean backend to remove cross-project dependencies
  3. Update docker-compose and .gitignore
  4. Create new repository for separated project
  5. Copy files and create independent git history

### Parallel URL Processing with ThreadPoolExecutor
- **Pattern**: Process multiple URLs concurrently using Python's ThreadPoolExecutor
- **Benefit**: Significant speed improvement for I/O-bound tasks (scraping + AI)
- **Implementation**: Each worker gets own DB connection, configurable 1-10 workers
```python
with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
    results = list(executor.map(process_single_url, urls))
```

### Handling Hybrid Database Schema Differences (Timestamps)
- **Problem**: Hybrid architecture (PostgreSQL + Redshift) where output table exists in both databases, but Redshift table lacks created_at column
- **Use Case**: Displaying recent results with timestamps when Redshift is primary output destination
- **Solution**: Query local PostgreSQL for recent results with timestamps as fallback
- **Implementation**:
  1. Check if output connection is Redshift or PostgreSQL
  2. For Redshift: Query local PostgreSQL connection separately for timestamp data
  3. Handle gracefully when timestamps unavailable (set to None)
  4. Frontend conditionally displays timestamps only when available
- **Benefits**:
  - Works with schema differences between databases
  - Graceful degradation when timestamps unavailable
  - No need to modify Redshift schema
  - Users see timestamps when possible, clean UI when not
- **Example**:
```python
# Always query local PostgreSQL for timestamps
try:
    local_conn = get_db_connection()
    local_cur = local_conn.cursor()
    local_cur.execute("SELECT url, content, created_at FROM pa.content_urls_joep ORDER BY created_at DESC LIMIT 5")
    recent_rows = local_cur.fetchall()
    recent = [{'url': r['url'], 'content': r['content'], 'created_at': r['created_at'].isoformat() if r.get('created_at') else None} for r in recent_rows]
except Exception as e:
    # Fallback to output connection without timestamps
    output_cur.execute("SELECT url, content FROM pa.content_urls_joep LIMIT 5")
    recent = [{'url': r['url'], 'content': r['content'], 'created_at': None} for r in output_cur.fetchall()]
```
- **Location**: backend/main.py (lines 333-361)

### Conditional UI Element Display Based on Data Availability
- **Pattern**: Hide UI elements when data is unavailable instead of showing placeholder text like "N/A"
- **Use Case**: Timestamps, optional metadata, or any field that may not always be present
- **Benefits**:
  - Cleaner user interface
  - Avoids confusing users with "N/A" or "null" text
  - Dynamic layout adjusts to available data
- **Implementation**:
```javascript
// Check for data availability
const dateText = item.created_at ? new Date(item.created_at).toLocaleString() : '';

// Conditionally render element
itemDiv.innerHTML = `
    <h6 style="${dateText ? 'max-width: 85%;' : ''}">${item.url}</h6>
    ${dateText ? `<small>${dateText}</small>` : ''}
`;
```
- **Alternative Approaches**:
  - CSS display: none (requires extra DOM elements)
  - React conditional rendering (not applicable for vanilla JS)
- **Location**: frontend/js/app.js (lines 312-322)

### Database Cleanup and State Reset Workflow
- **Pattern**: When removing bad AI-generated results, follow 4-step process to ensure clean state
- **Use Case**: Removing results with quality issues (e.g., long hyperlinks) and reprocessing
- **Steps**:
  1. Re-add URLs to pending queue: `INSERT INTO pa.jvs_seo_werkvoorraad ... ON CONFLICT (url) DO NOTHING`
  2. Remove from tracking table: `DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check WHERE url IN (...)`
  3. Delete bad results: `DELETE FROM pa.content_urls_joep WHERE url IN (...)`
  4. Reset kopteksten flag: `UPDATE pa.jvs_seo_werkvoorraad SET kopteksten = 0 WHERE url IN (...)`
- **Benefit**: Ensures URLs can be reprocessed without duplicates or state conflicts
- **Important**: Use transactions (BEGIN/COMMIT) to ensure atomicity

### Database Query Performance - Avoiding NOT IN with Large Datasets
- **Problem**: Query timeout on status endpoint with 75,858 URLs (30+ seconds → timeout)
- **Cause**: `NOT IN (SELECT url FROM table)` performs poorly on large datasets (75k+ rows)
- **Solution**: Replace with `LEFT JOIN ... WHERE IS NULL` pattern
- **Performance**: Query time reduced from 30+ seconds to <100ms
- **Example**:
```sql
-- ❌ Slow: NOT IN subquery (75k rows = timeout)
SELECT COUNT(*) FROM pa.jvs_seo_werkvoorraad
WHERE url NOT IN (SELECT url FROM pa.jvs_seo_werkvoorraad_kopteksten_check);

-- ✅ Fast: LEFT JOIN pattern (<100ms)
SELECT COUNT(*)
FROM pa.jvs_seo_werkvoorraad w
LEFT JOIN pa.jvs_seo_werkvoorraad_kopteksten_check c ON w.url = c.url
WHERE c.url IS NULL;
```
- **Additional Optimization**: Add index on frequently filtered columns (e.g., `CREATE INDEX idx_kopteksten_check_status ON pa.jvs_seo_werkvoorraad_kopteksten_check(status)`)
- **Location**: backend/main.py - `/api/status` endpoint

### CSV Export with Proper Encoding and Formatting
- **Pattern**: Export database content to CSV with UTF-8 encoding and proper newline handling
- **Use Case**: Exporting AI-generated content that contains HTML, special characters, and multiline text
- **Implementation**:
  1. Add UTF-8 BOM (`\ufeff`) for Excel compatibility
  2. Strip newlines from content fields to prevent row breaks: `content.replace('\n', ' ').replace('\r', ' ')`
  3. Use `csv.QUOTE_ALL` to properly escape special characters
  4. Use `BytesIO` for binary output with UTF-8 encoding
  5. Set proper content type: `text/csv; charset=utf-8`
- **Benefits**:
  - No empty rows in exported CSV
  - Proper UTF-8 character display (fixes "geÃ¯" → "geï")
  - Excel opens file correctly without import wizard
- **Example**:
```python
from io import StringIO, BytesIO
import csv

output = BytesIO()
output.write('\ufeff'.encode('utf-8'))  # UTF-8 BOM

text_output = StringIO()
writer = csv.writer(text_output, quoting=csv.QUOTE_ALL, lineterminator='\n')
writer.writerow(['url', 'content'])

for row in rows:
    content = row['content'].replace('\n', ' ').replace('\r', ' ') if row['content'] else ''
    writer.writerow([row['url'], content])

output.write(text_output.getvalue().encode('utf-8'))
```
- **Location**: backend/main.py - `/api/export/csv` endpoint

### CSV Import for Bulk Content Upload
- **Pattern**: Import pre-generated content from CSV with semicolon delimiters and UTF-8 BOM
- **Use Case**: Bulk upload of AI-generated content (e.g., 19,791 items) from external sources
- **Implementation**:
  1. Read CSV with UTF-8-sig encoding (auto-strips BOM)
  2. Use semicolon (`;`) as delimiter for compatibility
  3. Extract `url` and `content_top` columns
  4. Insert into three tables atomically:
     - `pa.jvs_seo_werkvoorraad` - mark as processed (`kopteksten = 1`)
     - `pa.content_urls_joep` - store generated content
     - `pa.jvs_seo_werkvoorraad_kopteksten_check` - track as success
  5. Use `ON CONFLICT DO NOTHING` to skip duplicates
  6. Commit every 100 rows for progress tracking
- **Benefits**:
  - Handles large files (19k+ rows) efficiently
  - Transactional safety with periodic commits
  - Progress reporting during import
  - Skips duplicates automatically
- **Example**:
```python
import csv

with open(csv_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f, delimiter=';')
    for row in reader:
        url = row['url'].strip()
        content = row['content_top'].strip()

        # Insert into work queue (mark as processed)
        cur.execute("INSERT INTO pa.jvs_seo_werkvoorraad (url, kopteksten) VALUES (%s, 1) ON CONFLICT (url) DO UPDATE SET kopteksten = 1", (url,))

        # Insert content
        cur.execute("INSERT INTO pa.content_urls_joep (url, content) VALUES (%s, %s) ON CONFLICT DO NOTHING", (url, content))

        # Track as success
        cur.execute("INSERT INTO pa.jvs_seo_werkvoorraad_kopteksten_check (url, status) VALUES (%s, 'success') ON CONFLICT DO NOTHING", (url,))
```
- **Location**: backend/import_content.py

### Hybrid Database Architecture (Local PostgreSQL + Cloud Redshift)
- **Pattern**: Split database responsibilities between local PostgreSQL and cloud Redshift
- **Use Case**: Large-scale data processing where some tables benefit from cloud storage
- **Architecture**:
  - **Local PostgreSQL**: Fast tracking tables (processing status, temporary data)
  - **Redshift**: Persistent data tables (work queue, generated content)
- **Implementation**:
  1. Create separate connection functions: `get_db_connection()` (local), `get_redshift_connection()` (cloud), `get_output_connection()` (smart router)
  2. Route operations based on table purpose: tracking → local, data → Redshift
  3. Handle schema differences (e.g., Redshift table has no `created_at` column)
  4. Sync operations across both databases (delete from both, update in Redshift + track locally)
- **Benefits**:
  - Local tracking is fast (no network latency)
  - Centralized data in Redshift (accessible to other systems)
  - Can scale independently (add Redshift replicas without affecting local operations)
  - Redshift optimized for large datasets (166K+ URLs)
- **Environment Variables**:
  ```bash
  # Redshift configuration
  USE_REDSHIFT_OUTPUT=true
  REDSHIFT_HOST=production-redshift.amazonaws.com
  REDSHIFT_PORT=5439
  REDSHIFT_DB=database_name
  REDSHIFT_USER=username
  REDSHIFT_PASSWORD=password
  ```
- **Important**: Redshift credentials should be in `.gitignore` (use separate config file)
- **Location**: backend/database.py (lines 12-29), backend/main.py (throughout)

### Hyperlink Validation with Status Code Checking
- **Pattern**: Validate hyperlinks in generated content by checking HTTP status codes (301/404)
- **Use Case**: Quality control for AI-generated content - detect broken product links
- **Implementation**:
  1. Extract all `<a href>` tags from HTML content using BeautifulSoup
  2. Prepend base domain (`https://www.beslist.nl`) to relative URLs
  3. Check HTTP status with `requests.head()` (faster than GET)
  4. Parallel processing with ThreadPoolExecutor for speed
  5. If broken links found (301/404), auto-reset content to pending for regeneration
  6. Store validation results in JSONB column for audit trail
  7. Skip URLs already validated (LEFT JOIN check)
  8. Reset validation history when needed via DELETE endpoint
- **Benefits**:
  - Automated quality control for product links
  - Parallel validation speeds up large batches
  - Historical tracking of broken links
  - Auto-recovery workflow (reset to pending)
  - Incremental validation - only checks unvalidated URLs
  - Can reset and re-validate all URLs when needed
- **Example**:
```python
from bs4 import BeautifulSoup
import requests
from concurrent.futures import ThreadPoolExecutor

def validate_content_links(content):
    soup = BeautifulSoup(content, 'html.parser')
    links = [link['href'] for link in soup.find_all('a', href=True) if link['href'].startswith('/')]

    broken_links = []
    for link in links:
        full_url = 'https://www.beslist.nl' + link
        response = requests.head(full_url, allow_redirects=False, timeout=10)
        if response.status_code in [301, 404]:
            broken_links.append({'url': link, 'status_code': response.status_code})

    return {'broken_links': broken_links, 'has_broken_links': len(broken_links) > 0}

# Parallel validation
with ThreadPoolExecutor(max_workers=3) as executor:
    results = list(executor.map(validate_single_content, content_items))
```
- **Location**: backend/link_validator.py, backend/main.py - `/api/validate-links` endpoint

### CloudFront WAF Blocking Bot Traffic
- **Problem**: Website returns HTTP 403/405 errors for certain URLs when scraped
- **Cause**: CloudFront (AWS CDN) Web Application Firewall detecting automated traffic
- **Symptoms**:
  - Some category pages (e.g., `/products/accessoires/`) blocked regardless of User-Agent
  - Residential IP addresses more likely to be blocked than datacenter IPs
- **Troubleshooting**:
  1. Check public IP: `curl -s https://api.ipify.org`
  2. Test URL directly: `curl -I -A "User-Agent" "https://example.com"`
  3. Verify IP details: `curl -s https://ipinfo.io/YOUR_IP`
- **Solutions**:
  - Whitelist scraper IP in CloudFront WAF rules
  - Use slower request rates to avoid rate limiting
  - Contact IT department to adjust WAF settings
  - Consider using datacenter IPs instead of residential
- **Example**: IP `87.212.193.148` (Odido Netherlands residential FTTH) blocked by beslist.nl CloudFront

### VPN Routing Bypass for Whitelisted IP (Windows + WSL2/Docker)
- **Problem**: Company VPN routes all traffic through different IP, but scraper needs to use whitelisted IP (87.212.193.148) for beslist.nl
- **Scenario**:
  - Without VPN: Machine uses 87.212.193.148 (whitelisted)
  - With VPN: All traffic routes through 94.142.210.226 (not whitelisted)
  - Need: VPN connected for work (Redshift access), but scraper uses whitelisted IP
- **Failed Approaches**:
  1. OpenVPN client-side routing (`route X.X.X.X net_gateway`) - Error: "option 'route' cannot be used in this context [PUSH-OPTIONS]"
  2. OpenVPN `route-nopull` - `net_gateway` keyword doesn't work client-side
  3. OpenVPN `pull-filter ignore "redirect-gateway"` - VPN still captured CloudFront traffic
  4. Privoxy on Windows - Proxy itself routes through VPN
  5. Docker `network_mode: "host"` - Still uses VPN routing
- **Working Solution**: Windows Static Route with Lower Metric
  ```cmd
  # Step 1: Find your default gateway (before/during VPN)
  route print 0.0.0.0
  # Look for physical adapter (Ethernet/Wi-Fi), note Gateway IP (e.g., 192.168.1.1)

  # Step 2: Add persistent route with interface specification (as Administrator)
  route delete 65.9.0.0
  route add -p 65.9.0.0 mask 255.255.0.0 192.168.1.1 metric 1 if 10
  # Replace 192.168.1.1 with your gateway
  # Replace 10 with your Wi-Fi/Ethernet interface number from 'route print'

  # Step 3: Verify route is active
  route print 65.9.0.0
  # Should show metric 1 in Active Routes

  # Step 4: Restart WSL2 to pick up new routing
  # In PowerShell: wsl --shutdown
  # Then restart Docker Desktop
  ```
- **Why It Works**:
  - Windows routing is hierarchical: lower metric = higher priority
  - VPN routes typically have metric 25-50
  - Our route with metric 1 takes precedence for CloudFront IPs (65.9.0.0/16)
  - WSL2 and Docker inherit Windows routing table
  - Route is persistent (`-p` flag) - survives reboots
  - Interface specification (`if 10`) ensures it binds to physical adapter
- **Verification**:
  ```bash
  # From WSL2/Docker
  curl https://api.ipify.org          # Should show whitelisted IP
  curl https://www.beslist.nl/health  # Should show same IP
  ```
- **Result**: VPN stays connected, Redshift accessible, beslist.nl sees whitelisted IP (87.212.193.148)

### OpenVPN Split Tunneling Limitations on Windows Client
- **Problem**: Cannot configure OpenVPN split tunneling from client-side config file
- **Root Cause**: OpenVPN server pushes routes that override client-side directives
- **Why Client-Side Routes Fail**:
  1. `route X.X.X.X net_gateway` requires server push context - error: "cannot be used in this context [PUSH-OPTIONS]"
  2. `net_gateway` keyword only works in server-pushed routes, not client config
  3. `route-nopull` removes ALL routes including necessary internal network routes
  4. `pull-filter` can filter server options but Windows VPN adapter still captures traffic at OS level
- **Key Learning**: For corporate VPNs, split tunneling must be configured at:
  - **Server level** (requires admin/IT): Server pushes specific routes instead of redirect-gateway
  - **OS routing level** (can do yourself): Add Windows static routes with lower metric (see VPN Routing Bypass pattern above)
- **Alternative if Server-Side Split Tunneling Available**:
  - Server config: `push "route 10.0.0.0 255.0.0.0"` instead of `push "redirect-gateway def1"`
  - Client automatically gets split tunnel without config changes

### Privoxy Proxy Configuration for Docker/WSL2 Access
- **Problem**: Docker containers in WSL2 cannot connect to Privoxy running on Windows localhost
- **Cause**: Privoxy listens on 127.0.0.1:8118 by default, which only accepts local connections
- **Solution**: Configure Privoxy to accept connections from WSL2 network
  1. Edit Privoxy config file (usually `C:\Program Files\Privoxy\config.txt`)
  2. Change: `listen-address  127.0.0.1:8118`
  3. To: `listen-address  0.0.0.0:8118` (all interfaces) OR `listen-address  172.21.160.1:8118` (WSL2 gateway only)
  4. Restart Privoxy service
- **Finding WSL2 Gateway IP**: `ip route | grep default | awk '{print $3}'` (from WSL2, returns Windows host IP like 172.21.160.1)
- **Docker Proxy Config**:
  ```python
  session.proxies = {
      'http': 'http://172.21.160.1:8118',
      'https': 'http://172.21.160.1:8118'
  }
  ```
- **Note**: In this project, we ultimately used Windows static routing instead of Privoxy (more reliable)

### WSL2 IP Gateway Discovery
- **Problem**: Need to access Windows services (like Privoxy) from Docker containers running in WSL2
- **Solution**: Windows host is accessible via WSL2's default gateway IP
- **Command**: `ip route | grep default | awk '{print $3}'`
- **Example Output**: `172.21.160.1` (this is the Windows host IP from WSL2 perspective)
- **Common Use Cases**:
  - Connecting to Windows-hosted proxy servers
  - Accessing Windows file shares from containers
  - Connecting to Windows-hosted databases or services
- **Important**: This IP changes if network configuration changes, so don't hardcode in committed code

### Batch Database Operations for Performance
- **Problem**: Each URL processing makes 2 Redshift calls (INSERT content + UPDATE werkvoorraad), causing connection overhead
- **Impact**: With parallel workers, this creates many simultaneous Redshift connections (e.g., 10 workers × 2 calls = 20 connections)
- **Solution**: Batch Redshift operations after parallel processing completes
- **Implementation**:
  1. Modify worker function to return tuple: `(result_dict, redshift_operations)`
  2. Workers collect operations in list instead of executing: `redshift_ops.append(('insert_content', url, content))`
  3. After all workers complete, execute all operations in single transaction
  4. Use single Redshift connection for entire batch
- **Benefits**:
  - Reduces Redshift connections from N×2 (per URL) to 1 (per batch)
  - Improves throughput by 15-20% with parallel workers
  - Reduces connection overhead and network latency
  - Single transaction ensures atomicity for entire batch
- **Example**:
```python
# Worker function returns operations instead of executing
def process_single_url(url):
    redshift_ops = []
    # ... processing logic ...
    redshift_ops.append(('insert_content', url, content))
    redshift_ops.append(('update_werkvoorraad', url))
    return (result, redshift_ops)

# Batch execution after parallel processing
with ThreadPoolExecutor(max_workers=3) as executor:
    result_tuples = list(executor.map(process_single_url, urls))

# Collect all operations
all_redshift_ops = []
for result, ops in result_tuples:
    all_redshift_ops.extend(ops)

# Execute in single transaction
output_conn = get_output_connection()
output_cur = output_conn.cursor()
for op in all_redshift_ops:
    if op[0] == 'insert_content':
        output_cur.execute("INSERT INTO pa.content_urls_joep ...")
    elif op[0] == 'update_werkvoorraad':
        output_cur.execute("UPDATE pa.jvs_seo_werkvoorraad ...")
output_conn.commit()
```
- **Important Note (2025-10-23)**: Use individual `execute()` loops for Redshift, NOT `executemany()`
  - Redshift `executemany()` blocks indefinitely and never releases connections
  - PostgreSQL `executemany()` works fine
  - See "Redshift executemany() Blocking Indefinitely" error section for details
- **Location**: backend/main.py - `process_single_url()`, `process_urls()` endpoint

### Conservative Mode Pattern with ThreadPoolExecutor
- **Problem**: Need to pass additional parameters to worker functions when using ThreadPoolExecutor.map()
- **Use Case**: Conservative mode flag needs to be passed to each worker alongside the URL
- **Solution**: Use `functools.partial` to bind parameters before passing to executor
- **Implementation**:
```python
from functools import partial
from concurrent.futures import ThreadPoolExecutor

# Bind conservative_mode parameter to function
process_func = partial(process_single_url, conservative_mode=True)

# Pass partially-applied function to executor
with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
    results = list(executor.map(process_func, urls))
```
- **Benefits**:
  - Clean parameter passing without changing function signature
  - Worker function receives both url (from map) and conservative_mode (from partial)
  - No need for lambda or wrapper functions
- **Alternative Approaches Rejected**:
  - Lambda functions: More verbose and harder to read
  - Wrapper functions: Unnecessary code duplication
  - Tuple unpacking: Requires restructuring URL list
- **Location**: backend/main.py - `process_urls()` endpoint (lines 216-218)

### CSS Theme Override Pattern with Custom Properties
- **Problem**: Need to override Bootstrap default colors consistently across entire UI
- **Use Case**: Apply custom brand colors (#059CDF blue, #9C3095 purple, #A0D168 green) to match company branding
- **Solution**: CSS custom properties (CSS variables) with !important overrides
- **Implementation**:
```css
/* Define custom color palette */
:root {
    --color-primary: #059CDF;   /* Blue */
    --color-info: #9C3095;      /* Purple/Magenta */
    --color-success: #A0D168;   /* Green */
}

/* Override Bootstrap classes */
.bg-primary { background-color: var(--color-primary) !important; }
.btn-primary { background-color: var(--color-primary); border-color: var(--color-primary); }
.text-primary { color: var(--color-primary) !important; }

/* Include hover states (20% darker) */
.btn-primary:hover { background-color: #0480b3; border-color: #0480b3; }
```
- **Benefits**:
  - Single source of truth for color values (CSS variables)
  - No need to modify Bootstrap source files
  - Easy to maintain and update colors
  - Supports hover states and all Bootstrap color classes
  - !important ensures overrides work everywhere
- **Coverage**: Primary, Info, Success colors for buttons, badges, alerts, backgrounds, text, progress bars
- **Location**: frontend/css/style.css (lines 4-148)
- **Documentation**: ARCHITECTURE.md includes full color codes, usage map, and rationale

### Database Deduplication Strategy
- **Problem**: Content table had 48,846 duplicate records (108,722 total → 59,876 unique URLs)
- **Use Case**: After bulk imports or if multiple generation runs created duplicate content
- **Solution**: Use temporary table with ROW_NUMBER() window function to deduplicate
- **Implementation**:
```sql
-- Create temp table with deduplicated data
CREATE TEMP TABLE content_deduped AS
SELECT url, content
FROM (
    SELECT url, content,
           ROW_NUMBER() OVER (PARTITION BY url ORDER BY content) as rn
    FROM pa.content_urls_joep
)
WHERE rn = 1;

-- Replace original table
DELETE FROM pa.content_urls_joep;
INSERT INTO pa.content_urls_joep (url, content)
SELECT url, content FROM content_deduped;
```
- **Benefits**:
  - Handles large datasets efficiently (100K+ records)
  - Single transaction ensures data integrity
  - Window function picks one record per URL (randomly if no timestamp)
  - Works on Redshift without created_at column
- **Script**: `backend/deduplicate_content.py`
- **Result**: Removed 48,846 duplicates, 100% clean (0 duplicates remaining)

### Werkvoorraad Synchronization Pattern
- **Problem**: Content exists but werkvoorraad table not updated (URLs marked pending but have content)
- **Use Case**: After bulk imports, manual content additions, or interrupted processing
- **Solution**: Use SQL JOIN to update werkvoorraad table based on content table
- **Implementation**:
```sql
-- Update werkvoorraad table
UPDATE pa.jvs_seo_werkvoorraad_shopping_season w
SET kopteksten = 1
FROM pa.content_urls_joep c
WHERE w.url = c.url AND w.kopteksten = 0;

-- Add tracking records
INSERT INTO pa.jvs_seo_werkvoorraad_kopteksten_check (url, status)
SELECT c.url, 'success'
FROM pa.content_urls_joep c
LEFT JOIN pa.jvs_seo_werkvoorraad_kopteksten_check k ON c.url = k.url
WHERE k.url IS NULL
ON CONFLICT (url) DO UPDATE SET status = 'success';
```
- **Benefits**:
  - Efficient single-query update for thousands of URLs
  - Synchronizes both werkvoorraad and tracking tables
  - Prevents duplicate content generation
- **Script**: `backend/sync_werkvoorraad.py`
- **Result**: Synchronized 17,672 URLs, 0 overlaps remaining

### Link Validation Performance Analysis
- **Conservative Mode**: 0.5-0.7s delay per link check
  - 100 items with ~350 links = ~3m52s (~2.3s per item)
  - Rate: ~1,552 items/hour
  - Use case: Maximum caution, avoiding any rate limit concerns
- **Optimized Mode**: No delay between checks
  - Estimated 5-10x faster than conservative
  - With 5 workers: ~60K items in ~1 hour
  - Rate: ~60,000 items/hour (38x faster)
- **Recommendation**: Use optimized mode for link validation
  - Link validation just checks HTTP status (HEAD requests)
  - Much lighter than content scraping
  - Whitelisted IP has no rate limits
  - Conservative mode unnecessary for validation workloads

### Connection Pooling with psycopg2.pool.ThreadedConnectionPool
- **Problem**: Each worker creates/closes database connections for every URL processed
- **Impact**: Connection overhead of 50-200ms per URL adds up significantly
- **Solution**: Implement connection pooling to reuse connections across requests
- **Implementation**:
```python
from psycopg2 import pool

# Create connection pools (global, initialized on first use)
_pg_pool = None
_redshift_pool = None

def _get_pg_pool():
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            dsn=os.getenv("DATABASE_URL"),
            cursor_factory=RealDictCursor
        )
    return _pg_pool

def get_db_connection():
    """Get connection from pool"""
    return _get_pg_pool().getconn()

def return_db_connection(conn):
    """Return connection to pool"""
    if conn:
        _get_pg_pool().putconn(conn)
```
- **Benefits**:
  - 30-50% faster per URL (eliminates connection overhead)
  - Reduces network latency and handshake time
  - Better resource utilization (reuse existing connections)
  - Automatic connection management (pool handles lifecycle)
- **Configuration**: Pool size 2-10 connections per database (PostgreSQL + Redshift)
- **Important**: Always return connections to pool with `return_db_connection(conn)` in finally blocks
- **Location**: backend/database.py (lines 6-67), backend/main.py (all connection usage updated)

### Redshift COPY Command for Bulk Inserts
- **Problem**: Using executemany() for Redshift bulk inserts causes multiple network round-trips
- **Impact**: Batch operations take longer than necessary, especially for large batches
- **Solution**: Use COPY command which is 5-10x faster for bulk data loads
- **Implementation**:
```python
from io import StringIO

# Prepare data buffer
buffer = StringIO()
for url, content in insert_content_data:
    # Escape tabs and newlines
    content_escaped = content.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
    buffer.write(f"{url}\t{content_escaped}\n")
buffer.seek(0)

# Use COPY command (Redshift only)
if use_redshift:
    output_cur.copy_from(buffer, 'pa.content_urls_joep', columns=['url', 'content'], sep='\t')
else:
    # Fallback to executemany for PostgreSQL
    output_cur.executemany("INSERT INTO pa.content_urls_joep (url, content) VALUES (%s, %s)", insert_content_data)
```
- **Benefits**:
  - 20-30% faster for Redshift batch operations
  - Reduces network round-trips from N to 1 per batch
  - More efficient for large datasets (100+ rows)
  - Automatically falls back to executemany() for PostgreSQL
- **Performance**: COPY is 5-10x faster than INSERT for bulk operations in Redshift
- **Location**: backend/main.py (lines 260-275)

### Three-State URL Tracking in Redshift
- **Pattern**: Use tri-state flag instead of boolean for better tracking granularity
- **Use Case**: Need to distinguish between "successfully processed with content" vs "processed but no usable content" vs "not yet processed"
- **Implementation**:
  - `kopteksten = 0`: Pending (not yet processed)
  - `kopteksten = 1`: Successfully processed with content (has entry in content_urls_joep table)
  - `kopteksten = 2`: Processed without content (skipped, failed, no products, AI errors, etc.)
- **Benefits**:
  - Query for problematic URLs: `WHERE kopteksten = 2` shows all non-productive URLs
  - Better analytics: Can calculate success rate, skip rate, etc.
  - Clear distinction between "has content" and "tried but failed"
  - Prevents re-processing of legitimately empty pages
- **Redshift Operations**:
  - Success: `('update_werkvoorraad_success', url)` → sets kopteksten=1
  - Processed without content: `('update_werkvoorraad_processed', url)` → sets kopteksten=2
  - 503 errors: No Redshift update → stays kopteksten=0 for retry
- **Location**: backend/main.py (lines 73-135 for logic, 267-308 for batch execution)
- **Date**: 2025-10-22

### Distinguishing Scraping Failure Types for Retry Logic
- **Pattern**: Return different indicators from scraper for retriable vs non-retriable failures
- **Use Case**: Need to stop batch immediately on rate limiting (503) but mark other failures as processed
- **Implementation**:
  - **503 errors** (rate limiting): Return `{'error': '503'}` - triggers immediate batch stop
  - **Other failures** (timeout, network error): Return `None` - marked as processed (kopteksten=2)
  - **Success**: Return dict with scraped data
- **Benefits**:
  - Batch stops immediately on first 503 (not after 3 consecutive failures)
  - Non-retriable failures (timeout, connection error) don't stay in pending forever
  - Clear signal to calling code about failure type
  - Prevents wasting API calls when rate limited
- **Processing Logic**:
```python
scraped_data = scrape_product_page(url)
if scraped_data and scraped_data.get('error') == '503':
    # Rate limited - keep pending, stop batch
    result["reason"] = "rate_limited_503"
    rate_limited = True
    break
elif not scraped_data:
    # Other failure - mark as processed (kopteksten=2)
    result["reason"] = "scraping_failed"
    redshift_ops.append(('update_werkvoorraad_processed', url))
```
- **Location**: backend/scraper_service.py (returns {'error': '503'}), backend/main.py (lines 73-87, 256-260)
- **Date**: 2025-10-22

### Batch UPDATE Operations to Prevent Serialization Conflicts
- **Pattern**: When updating multiple rows in concurrent transactions, use batch operations with IN clauses instead of loops
- **Use Case**: Concurrent batch jobs updating the same database table (common in parallel processing systems)
- **Problem**: Individual UPDATEs in loops create transaction cycles when multiple workers access same rows
  - Worker A updates URL 1, waits for URL 2
  - Worker B updates URL 2, waits for URL 1
  - Serialization conflict detected, one transaction aborted
- **Solution**: Collect all URLs first, then execute single batch UPDATE
  ```python
  # ❌ Wrong: Loop with individual UPDATEs (causes conflicts)
  for (url,) in update_urls:
      cur.execute("UPDATE table SET status = 1 WHERE url = %s", (url,))

  # ✅ Correct: Single batch UPDATE with IN clause
  if update_urls:
      url_list = [url for (url,) in update_urls]
      placeholders = ','.join(['%s'] * len(url_list))
      cur.execute(f"""
          UPDATE table SET status = 1
          WHERE url IN ({placeholders})
      """, url_list)
  ```
- **Benefits**:
  - Prevents serialization conflicts in concurrent transactions
  - Reduces database round-trips from N to 1
  - Shorter transaction duration = lower conflict probability
  - 15-20% performance improvement for batch operations
  - Works with DELETE and UPDATE operations
- **Important Notes**:
  - Applies to both PostgreSQL and Redshift
  - Critical for any concurrent batch processing system
  - Also improves performance even without concurrency
  - Use same pattern for DELETE operations with broken links
- **Example - Link Validation**:
  ```python
  # Collect URLs with broken links first
  urls_with_broken_links = []
  for validation_result in validation_results:
      if validation_result['has_broken_links']:
          urls_with_broken_links.append(validation_result['content_url'])

  # Execute batch operations
  if urls_with_broken_links:
      placeholders = ','.join(['%s'] * len(urls_with_broken_links))
      # Delete content
      cur.execute(f"DELETE FROM pa.content_urls_joep WHERE url IN ({placeholders})", urls_with_broken_links)
      # Reset flags
      cur.execute(f"UPDATE pa.jvs_seo_werkvoorraad_shopping_season SET kopteksten = 0 WHERE url IN ({placeholders})", urls_with_broken_links)
  ```
- **Location**: backend/main.py (lines 295-317 for batch processing, lines 770-791 for link validation)
- **Date**: 2025-10-28

### Database Synchronization Pattern for Hybrid Architecture
- **Pattern**: Periodic sync script to ensure consistency between local and cloud databases
- **Use Case**: Hybrid architecture (local PostgreSQL + Redshift) where local writes may not complete in cloud due to network issues or interruptions
- **Problem**: Local content exists but cloud flags (kopteksten) not updated, causing mismatch between source of truth
- **Implementation**:
  1. Identify "source of truth" table (e.g., local content table with actual data)
  2. Identify "tracking" table (e.g., cloud flags indicating processing state)
  3. Create sync script that queries source table and updates tracking table
  4. Use batch updates (1000 rows) for performance on large datasets
  5. Make idempotent (safe to run multiple times, only updates stale records)
- **Benefits**:
  - Recovers from interrupted batch operations
  - Maintains data consistency across hybrid architecture
  - Can run safely anytime without duplicating work
  - Prevents progress stalls due to filtering mismatches
  - Provides one-time fix for accumulated inconsistencies
- **Example**:
```python
# Sync script structure
def sync_flags():
    # 1. Get source of truth
    local_cur.execute("SELECT url FROM pa.content_urls_joep")
    urls_with_content = [row['url'] for row in local_cur.fetchall()]

    # 2. Check cloud for stale records
    output_cur.execute("""
        SELECT COUNT(*) FROM pa.jvs_seo_werkvoorraad_shopping_season
        WHERE url IN (%s) AND kopteksten = 0
    """, urls_with_content)

    # 3. Batch update cloud flags
    for batch in chunks(urls_with_content, 1000):
        output_cur.execute("""
            UPDATE pa.jvs_seo_werkvoorraad_shopping_season
            SET kopteksten = 1
            WHERE url IN (%s) AND kopteksten = 0
        """, batch)
        output_conn.commit()
```
- **When to Run**: After interrupted sessions, network issues, or when progress stalls unexpectedly
- **Location**: backend/sync_redshift_flags.py (created 2025-10-22)
- **Date**: 2025-10-22

### Auto-Stop on Consecutive Scraping Failures
- **Pattern**: Track consecutive failures and stop batch processing after threshold
- **Use Case**: Detecting rate limiting (503 errors) and preventing wasted processing time
- **Implementation**:
```python
consecutive_failures = 0
for result in results:
    if result['status'] == 'failed' and result.get('reason') == 'scraping_failed':
        consecutive_failures += 1
        if consecutive_failures >= 3:
            print(f"[RATE LIMIT DETECTED] Stopping batch - {consecutive_failures} consecutive scraping failures")
            break
    else:
        consecutive_failures = 0  # Reset on success
```
- **Benefits**:
  - Prevents marking thousands of URLs incorrectly during rate limiting
  - Saves API costs (OpenAI) by stopping early
  - Clear signal to user that system is rate limited
  - Automatic recovery when resumed later
- **Threshold**: 3 consecutive failures (configurable)
- **Location**: backend/main.py - `process_urls()` endpoint (lines 242-256)
- **Date**: 2025-10-21

### CSV Upload with Relative URL Conversion
- **Pattern**: Handle CSV files with relative URLs by converting to absolute URLs
- **Use Case**: Importing URL lists where some URLs are relative (/products/...) instead of absolute (https://...)
- **Implementation**:
```python
import csv
from io import StringIO

# Parse CSV with auto-detected delimiter
csvfile = StringIO(file_content.decode('utf-8-sig'))
dialect = csv.Sniffer().sniff(csvfile.read(1024), delimiters=',;\t')
csvfile.seek(0)
reader = csv.reader(csvfile, dialect)

# Convert relative URLs to absolute
base_url = 'https://www.beslist.nl'
urls = []
for row in reader:
    if row and row[0].strip():
        url = row[0].strip()
        # Convert relative to absolute
        if url.startswith('/'):
            url = base_url + url
        urls.append(url)
```
- **Benefits**:
  - Handles both relative and absolute URLs seamlessly
  - Auto-detects CSV delimiter (comma, semicolon, tab)
  - Handles UTF-8 BOM encoding
  - Skips empty rows automatically
- **Batch Checking**: For Redshift compatibility (no ON CONFLICT), use batch checking:
  - Query existing URLs in batches of 500
  - Filter duplicates in Python
  - Insert only new URLs
- **Location**: backend/main.py - `/api/upload-urls` endpoint (lines 463-542)
- **Date**: 2025-10-21

### Elasticsearch plpUrl Lookup with Maincat Routing
- **Pattern**: Query Elasticsearch using pimId with maincat-specific indices
- **Use Case**: Map old product URLs to new plpUrl format
- **URL Formats Supported**:
  - Old: `/p/gezond_mooi/nl-nl-gold-6150802976981/` (maincat_url + pimId with prefix)
  - New: `/p/product-name/286/6150802976981/` (maincat_id in URL, pimId without prefix)
- **Implementation**:
  1. Load maincat mapping from CSV (maincat_url → maincat_id)
  2. Extract maincat_id from URL (check URL patterns)
  3. Build index name: `product_search_v4_nl-nl_{maincat_id}`
  4. Query with terms filter on pimId field
  5. Batch queries (10K pimIds per request)
- **ES API Endpoint**: `https://elasticsearch-job-cluster-eck.beslist.nl/{index}/_search`
- **Query Example**:
```json
{
  "_source": ["plpUrl", "pimId"],
  "size": 10000,
  "query": {
    "terms": {
      "pimId": ["nl-nl-gold-6150802976981", "nl-nl-gold-..."]
    }
  }
}
```
- **Maincat Mapping**: `/mnt/c/Users/JoepvanSchagen/Downloads/Python/maincat_mapping.csv` (semicolon-delimited, columns: maincat;maincat_url;maincat_id)
- **Location**: cc1/lookup_plp_urls.py
- **Date**: 2025-12-09

### Elasticsearch-based Link Validation (replaces HTTP checking)
- **Pattern**: Use Elasticsearch plpUrl lookup instead of HTTP HEAD requests for link validation
- **Use Case**: Validate product links in generated content - check if products still exist and URLs are correct
- **Why Replace HTTP Checking**:
  - HTTP HEAD requests are slow (network latency per link)
  - Can trigger rate limiting on large batches
  - Elasticsearch lookup is faster and more reliable
  - Can also detect URL changes (old URL → new plpUrl)
- **Implementation**:
  1. Extract all `<a href="/p/...">` links from HTML content using BeautifulSoup
  2. Parse pimId and maincat_id from each URL
  3. Query Elasticsearch for current plpUrl by pimId
  4. Compare: if plpUrl differs → replace in content; if product GONE → reset to pending
- **Three Outcomes**:
  - **Valid**: plpUrl matches original URL → no action
  - **Outdated**: plpUrl differs → auto-replace URL in content (kopteksten stays 1)
  - **Gone**: Product not found in ES → reset URL to pending (kopteksten=0) for content regeneration
- **Benefits**:
  - No rate limiting concerns (internal ES cluster)
  - Batch processing of multiple links per content item
  - Auto-correction of outdated URLs without regenerating content
  - Clear distinction between "needs URL update" vs "needs new content"
- **Location**: backend/link_validator.py (completely rewritten)
- **Date**: 2025-12-10

### Product Search API Content Generation
- **Pattern**: Generate SEO content by querying Product Search API and using product descriptions
- **Use Case**: Create content for SEO URLs with specific filters (brand, color, category)
- **URL Format**: `/products/{maincat}/{category}/c/{filter1~value1~~filter2~value2}`
  - Example: `/products/accessoires/accessoires_2596345/c/merk~2685977`
  - Also supports: `/products/{maincat}/c/{filters}` (without subcategory)
- **API Endpoint**: `https://productsearch-v2.api.beslist.nl/search/products`
- **Required API Parameters**:
  ```
  query=                           # Can be empty for category browsing
  mainCategory={maincat_name}      # e.g., "kantoorartikelen" (name, not ID)
  category={category_urlname}      # e.g., "kantoorartikelen_558052_558970"
  filters[{facet}][0]={value_id}   # e.g., filters[merk][0]=2829915
  limit=76                         # Max products to return
  offset=0                         # Pagination offset
  isBot=false                      # REQUIRED - API returns 400 without this
  countryLanguage=nl-nl            # REQUIRED - API returns 500 without this
  experiment=topProducts           # Optional, for ranking experiment
  trackTotalHits=false             # Optional
  ```
- **API Error Messages**:
  - Missing `isBot`: `{"errors":"isBot is a required parameter."}` (HTTP 400)
  - Missing `countryLanguage`: `findCategoryIdByCategoryUrlAndCountryLanguage(): Argument #2 ($countryLanguage) must be of type string, null given` (HTTP 500)
- **Request Headers**:
  ```
  Accept: application/json
  User-Agent: Beslist script voor SEO
  ```
- **Product Filtering** (as of 2026-01-28):
  - **Type filter**: Only include `type="result"` products, skip `type="orResult"`
    - `result` = exact match for all filters (correct brand, category, etc.)
    - `orResult` = partial/related match (may be wrong brand or loosely related)
  - **shopCount filter**: Only include products with `shopCount >= 2`
  - If URL returns only `orResult` products, it is skipped (no content generated)
  - Code location: `backend/scraper_service.py`, `backend/faq_service.py`
- **Implementation**:
  1. Parse URL to extract maincat name, category, and filters
  2. Build API params with required `isBot=false` and `countryLanguage=nl-nl`
  3. Build API URL with filters encoded as `filters%5B{name}%5D%5B0%5D={value}`
  4. Fetch products and filter by shopCount
  5. Extract `plpUrl` and `title` from each product
  6. Generate GPT content using product titles/descriptions
  7. Output includes proper `<a href="{plpUrl}">` links with product titles as anchor text
- **Output**: Excel file with columns: url, maincat_id, category, products_found, success, content
- **GPT Settings**: max_tokens=500 (increased from 200 to accommodate HTML links)
- **Location**: backend/scraper_service.py, backend/seo_content_generator.py
- **Date**: 2025-12-10, updated 2026-01-28

### Switching from Redshift to Local PostgreSQL Only
- **Context**: User requested to stop using Redshift and use only local PostgreSQL
- **Changes Made**:
  1. Updated `/api/status` endpoint to query local tables only
  2. Updated `/api/process-urls` endpoint to get pending URLs from local werkvoorraad
  3. Content is now saved directly in `process_single_url()` instead of batching to Redshift
  4. Removed all Redshift batch operations from process_urls endpoint
- **Tables Used (Local PostgreSQL)**:
  - `pa.jvs_seo_werkvoorraad` - Source URLs to process
  - `pa.jvs_seo_werkvoorraad_kopteksten_check` - Tracking table (status: success/failed/skipped)
  - `pa.content_urls_joep` - Generated content storage
- **Pending Calculation**: Uses LEFT JOIN to find URLs in werkvoorraad not yet in tracking table
- **Location**: backend/main.py
- **Date**: 2025-12-11

### URL Format Normalization
- **Problem**: Mixed URL formats causing mismatches between tables (relative vs absolute)
- **Formats Found**:
  - Absolute: `https://www.beslist.nl/products/...`
  - Relative: `/products/...`
  - Invalid: `/l/...` (old format)
- **Solution**: Normalize all URLs to absolute format
  ```sql
  -- Update relative to absolute
  UPDATE table SET url = 'https://www.beslist.nl' || url WHERE url LIKE '/products/%';
  -- Delete invalid /l/ URLs
  DELETE FROM table WHERE url LIKE '/l/%';
  ```
- **Date**: 2025-12-11

### MAIN_CATEGORY_IDS Authoritative Source
- **Source File**: `maincat_ids_new.xlsx` - DO NOT EDIT mapping without updating this file
- **Location**: backend/scraper_service.py MAIN_CATEGORY_IDS dict
- **How to Find Correct ID**: Check product URLs on beslist.nl category page - ID is in URL path `/p/product-name/{maincat_id}/ean/`
- **Key Mappings** (31 total):
  - autos: 37000, baby_peuter: 8, boeken: 701, computers: 6
  - elektronica: 655, fietsen: 38000, huis_tuin: 165, klussen: 35000
  - meubilair: 10, mode: 137, schoenen: 32000, speelgoed_spelletjes: 332
- **Date**: 2025-12-12

### Content Generation Performance Optimization
- **API Delay**: Reduced from 0.1-0.2s to 0.02-0.05s per call (5x faster)
- **Default Workers**: Increased from 3 to 6 (2x parallelization)
- **Default Batch Size**: Increased from 10 to 50 (5x per request)
- **Why Safe**: Product Search API is internal, less restrictive than scraping
- **Location**:
  - API delay: backend/scraper_service.py line 488
  - Workers/batch: frontend/index.html
- **Date**: 2025-12-12

### Product Search API-based Content Generation with Facet Extraction
- **Pattern**: Use Product Search API output to extract selected facet values and build product subjects
- **Use Case**: Generate SEO content for filtered category pages (e.g., `/products/elektronica/.../c/kleur~19958432~~modelnaam_mob~23748469`)
- **Implementation**:
  1. Parse URL to extract maincat, category, and filter parameters
  2. Call Product Search API with filters
  3. Extract facets where `"selected": true` from response
  4. Use `detailValue` field (Dutch adjective form) for colors/materials
  5. Build product subject with smart ordering: colors → product names → brands → category
- **Smart Category Name Inclusion**:
  - Include category name when only generic facets present (brand, color, target group)
  - Skip category when specific product/model/type facets exist (would be redundant)
  - Example: "Nike Heren" needs category → "Nike Heren voetbalschoenen"
  - Example: "iPhone 15" has specific product → no category needed
- **Key Functions**:
  - `parse_beslist_url()` - Extracts maincat, category, filters from URL
  - `build_api_params()` - Constructs API query parameters
  - `extract_selected_facets()` - Finds facets with `"selected": true`
  - `build_product_subject()` - Builds product name from facets
  - `scrape_product_page_api()` - Main function replacing HTML scraping
- **Integration**: Used in frontend SEO Content Generation (process_single_url in main.py)
- **Location**: backend/scraper_service.py
- **Date**: 2025-12-11

### SQL Filtering for Unvalidated URLs (LEFT JOIN vs In-Memory)
- **Problem**: "No content to validate" error when URLs exist but query returns wrong results
- **Cause**: SQL query with LIMIT but filtering done in-memory returns same already-validated rows
- **Wrong Approach**:
  ```python
  # Fetches all content, filters in Python - doesn't scale with LIMIT
  cur.execute("SELECT url, content FROM content LIMIT 1000")
  rows = cur.fetchall()
  unvalidated = [r for r in rows if r['url'] not in validated_set]  # May be empty!
  ```
- **Correct Approach**:
  ```sql
  -- Filter at database level using LEFT JOIN
  SELECT c.url, c.content
  FROM pa.content_urls_joep c
  LEFT JOIN pa.link_validation_results v ON c.url = v.content_url
  WHERE v.content_url IS NULL
  LIMIT 1000
  ```
- **Benefits**:
  - Efficiently finds unvalidated URLs at database level
  - LIMIT applies after filtering, not before
  - Works regardless of validation history size
- **Location**: backend/main.py `/api/validate-all-links` endpoint
- **Date**: 2025-12-11

### Content Generation Performance Optimizations
- **Problem**: Processing 131K URLs at ~4-10 seconds per URL would take 18-46 days
- **Goal**: Reduce processing time to 3-9 days (2.8-6x faster)
- **Optimizations Implemented**:
  1. **Reduced scraping delay** (0.5-1s → 0.05-0.1s): Whitelisted IP doesn't need aggressive rate limiting
  2. **Reduced AI max_tokens** (500 → 300): Content is max 100 words (~130 tokens), so 300 is sufficient
  3. **Batch local PostgreSQL commits**: Changed from 3-5 commits per URL to 1 commit per URL (all operations in single transaction at end)
  4. **Switch to lxml parser**: BeautifulSoup now uses lxml instead of html.parser (2-3x faster HTML parsing)
  5. **Use executemany() for Redshift**: Batch all INSERTs and UPDATEs using cursor.executemany() instead of loop
- **Performance Impact**:
  - Scraping delay: Save ~0.5-0.9s per URL (10-20% speedup)
  - AI tokens: Save ~0.5-1s per URL (10-15% speedup)
  - Local DB batching: Save ~0.2-0.4s per URL (5-10% speedup)
  - lxml parser: Save ~0.3-0.5s per URL (5-8% speedup)
  - executemany(): Save ~0.1-0.2s per batch (marginal but helpful)
  - **Total: 30-50% faster per URL** (4-10s → 2.5-7s per URL)
- **Combined with parallel workers**: Original default of 3 workers can be increased to 5-7 for linear speedup
- **Expected Results**:
  - Before: ~120-300 URLs/hour with 3 workers
  - After: ~350-840 URLs/hour with 3 workers, or ~580-1,400 URLs/hour with 5 workers
  - 131K URLs: 18-46 days → 5-15 days (with 3 workers) or 4-9 days (with 5 workers)
- **Files Modified**:
  - `backend/scraper_service.py`: Adjusted delay to 0.2-0.3s (balanced for Cloudflare), switched to lxml parser
  - `backend/gpt_service.py`: Reduced max_tokens from 500 to 300
  - `backend/main.py`: Refactored process_single_url() to batch local commits, added executemany() for Redshift
- **Location**: backend/scraper_service.py (lines 70-72, 102), backend/gpt_service.py (line 89), backend/main.py (lines 52-145, 208-243)
- **Note on Scraping Delay**: Initial attempt at 0.05-0.1s was too aggressive, causing Cloudflare HTTP 202 (queuing) responses even with whitelisted IP. Adjusted to 0.2-0.3s as sweet spot between speed and avoiding rate limits.

### Status API Consistency Fix - Distinct URL Counts
- **Problem**: Content status showed processed=167,841 but total_urls=149,954 (inconsistent)
- **Cause**: `processed` used `COUNT(*)` (total rows) while `total_urls` used `COUNT(DISTINCT url)`
- **Impact**: Duplicate URLs in content table caused misleading dashboard numbers
- **Solution**: Changed both endpoints to use `COUNT(DISTINCT url)` for all counts
  ```sql
  -- Before: COUNT(*) = 167,841 (total rows including duplicates)
  SELECT COUNT(*) as processed FROM pa.content_urls_joep;

  -- After: COUNT(DISTINCT url) = 149,954 (unique URLs only)
  SELECT COUNT(DISTINCT url) as processed FROM pa.content_urls_joep;
  ```
- **Also Fixed**: FAQ status endpoint now uses `pa.content_urls_joep` as source for total_urls (same as content status)
- **Result**: Both status endpoints show consistent counts based on distinct URLs
- **Location**: backend/main.py - `/api/status` (line 315) and `/api/faq/status` (lines 1066-1079)
- **Date**: 2025-12-17

### Color Theme - Purple Navbar, Grey Headers, Orange Buttons
- **Change**: Complete UI color overhaul
  - Top navbar: Purple (`#5e4a90`)
  - Section headers: Light grey (`#E8E9EB`)
  - All buttons: Burnt orange (`#CC5500`) with coral hover (`#E97451`)
- **Button Types Updated**: btn-primary, btn-info, btn-success, btn-secondary, btn-warning, btn-outline-* (all variants)
- **Header Types Updated**: All card-header elements use light grey
- **Exceptions**: btn-danger kept red for destructive actions
- **CSS Variables**:
  ```css
  --color-navbar: #5e4a90;       /* Purple - top navbar only */
  --color-section: #E8E9EB;      /* Light grey - section headers */
  --color-button: #CC5500;       /* Burnt orange - for buttons */
  --color-button-hover: #E97451; /* Coral orange - for hover */
  ```
- **Location**: frontend/css/style.css
- **Date**: 2025-12-17

### Project Merge - Unified SEO Tools Platform
- **Change**: Merged seo_faq, content_top, and theme_ads into single unified platform
- **Port**: All services now run on port 8003 (previously 8003 + 8002)
- **Approach**: Used FastAPI APIRouter to keep thema_ads code modular
- **Files Copied**:
  - `theme_ads/backend/thema_ads_service.py` → `backend/thema_ads_service.py`
  - `theme_ads/backend/database.py` → `backend/thema_ads_db.py`
  - `theme_ads/backend/main.py` → `backend/thema_ads_router.py` (converted to APIRouter)
  - `theme_ads/frontend/thema-ads.html` → `frontend/thema-ads.html`
  - `theme_ads/frontend/js/thema-ads.js` → `frontend/js/thema-ads.js`
  - `theme_ads/thema_ads_optimized/` → `thema_ads_optimized/`
  - `theme_ads/themes/` → `themes/`
- **Router Pattern**:
  ```python
  # thema_ads_router.py
  router = APIRouter(prefix="/api/thema-ads", tags=["thema-ads"])

  # main.py
  from backend.thema_ads_router import router as thema_ads_router
  app.include_router(thema_ads_router)
  ```
- **Benefits**: Single deployment, shared CSS/styling, unified dashboard
- **Date**: 2025-12-17

### UI Cleanup - Compact Tabs and Collapsible Info
- **Change**: Cleaned up thema-ads UI for better usability
- **Tab Improvements**:
  - Shortened tab names: Plan, Ad Groups, Discover, Check-up, Cleanup, All Themes, Activate, Duplicates
  - Compact padding and font-size for single-row display
  - Consistent anthracite (#2d3436) text with orange hover (#E97451)
- **Content Cleanup**:
  - Removed verbose explanatory text from all tabs
  - Added collapsible "More info" buttons for detailed documentation
  - "More info" buttons use inverted orange style (border/text → filled on hover)
- **CSS Pattern for collapsible info buttons**:
  ```css
  .more-info-btn {
      border: 1px solid #CC5500;
      color: #CC5500;
      background: transparent;
      transition: all 0.2s ease;
  }
  .more-info-btn:hover {
      background: #CC5500;
      color: white;
  }
  ```
- **Location**: frontend/thema-ads.html
- **Date**: 2025-12-17

### Canonical URL Generator - UI & Feature Updates
- **Button Styling**:
  - "Download CSV" button: Changed from `btn-outline-light` (grey) to `btn-outline-warning` (orange)
  - "Add Rule" buttons: Changed from `btn-outline-primary` (blue) to `btn-warning` (solid orange)
- **Tab Styling**:
  - Tab text color changed to anthracite (#3a3a3a) with bold font
  - Hover state: darker anthracite (#1a1a1a)
  ```css
  .nav-tabs .nav-link { color: #3a3a3a; font-weight: bold; }
  .nav-tabs .nav-link:hover { color: #1a1a1a; }
  .nav-tabs .nav-link.active { color: #3a3a3a; font-weight: bold; }
  ```
- **FACET-FACET Category Filter**: Added optional category filter to FACET-FACET rules (same as CAT+FACET)
  - Frontend: Added "Category (opt)" column and input field
  - Backend: Added `cat: Optional[str] = None` to `FacetFacetRule` dataclass
  - URL fetching and rule application now respect the category filter
- **Location**: `frontend/canonical.html`, `backend/canonical_service.py`
- **Date**: 2026-01-27

### Brand Mismatch Detection and Content Reset for Merk URLs
- **Problem**: SEO content generated before 2026-01-27 for URLs with `merk~` facet often linked to products from wrong brands
- **Root Cause**: AI prompt wasn't strict enough about linking only to products matching the URL's brand filter
- **Detection Method**: SQL-based validation using brand lookup table
  1. Load brand ID → name mapping from Excel into `pa.merk_lookup` table
  2. Extract merk ID from URL using regex: `regexp_match(url, '~~merk~([0-9]+)')` or `/merk~([0-9]+)`
  3. Check if brand name appears in content (missing = problematic)
  4. Extract product links from content and check if slug starts with brand name (wrong brand links)
- **Detection Query Pattern**:
  ```sql
  -- Find URLs where brand name is missing from content
  SELECT url FROM pa.content_urls_joep c
  WHERE c.url LIKE '%/merk~%'
    AND EXISTS (
        SELECT 1 FROM pa.merk_lookup m
        WHERE m.merk_id = (regexp_match(c.url, '/merk~([0-9]+)'))[1]::integer
        AND c.content NOT ILIKE '%' || m.brand_name || '%'
    );

  -- Find URLs where links point to wrong brands
  SELECT url FROM (
      SELECT url, LOWER(brand_name) as brand_lower,
          (regexp_matches(content, 'href="https://www.beslist.nl/p/([^/"]+)', 'g'))[1] as product_slug
      FROM content_with_brands
  ) WHERE LOWER(product_slug) NOT LIKE brand_lower || '%';
  ```
- **Reset Process** (to make URLs pending for regeneration):
  1. Archive old content: `INSERT INTO pa.content_history ... WHERE reset_reason = 'brand_mismatch_reset'`
  2. Delete content: `DELETE FROM pa.content_urls_joep WHERE url IN (...)`
  3. Reset werkvoorraad flag: `UPDATE pa.jvs_seo_werkvoorraad SET kopteksten = 0 WHERE url IN (...)`
  4. **Critical**: Delete from tracking table: `DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check WHERE url IN (...)`
- **Pending URL Logic**: URLs are pending when they exist in `jvs_seo_werkvoorraad` but NOT in `jvs_seo_werkvoorraad_kopteksten_check`
- **Results** (2026-01-28):
  - Total old merk content: 93,459
  - Missing brand name: 19,888 (21%)
  - Wrong brand links: 35,442 (38%)
  - Total reset to pending: 55,330 (59%)
- **Location**: Database tables `pa.merk_lookup`, `pa.content_history`, `pa.content_urls_joep`, `pa.jvs_seo_werkvoorraad_kopteksten_check`
- **Date**: 2026-01-28

### Beslist Product Search API Facet Validation Errors
- **Problem**: API calls fail with HTTP 400 for certain URLs
- **Cause**: URLs contain facet names or value IDs that are no longer valid for that category
- **Error Response Types**:
  - Invalid facet name: `{"context": "facet", "errorInfo": "The given facet is not valid.", "value": "personage"}`
  - Invalid facet value: `{"context": "merk", "errorInfo": "The given facet value is not valid.", "value": 19957206}`
- **Solution**: Detect 400 errors with "not valid" in errorInfo, return `facet_not_available` error type
- **Error Reasons Reference**:
  - `facet_not_available` - URL contains invalid facet name or value ID for category
  - `api_failed` - Generic API failure (non-400 or unparseable error)
  - `no_products_found` - API returned 0 products (skipped)
- **Location**: backend/scraper_service.py, backend/faq_service.py
- **Date**: 2025-12-26

### External API SQL Escaping Errors
- **Problem**: Website-configuration API returns MySQL INSERT exception when content contains apostrophes
- **Error**: `An exception occurred while executing 'INSERT INTO ... VALUES ('...DVD's...')`
- **Cause**: External API's MySQL INSERT not properly escaping single quotes
- **Solution**: Sanitize content before sending to external API:
```python
def sanitize_for_api(text: str) -> str:
    if not text:
        return ""
    # Normalize double single quotes to single (legacy data issue)
    text = text.replace("''", "'")
    # Replace single quotes with HTML entity
    return text.replace("'", "&#39;")
```
- **Location**: backend/content_publisher.py
- **Date**: 2026-01-15

### Redshift Serializable Isolation Violation (Error 1023)
- **Error**: `Error: 1023 DETAIL: Serializable isolation violation on table`
- **Cause**: Multiple concurrent batch jobs updating the same Redshift table with individual UPDATE statements
- **Solution**: Replace individual UPDATEs with batch UPDATE using IN clauses:
```python
# ❌ Wrong: Individual UPDATEs cause serialization conflicts
for url in urls:
    cur.execute("UPDATE ... SET kopteksten = 1 WHERE url = %s", (url,))

# ✅ Correct: Single batch UPDATE
if urls:
    placeholders = ','.join(['%s'] * len(urls))
    cur.execute(f"UPDATE ... SET kopteksten = 1 WHERE url IN ({placeholders})", urls)
```
- **Impact**: Eliminates serialization conflicts, 15-20% throughput improvement
- **Location**: backend/main.py
- **Date**: 2025-10-28

### FastAPI Async vs Sync Endpoints with psycopg2
- **Problem**: API endpoint hangs indefinitely at database connection
- **Cause**: Async endpoint (`async def`) calling synchronous psycopg2 pool operations blocks event loop
- **Solution**: Use synchronous endpoints (`def`) when using synchronous database drivers
```python
# ❌ Wrong: Async endpoint with sync database pool
@app.post("/api/process-urls")
async def process_urls():
    conn = get_db_connection()  # Blocks event loop!

# ✅ Correct: Synchronous endpoint
@app.post("/api/process-urls")
def process_urls():
    conn = get_db_connection()  # No event loop blocking
```
- **Rule**: Use `def` with psycopg2, use `async def` only with async-compatible drivers (asyncpg)
- **Location**: backend/main.py, backend/database.py
- **Date**: 2025-10-23

### Database Query Performance - LEFT JOIN vs NOT IN
- **Problem**: Query timeout with 75k+ URLs when using NOT IN subquery
- **Solution**: Replace with LEFT JOIN ... WHERE IS NULL pattern
```sql
-- ❌ Slow: NOT IN subquery (timeout)
SELECT COUNT(*) FROM table1 WHERE url NOT IN (SELECT url FROM table2);

-- ✅ Fast: LEFT JOIN pattern (<100ms)
SELECT COUNT(*) FROM table1 t1
LEFT JOIN table2 t2 ON t1.url = t2.url
WHERE t2.url IS NULL;
```
- **Performance**: 30+ seconds → <100ms
- **Location**: backend/main.py - `/api/status` endpoint
- **Date**: 2025-10-22

### R-Finder Tool - Finding /r/ Redirect URLs from Redshift
- **Purpose**: Find visited /r/ redirect URLs from Redshift for analysis
- **Endpoint**: `POST /api/rfinder/search` with filters, min_visits, date range, limit
- **Query Logic**: Uses AND logic for multiple filters (all must match)
- **Tables**: Same as canonical generator (`datamart.fct_visits`, `datamart.dim_visit`)
- **Key Features**:
  - Multiple URL path filters (combined with AND)
  - Minimum visits threshold
  - Date range filtering (default: 2015-01-01 to present)
  - Copy-to-clipboard with tab-separated relative URLs
- **Frontend**: `frontend/rfinder.html` with `frontend/js/rfinder.js`
- **Backend**: `backend/rfinder_service.py`
- **Location**: http://localhost:8003/static/rfinder.html
- **Date**: 2026-01-29

### Canonical REMOVEBUCKET Transformation
- **Purpose**: Remove facet buckets from URLs based on facet/category rules
- **Input**: Excel file with columns: `facet`, `caturl`
- **Script**: `run_canonical_removebucket.py` (standalone, bypasses API timeout)
- **Algorithm**:
  1. Parse facet→category mapping from Excel
  2. Query Redshift for URLs containing facet patterns (`facet~number`)
  3. For each URL matching a category, remove specified facet buckets
  4. Clean up separators (`~~` → `~`, trailing `/c/`, etc.)
- **Key Function**:
  ```python
  def remove_bucket_from_url(url, facet_name):
      pattern = rf'{re.escape(facet_name)}~\d+'
      # Handles: facet~~other, other~~facet, /c/facet
  ```
- **Results** (2026-01-30):
  - 780 rules (30 facets, 13 categories)
  - 10,000 URLs queried
  - 7,778 URLs transformed
  - 22 facets had no matching URLs in visited data
- **Output**: `/tmp/canonicals_output_removebucket.csv`
- **Date**: 2026-01-30

---
_Last updated: 2026-02-03 (301 Generator, UI/UX improvements, navigation updates)_

## 301 Generator Tool
- **Purpose**: Generate 301 redirects for URLs with unsorted facets or facet/category transformations
- **Features**:
  - Sort facets alphabetically (facet names before `~` are sorted)
  - Category transformations (CAT-CAT): Replace category slugs
  - Facet transformations (FACET-FACET): Replace facets with/without IDs
  - Works with facets with IDs (e.g., `merk~4412606` → `materiaal~484491`)
  - Works with facets without IDs (e.g., `merk` → `materiaal`, keeps the ID)
  - Category filter for facet rules (apply only to specific categories)
- **Files**:
  - Backend: `backend/redirect_301_service.py`
  - Frontend: `frontend/301-generator.html`
  - API endpoints in `backend/main.py`
- **API Endpoints**:
  - `POST /api/301-generator/generate` - Generate 301 redirects
  - `POST /api/301-generator/check` - Check single URL sorting
- **URL Sources**: Redshift visits data or manual URL input
- **Output**: CSV export with original URL and redirect target
- **Date**: 2026-02-03

## UI/UX Improvements (2026-02-03)
- **Project Rename**: "SEO Tools Dashboard" → "DM Tools Dashboard"
- **Tool Rename**: "SEO Content Generator" → "Kopteksten Generator"
- **Navigation Updates**:
  - All tools now linked in all page headers
  - Sticky navigation header (stays visible when scrolling)
  - Selected tool highlighted with lighter purple (#8b7bb5)
  - Removed `target="_blank"` from all navigation links
- **Dashboard Redesign**:
  - Inverted icon colors (purple background, white icons)
  - Removed bullet point feature lists for compact tiles
  - Removed subtitle text
  - Updated footer: "Digital Marketing tools by Joep van Schagen - 2026"
- **Footer**: Added consistent footer to all pages
- **Default Values Updated**:
  - FAQ Generator: Batch size 100, Parallel workers 20, Validation batch 500, Validation workers 20
  - Kopteksten Generator: Same defaults as FAQ Generator
  - Content Publishing: Default environment changed from "staging" to "production"
- **301 Generator Cleanup**:
  - Removed "Sort only" option
  - Removed info box
  - Changed rule headers from "Category Rules (CAT-CAT)" to "Category rules"
  - Changed X-button character from `X` to `×` (matching canonical generator)
  - Removed "New Maincat" option from category rules
- **Date**: 2026-02-03

## Combined Reset Validation Functionality
- **Change**: "Reset Validation" button now also resets skipped URLs recheck status
- **Behavior**: Calls both `/api/validation-history/reset` and `/api/recheck-skipped-urls/reset`
- **Purpose**: Single button to reset all validation/recheck state for fresh start
- **Location**: `frontend/js/app.js` - `resetValidationHistory()` function
- **Date**: 2026-02-03

## Redirect Checker Comma-Separated Input
- **Change**: URL input now accepts both newline and comma-separated URLs
- **Regex**: `urlInput.split(/[\n,]+/)` instead of just newlines
- **Location**: `frontend/redirect-checker.html`
- **Date**: 2026-02-03

## 301 Generator Bulk Paste & Remove All
- **Bulk Paste Feature**: Added ability to paste tab-separated data from Excel into both category and facet rules
  - Button "Bulk Paste" opens modal with textarea for pasting
  - Parses tab-separated lines (Excel format): `old_value\tnew_value` or `old_value\tnew_value\tcategory_filter`
  - Empty existing rules are automatically removed before adding new ones
  - Placeholder text explains expected format with outlined styling for visibility
- **Remove All Buttons**: Added "Remove All" button for both category and facet rules
  - Confirms before removing: "Are you sure you want to remove all category/facet rules?"
  - Clears all rules and adds one empty rule as placeholder
- **Key Functions**:
  - `parseCategoryBulk()` - Parse bulk category rules
  - `parseFacetBulk()` - Parse bulk facet rules (supports optional 3rd column for category filter)
  - `removeAllCategoryRules()` - Remove all category rules
  - `removeAllFacetRules()` - Remove all facet rules
- **Location**: `frontend/301-generator.html`
- **Date**: 2026-02-03

## 301 Generator Auto-Filter from Rules
- **Problem**: Previously fetched ALL faceted URLs from Redshift (e.g., 100,000), then applied rules to find matches (e.g., 93)
- **Solution**: Now automatically extracts patterns from rules and uses BATCHED queries
- **Smart Pattern Extraction**:
  - Extracts first facet from compound facets: `merk~83723~~model_lamp~123` → `merk~83723`
  - Deduplicates to get unique prefixes only
  - Handles many rules efficiently (100+ rules → ~10 unique patterns)
- **Batched Queries** (better than OR logic):
  - Runs separate query for each unique pattern
  - Each query is fast and simple: `WHERE url LIKE '%pattern%'`
  - Combines and deduplicates results
  - No limit on number of patterns
- **New Function**: `fetch_urls_with_facets_batched(patterns, ...)` in `redirect_301_service.py`
- **API Response**: Includes `search_patterns` showing which patterns were used
- **Frontend Changes**:
  - Removed "URL contains" filter (redundant with auto-filter)
  - Auto-filter always enabled when using Redshift
- **Example**: 36 rules with `model_lamp~XXXXX` → 36 batched queries (one per unique pattern)
- **Date**: 2026-02-03

## Google Ads WSA Error 10048 (Port Exhaustion)
- **Error**: "failed to connect to all addresses; WSA Error 10048"
- **Cause**: Too many API connections opened too quickly, exhausting available local ports
- **Windows Socket Error 10048** = "Address already in use" (WSAEADDRINUSE)
- **Solutions**:
  - Add delays between API calls (0.3s recommended)
  - Implement retry logic with exponential backoff
  - Wait 2-4 minutes after errors for ports to release (TIME_WAIT state)
- **Implementation**: Added to `process_reverse_exclusion_sheet` in campaign_processor.py
- **Date**: 2026-02-04

## Google Ads "unauthorized_client" Error
- **Error**: "unauthorized_client: Unauthorized" when initializing GoogleAdsClient
- **Root Cause**: refresh_token was generated with different client_id/client_secret than those in google-ads.yaml
- **Working Script Pattern** (`create GSD-campaigns WB.py`):
  - Hardcodes refresh_token and developer_token
  - Loads client_id/client_secret from environment variables (GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET)
  - Uses `GoogleAdsClient.load_from_dict()` instead of `load_from_storage()`
- **Fix**: Updated campaign_processor.py to use same approach as working script
- **Date**: 2026-02-04

## cat_ids Mapping for Campaign Matching
- **Purpose**: Map maincat_id to list of deepest_cats for finding related campaigns
- **Sheet**: `cat_ids` sheet in workbook with columns: maincat, maincat_id, deepest_cat, cat_id
- **Function**: `load_cat_ids_mapping(workbook)` returns `{maincat_id: [deepest_cat1, deepest_cat2, ...]}`
- **Usage Pattern**:
  1. Get maincat_id from input row
  2. Look up all deepest_cats: `cat_ids_mapping.get(maincat_id_str, [])`
  3. For each deepest_cat, construct campaign name: `PLA/{deepest_cat}_{cl1}`
- **Used By**: `process_exclusion_sheet_v2`, `process_reverse_exclusion_sheet`
- **Date**: 2026-02-04

## Canonical Generator Category Filter Bug
- **Problem**: CAT+FACET rules with category filter weren't respecting the filter
- **Root Cause**: `fetch_urls_for_rules()` fetched all URLs with facet but didn't filter by category
- **Affected Functions**:
  - `fetch_urls_for_rules()` - wasn't filtering fetched URLs
  - `_determine_tasks()` - applied rule to URLs not matching category
  - `_apply_cat_facet()` - didn't skip rules where category didn't match
- **Fix**: Added category filter checks in all three functions
- **Location**: `backend/canonical_service.py`
- **Date**: 2026-02-04

## Kopteksten Skip Reasons
- **Table**: `pa.jvs_seo_werkvoorraad_kopteksten_check` (PostgreSQL in seo_tools_db)
- **Columns**: url, status (success/skipped/failed), skip_reason
- **Skip Reasons**:
  - `no_products_found` (54,053) - page has no products
  - `api_failed` (3,670) - scraper API call failed
  - `no_valid_links` (402) - AI content has no valid /p/ links
  - `ai_generation_error: {msg}` - OpenAI generation failed
  - `rate_limited_503` - 503 error (rate limiting)
- **Reset Query**: `DELETE FROM ... WHERE status IN ('skipped', 'failed') AND skip_reason <> 'no_products_found'`
- **Date**: 2026-02-04

## IndexNow Integration
- **Service**: `backend/indexnow_service.py` - adapted from standalone `index_now.py`
- **Uses**: `database.py` Redshift connection pool (not hardcoded credentials)
- **API**: POST to `https://api.indexnow.org/IndexNow` with host, key, keyLocation, urlList
- **Redshift table**: `pa.index_now_joep` for submission tracking
- **Endpoints**: POST `/api/indexnow/submit`, POST `/api/indexnow/upload-excel`, GET `/api/indexnow/history`
- **Date**: 2026-02-10

## SEO Index Checker Integration
- **Service**: `backend/index_checker_service.py` - Google Search Console URL Inspection API
- **Uses**: Service account JSON files in `backend/service_accounts/` (gitignored)
- **Quota**: 2,000 requests/day per service account, rotates on quota exhaustion
- **Service account credentials**: Different from Keyword Planner (service account JSON vs OAuth2) - NOT interchangeable
- **Endpoints**: POST `/api/index-checker/check`, POST `/api/index-checker/upload-excel`, GET `/api/index-checker/quota`
- **Date**: 2026-02-10

## AI Title Generation - Size Placement Rule
- **Issue**: Sizes (Maat L, XL, 42, etc.) were placed before the product name ("Nike Heren Maat L tanktops")
- **Fix**: Added rule to both prompts in `ai_titles_service.py` to place sizes AFTER the product name
- **Correct**: "Nike Heren tanktops Maat L"
- **Both prompts updated**: `generate_ai_title()` (rule 4) and `generate_title_from_api()` (rule 6)
- **Reset**: 2,231 URLs with `maat` facets reset to pending for reprocessing
- **Date**: 2026-02-10

## Silent Docker Exec Output
- **Issue**: `docker exec` with inline Python (`python3 -c "..."`) sometimes produces no output
- **Workaround**: Write a .py script file and run it with `python3 -m backend.script_name` from `/app`
- **Date**: 2026-02-10
