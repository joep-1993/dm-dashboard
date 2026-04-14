Session Summary — 2026-04-14

1. Kopteksten Prompt — Vary Opening Phrases (backend/gpt_service.py:80,159)

- Problem: Nearly all generated kopteksten opened with "Bij het kiezen van een..." — repetitive across content
- Fix: Added soft variation rule to both subcategory and main category system prompts. Not a hard ban — the phrase can still appear occasionally, just not as the default opener
- Subcategory prompt (line 80): "Vermijd ook om te vaak te openen met 'Bij het kiezen van' — gebruik dit hooguit af en toe, niet standaard"
- Main category prompt (line 159): same rule added alongside existing "Welkom op de..." ban

---

  Session Summary — 2026-04-09 / 2026-04-10                                                                                                                                     
                                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
  1. AI Title Kinder+Meisjes/Jongens Fix (backend/ai_titles_service.py:509-523)                                                                                                 
                                                                                                                                                                                
  - Problem: Titles like "Kinder Meisjes Panty's" — "Kinder" not being dropped when "Meisjes" present                                                                           
  - Root cause: Old dedup logic was facet-name-based (only matched 3 specific facet names) but actual facets use dozens of different names (doelgroep_feestkleding,
  doelgroep_fietsen, dg_kind_horloge, etc.)                                                                                                                                     
  - Fix: Changed to value-based approach — any facet with value "Kinder"/"Kinderen"/"Baby" dropped when any facet has "Meisjes"/"Jongens". Also strips "Kinder" prefix from
  category names in H1                                                                                                                                                          
  - Reset 403 affected URLs                                       
                                                                                                                                                                                
  2. Performance Optimization — Query + Workers                   
                                                                                                                                                                                
  - Converted 4 LEFT JOIN queries to NOT EXISTS in backend/main.py — FAQ URL selection went from 4.2s to 190ms (16.5x faster), kopteksten similar improvement                   
  - DB connection pool: maxconn 20→60 in backend/database.py
  - Worker limits: 20→100 max across all backend endpoints, frontend defaults changed from 20 to 50                                                                             
  - Frontend validation updated in app.js, faq.js, index.html, faq.html                                                                                                         
                                                                                                                                                                                
  3. OpenAI Batch API Integration (NEW)                                                                                                                                         
                                                                                                                                                                                
  - New file: backend/batch_api_service.py (~500 lines) — full Batch API pipeline for both FAQ and kopteksten                                                                   
  - New endpoints: POST/GET /api/batch-start, POST/GET /api/faq/batch-start, /api/batch-status, /api/faq/batch-status
  - Frontend: "Bulk API" checkbox on both FAQ (faq.html) and Kopteksten (index.html) pages                                                                                      
    - When checked: batch size, workers inputs, and "Process URLs" button greyed out                                                                                            
    - "Process All URLs" triggers batch pipeline instead of normal loop                                                                                                         
    - Progress bar shows phases: preparing → uploading → processing → saving                                                                                                    
    - Polls /api/batch-status every 3 seconds                                                                                                                                   
  - How it works:                                                 
    a. Fetches all pending URLs from DB                                                                                                                                         
    b. Calls Product Search API for each URL (50 concurrent threads) to get product data
    c. Builds prompts, writes JSONL file                                                                                                                                        
    d. Uploads to OpenAI Files API, creates batch job             
    e. Polls every 15 seconds until OpenAI completes processing                                                                                                                 
    f. Downloads results, parses, saves to DB in bulk                                                                                                                           
  - Cost: 50% cheaper than real-time API (OpenAI batch pricing)                                                                                                                 
  - Speed: Prepare phase ~5-15 min (API calls), OpenAI processing ~15-60 min (vs 4-5 hours real-time for 30K URLs)    
  ---                                                                                                                                                                                                                                         
  4. Fix Kinder+Meisjes/Jongens redundancy (ai_titles_service.py:509-523)
                                                                                                                                                                                                                                              
  Titles like "Kinder Meisjes Panty's" or "VTech Meisjes Kinder Smartwatches" had both a general audience ("Kinder") and a specific child audience ("Meisjes") — "Kinder" is redundant when "Meisjes" is already present.
                                                                                                                                                                                                                                              
  The old logic was facet-name-based: it only dropped "Kinder" when the general facet came from doelgroep, doelgroep mode, or doelgroep schoenen AND the specific facet came from kinderafdeling, afdeling baby/kind, or doelgroep kind. But  
  many categories use different facet names like doelgroep_feestkleding, doelgroep_fietsen, doelgroep_horloge, doelgroep_speelgoed, dg_kind_horloge, doelgroep_kind_baby_mode, etc. — none of which were matched.                             
                                                                                                                                                                                                                                              
  The new logic is value-based: it checks if any facet has the value "Meisjes" or "Jongens", and if so, drops any facet with value "Kinder"/"Kinderen"/"Baby" regardless of the facet name. It also strips the "Kinder" prefix from category  
  names embedded in the H1 (e.g., when the category is "Kinderfietsen" and "Meisjes" is a facet, the H1 becomes "Meisjes fietsen" instead of "Meisjes Kinderfietsen").
                                                                                                                                                                                                                                              
  403 existing titles with both patterns were reset to pending.   

  ---                                                                                                                                                                                                                                         
  5. Prompt improvements (ai_titles_service.py generate prompt)
                                                                                                                                                                                                                                              
  Several rules were added/refined for the GPT prompt that generates titles:
                                                                                                                                                                                                                                              
  - Rule 2 (adjective stacking limit): When there are 4+ adjectives before the product, the prompt now instructs to move material and placement AFTER the product using "van" (material) and "voor aan" (placement). Example: "Witte Metalen  
  Klassieke Rechthoekige Muur wijnrekken" → "Klassieke witte rechthoekige wijnrekken van metaal voor aan de muur"                                                                                                                             
  - Rule 3 (adjective ordering): Explicit ordering defined: stijl (Klassieke, Moderne) → kleur (witte, rode) → vorm/formaat (rechthoekige, kleine) → [product]. After the first word, always lowercase.                                       
  - Rule 9 (capitalization): Only the first word capitalized, then lowercase (except brand names). Example: "Klassieke Witte Rechthoekige wijnrekken" → "Klassieke witte rechthoekige wijnrekken"                                             
  - Additional examples added to the prompt for edge cases (vibrators size placement, poloshirts lowercase, material-heavy titles with "van"/"voor aan")    

  ---
  6. Title Scoring (completed)

  - Ran scripts/score_titles.py on 684K unscored titles using GPT-4o-mini (25/batch, 20 workers, 2 parallel processes)
  - Result: All 1,023,808 titles scored in ~4.4 hours, 0 errors, avg score 8.00
  - Exported to ~/unique_titles_scored.xlsx via scripts/export_scored_titles.py
  - Reset 125,436 titles scoring <7 to pending for regeneration

  ---
  7. Data Cleanup

  - Removed 1,944 bad URLs containing "pricemax" or "+" from pa.unique_titles
  - Removed 102 URLs containing "merk~0" from all 6 tables (unique_titles, content_urls_joep, kopteksten_check, faq_content, faq_tracking, werkvoorraad)
  - Removed 29,632 URLs with winkel~ facet from all 6 tables — Product Search API returns no facet data for winkel-filtered URLs, so titles were just bare category names
  - Fixed 9 "vases" titles — GPT translated Dutch "vazen" to English, reset to pending

  ---
  8. FAQ/Kopteksten Tracking Ghost Records

  - 45,004 FAQ tracking records had status='success' but no content in faq_content — reset to pending
  - 373 kopteksten tracking records same issue — reset to pending
  - Also fixed 9 FAQ records with content but 'failed' status → success, inserted 7 missing tracking records
  - This explains the gap between FAQ content (200K) and kopteksten content (218K)

  ---
  9. Frontend Polish & Layout Standardization

  - Standardized page widths to col-md-10 mx-auto across ALL tools:
    - unique-titles.html was col-lg-8 (too narrow)
    - 301-generator.html, keyword-planner.html, url-checker.html, redirect-checker.html were col-md-11 (too wide)
  - Unified input-group layout with inline label prefix (Batch, Workers, FAQs) across Kopteksten, FAQ, Unique Titles
  - Right-aligned buttons via d-flex justify-content-between in all sections:
    - FAQ Generation, FAQ Link Validation, FAQ Content Publishing
    - Kopteksten Generation, Kopteksten Link Validation, Kopteksten Content Publishing
    - Unique Titles AI Generation
  - Kopteksten Upload URLs: merged Upload File + Add URLs into single "Process" button, file picker + textarea side by side
  - Content Publishing (both pages): dropdowns + last push timestamp left, Refresh Stats + Publish buttons right
  - Fixed .badge.bg-success color in css/style.css — was overridden to grey via --color-section, fixed to #198754 (green)
  - Fixed FAQ Recent Results X-button overflow — switched from flexbox to CSS grid (grid-template-columns: 1fr auto) with overflow: hidden on content div
  - Pre-filled title suffix "✔️ Tot !!DISCOUNT!! korting! | beslist.nl" in Unique Titles Add/Edit form

  Tool-specific cleanups:
  - MC ID Finder: wrapped in col-md-10, standard card headers, cleaned up inline styles, simplified search layout, placeholder "one shop per line"
  - URL Checker: 3-column layout (textarea col-md-7, file picker col-md-2, settings col-md-3). Settings inputs stacked with fixed-width labels (Parallel Workers, Requests/sec, Timeout), right-aligned. Check URLs button same width as settings. File picker height matches textarea. Placeholder changed to "url 1/url 2/url 3"
  - Redirect Checker: input-group settings with fixed-width labels, textarea height aligned with Check URLs button, placeholder changed to "url 1/url 2/url 3"
  - Redirects (301-generator): compact date/limit inputs inline with Generate button, transformation rules X-buttons right-aligned via flexbox, tab styling matched to Canonicals (bold text, full width), removed "(optional)" from section title
  - R-Finder: renamed "Category / Query Filters (AND logic)" to "URL-filters", placeholder text simplified to "Filter 1/2/3/4", removed helper text
  - FAQ Content Publishing: matched Kopteksten layout (dropdowns + last push left, buttons right)

  ---
  10. Bulk API for Unique Titles

  - Added "Bulk API" checkbox to unique-titles.html
  - When checked: batch size, workers, and Start Batch button greyed out
  - Process All triggers background processing with 50 concurrent workers
  - Note: Unlike FAQ/Kopteksten, unique titles use real-time API (not JSONL batch) because title generation has complex pre/post-processing tightly coupled around the OpenAI call
  - New endpoints: POST /api/ai-titles/batch-start, GET /api/ai-titles/batch-status
  - Backend defaults updated: workers 15→50

  ---
  11. Unique Titles Page Restructure

  - Split into separate card sections: Processing Status (numbers), AI Title Generation (controls), Recent Results
  - Removed "Idle" status badge from header
  - Processing Status has its own card header

  ---
  12. Unique Titles Batch UI Fix

  - Process All button was turning yellow briefly and showing "undefined: undefined/undefined (0%)"
  - Root cause: loadAiStatus() polls /api/ai-titles/status every 2 seconds and resets the UI when is_running is false. The batch uses a separate state (/api/ai-titles/batch-status), so the normal status always shows idle, causing UI reset
  - Fix: set aiBatchPolling flag immediately on click (before the fetch call), loadAiStatus returns early when flag is set. Batch/workers inputs hidden during run, restored in resetAiBatchUI. Progress shown in progress bar instead of button text

  ---
  13. Faulty URL Cleanup

  - Exported 158,751 faulty URLs to ~/faulty_unique_title_urls.xlsx (with reason column)
  - Removed from all 6 DB tables (unique_titles, content_urls_joep, kopteksten_check, faq_content, faq_tracking, werkvoorraad):
    - /r/ URLs (143,626): product redirect URLs that can't be parsed by Product Search API
    - populaire_themas_accessoires (8,134): invalid facet, API returns 400
    - type_parfum (6,901): invalid facet, API returns 400
    - pl_pennen (90): invalid facet, API returns 400

  ---
  14. Batch API 200MB Fix — Chunked Uploads

  - OpenAI Batch API has a 200MB file size limit per batch for gpt-4o-mini
  - 29K FAQ prompts with product data exceeded this limit, causing batch failure
  - Fix: split into chunks of 5,000 requests each, process sequentially, save results per chunk
  - Applied to both FAQ and kopteksten batch pipelines in batch_api_service.py

  ---
  15. First FAQ Bulk API Run (2026-04-11)

  - 29,076 FAQs generated via OpenAI Batch API (6 chunks of 5K requests each)
  - FAQ content total now 230,241 (up from ~200K)
  - 4 failures, 0 errors from this batch run
  - Processing took ~8 hours total (mostly OpenAI queue waiting time, not actual processing)
  - 13,584 URLs still pending (skipped during prepare phase — no products found)

You can find the changes in the commit history:

  1. 33c5d57 — Title scoring, Kinder dedup fix, tracking ghost records, prompt improvements
  2. 332f98a — Batch API, query optimization, worker limits
  3. b03f1c3 — Update cc1 docs and ARCHITECTURE.md
  4. d7f7c6d — Frontend polish, Bulk API for unique titles, standardize layouts
  5. 56632c7 — Batch UI fix, faulty URL cleanup
  6. 452d41f — Fix batch API 200MB limit, split into 5K-request chunks


---

# Session 2026-04-13 — Link validator V4 lookup fix + t_wanddeco title handling

## 1. Link validator — V4 product URLs were never validated

### TL;DR
`backend/link_validator.py` was querying Elasticsearch on the wrong field
(`pimId`) to resolve V4 product UUIDs. Because `pimId` never holds V4 UUIDs,
phase-1 ES lookup always returned zero hits, and the code deliberately
skipped phase-2 (wildcard fallback). Net effect: **every V4 product link in
generated content and FAQs was invisible to the validator** — never replaced
when slugs changed, never flagged `gone` when the product disappeared from
the catalog, and never triggering regeneration.

The fix changes the query field from `pimId` to `id` (V4 UUIDs live in the
`id` / `groupId` fields). This fixes both content validation
(`validate_and_fix_content_links`) and FAQ validation (`validate_faq_links`)
because both go through the same `query_elasticsearch_by_plpurl` helper.

### Root cause

Sampling a document in `product_search_v4_nl-nl_137` (Mode) confirms the
schema:

```
id       = 'V4_8e29c285-e70f-40b8-b0e2-884268d7936a'
groupId  = 'V4_8e29c285-e70f-40b8-b0e2-884268d7936a'
pimId    = 'nl-nl-gold-2TxuuRXyCrXDfgD9nU6dLuUC2e2o'
plpUrl   = '/p/.../137/V4_8e29c285-.../'
```

The V4 UUID is the document `id`, not `pimId`. Matching
`{"terms": {"pimId": [V4_...]}}` against a field that never contains `V4_...`
values trivially returns zero hits.

### File changed
`backend/link_validator.py` — function `query_elasticsearch_by_plpurl`
(around lines 147–230).

### Diff

```diff
-    # Phase 1: Fast pimId-based lookup with V4 UUIDs
+    # Phase 1: Fast id-based lookup with V4 UUIDs.
+    # V4 UUIDs are stored in the `id` / `groupId` fields of the index — NOT
+    # in `pimId` (which uses values like `nl-nl-gold-...`). Matching on
+    # pimId used to always miss, causing every V4 link to be silently
+    # skipped by the validator (neither replaced when slugs change, nor
+    # flagged gone when the product disappears from ES). Querying on `id`
+    # fixes both behaviors.
     try:
         v4_uuids = list(v4_to_original.keys())
         query = {
-            "_source": ["plpUrl", "pimId", "shopCount"],
+            "_source": ["plpUrl", "id", "shopCount"],
             "size": len(v4_uuids),
             "query": {
                 "terms": {
-                    "pimId": v4_uuids
+                    "id": v4_uuids
                 }
             }
         }

         es_url = f"{ES_URL}/{index}/_search"
         response = _es_session.post(es_url, json=query, timeout=15)
         response.raise_for_status()
         data = response.json()

         for hit in data.get('hits', {}).get('hits', []):
             source = hit.get('_source', {})
-            pim_id = source.get('pimId', '')
+            v4_id = source.get('id', '')
             es_plp_url = source.get('plpUrl', '')
             shop_count = source.get('shopCount', 0) or 0

-            if pim_id in v4_to_original:
-                original_url = v4_to_original[pim_id]
-                found_v4_parts.add(pim_id)
+            if v4_id in v4_to_original:
+                original_url = v4_to_original[v4_id]
+                found_v4_parts.add(v4_id)
                 if shop_count >= min_offers and es_plp_url:
                     result[original_url] = es_plp_url
                 else:
                     result[original_url] = None

-        if found_v4_parts:
-            print(f"[LINK_VALIDATOR] Fast pimId lookup found {len(found_v4_parts)}/{len(v4_uuids)} V4 URLs in {index}")
-    except Exception as e:
-        print(f"[LINK_VALIDATOR] Fast pimId lookup failed for {index}: {e}, falling back to wildcard")
-
-    # V4 URLs not found via pimId are skipped (not marked as gone).
-    # Wildcard queries on plpUrl are disabled because they always timeout on ES
-    # and never return results - they just slow down the entire validation process.
-    remaining = len(v4_to_original) - len(found_v4_parts)
-    if remaining > 0:
-        print(f"[LINK_VALIDATOR] Skipping {remaining} V4 URLs in {index} not found via pimId (wildcard disabled)")
+        # V4 UUIDs not in the phase-1 response are treated as GONE (product no
+        # longer exists in ES). We query on the authoritative `id` field, so
+        # a miss is reliable — no need for a wildcard fallback.
+        for v4_uuid, original_url in v4_to_original.items():
+            if v4_uuid not in found_v4_parts:
+                result[original_url] = None
+
+        if found_v4_parts:
+            print(f"[LINK_VALIDATOR] V4 id lookup found {len(found_v4_parts)}/{len(v4_uuids)} products in {index}")
+        missing = len(v4_to_original) - len(found_v4_parts)
+        if missing > 0:
+            print(f"[LINK_VALIDATOR] Marked {missing} V4 URLs as GONE in {index} (not found in ES)")
+    except Exception as e:
+        print(f"[LINK_VALIDATOR] V4 id lookup failed for {index}: {e} - skipping batch (not marking as gone)")
```

### Behavioral change beyond the field rename

Previously, V4 URLs missing from phase-1 results were intentionally left out
of the lookup result (silently skipped). That was defensive because phase-1
lookups were unreliable (wrong field) and phase-2 wildcards timed out.

With the fix, a miss on the authoritative `id` field is treated as **gone**
(the caller `validate_and_fix_content_links` adds the link to `gone_urls`,
which downstream triggers regeneration). If the ES request itself errors
out, the outer `except` still skips the whole batch without marking anything
gone — so transient ES failures cannot cause spurious regeneration.

### Scope — what this fixes

Two call sites invoke `query_elasticsearch_by_plpurl`:

1. `lookup_plp_urls_for_content` (line 308) — used by
   `validate_and_fix_content_links` → **content / koptekst link validation**.
2. `validate_faq_links` (line 619) → **FAQ link validation**.

Both now correctly resolve V4 URLs, replace stale slugs, and flag gone
products. Non-V4 URLs (numeric `pimId` like
`/p/foo/137/8718969401258/`) go through `query_elasticsearch` and were never
buggy — not changed.

### Verification

Tested against `/products/mode/mode_432356/` (4 V4 links in content).

Before the fix: validator reported `0 total / 0 broken / 0 valid`
(silently skipped all 4).

After the fix:

```
[LINK_VALIDATOR] V4 id lookup found 2/4 products in product_search_v4_nl-nl_137
[LINK_VALIDATOR] Marked 2 V4 URLs as GONE in product_search_v4_nl-nl_137 (not found in ES)
has_changes : True
gone_urls   : 2
  /p/nike-sportswear-hooded-jas-heren-met-capuchon/137/V4_2ecc...
  /p/kjelvik-gaby-gewatteerde-winterjas-blauw-halflang/137/V4_004a...
replaced    : 2
  /p/the-north-face-himalayan-gewatteerde-jas-zwart/.../V4_1468.../
    -> /p/the-north-face-himalayan-down-parka-winterjas-zwart-isolerende-550-fill-dons/.../V4_1468.../
  /p/pme-legend-snowpack-parka-wind-en-waterdicht-imitatiebont-voering/.../V4_05a4.../
    -> /p/pme-legend-snowpack-parka-wind-en-waterdicht/.../V4_05a4.../
```

Independently confirmed the two "gone" products are absent from ES across
`id`, `groupId`, and `plpUrl.keyword`; the two "replaced" products are
present with `shopCount=2`, `valid=true`.

### Data reset performed for the test URL

`/products/mode/mode_432356/` was reset for regeneration (same steps as the
`DELETE /api/result/{url:path}` endpoint in `backend/main.py:741`):

```sql
DELETE FROM pa.content_urls_joep
  WHERE url = '/products/mode/mode_432356/';                       -- 1 row
DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check
  WHERE url = '/products/mode/mode_432356/';                       -- 1 row
DELETE FROM pa.url_validation_tracking
  WHERE url = '/products/mode/mode_432356/';                       -- 0 rows
UPDATE pa.jvs_seo_werkvoorraad
  SET kopteksten = 0
  WHERE url = '/products/mode/mode_432356/';                       -- 1 row (was already 0)
```

The Redshift werkvoorraad equivalent
(`pa.jvs_seo_werkvoorraad_shopping_season`) was **not** touched here. If
`USE_REDSHIFT_OUTPUT=true` in the running process, that row still needs
`kopteksten=0` too.

### Recommended follow-ups

1. **Re-validate the full content corpus** with the fixed code so V4 links
   with stale slugs get rewritten and genuinely gone products trigger
   regeneration. Entry point: `validate_and_fix_content_batch` (line 469)
   or the FastAPI route that drives it.
2. **Same for the FAQ corpus** (`validate_faq_batch`, line 656).
3. Consider clearing old `pa.link_validation_results` rows that produced
   bogus `0/0/0` results so the re-run gives a clean history.

## 2. Unique-title generation — `t_wanddeco` treated as the category

### Background
When an AI title is generated (`backend/ai_titles_service.py`,
`improve_h1_title` around line 440), the API's category_name was unconditionally
appended to the H1 if missing, and then optionally stripped only when a
`Soort` facet value ended in a known product-type suffix. URLs with the
`t_wanddeco` facet therefore ended up with titles like
`"Acryl Metalen wandplaten Wanddecoratie kopen?..."` where
`Wanddecoratie` (the category) is redundant — the product type is already
carried by the `t_wanddeco` facet value (e.g. `wandplaten`).

### Change
`backend/ai_titles_service.py` — added a small override list and pre-pass
(around lines 453–475):

```python
# Facets that should be treated as the category name themselves. When any
# of these is present on the URL, the actual category_name is suppressed so
# the facet value (e.g. "wandplaten") carries the product noun instead of
# the generic category (e.g. "Wanddecoratie").
CATEGORY_OVERRIDE_FACETS = {'t_wanddeco'}
has_category_override = any(
    f['facet_name'].lower() in CATEGORY_OVERRIDE_FACETS for f in selected_facets
)
if has_category_override and category_name:
    cat_suffix = re.compile(r'\s+' + re.escape(category_name) + r'\s*$', re.IGNORECASE)
    api_h1 = cat_suffix.sub('', api_h1).strip()
    cat_prefix = re.compile(r'^' + re.escape(category_name) + r'\s+', re.IGNORECASE)
    api_h1 = cat_prefix.sub('', api_h1).strip()
    # Prevent downstream logic from re-appending it
    category_name = ''
```

The list is intentionally a single name for now — easy to extend later
without touching the logic.

### Data reset
All 61 `pa.unique_titles` rows matching `url ILIKE '%t_wanddeco%'` were
reset to pending so they get regenerated with the new logic:

```sql
UPDATE pa.unique_titles
SET ai_processed    = FALSE,
    ai_processed_at = NULL,
    ai_error        = NULL,
    title           = NULL,
    description     = NULL,
    h1_title        = NULL,
    title_score     = NULL,
    title_score_issue = NULL
WHERE url ILIKE '%t_wanddeco%';        -- 61 rows
```

"Pending" in the unique-titles flow means `(ai_processed IS NULL OR FALSE)
AND (title IS NULL/empty OR h1_title IS NULL/empty)` — see
`ai_titles_service.py:219–220`. Clearing the text fields is required;
flipping `ai_processed` alone is not enough.

## 3. thema_ads list_jobs query — GROUP BY error on startup

### Symptom
On every server startup the log showed:

```
Error cleaning up stale jobs: column "j.status" must appear in the GROUP BY
clause or be used in an aggregate function
LINE 3:                     j.*,
```

Non-fatal (startup continued) but the stale-job cleanup never actually ran,
so any `running` job left over from a crash/restart stayed marked `running`
forever.

### Root cause
`backend/thema_ads_service.py:list_jobs` did:

```sql
SELECT j.*, COALESCE(SUM(...), 0) AS ...
FROM thema_ads_jobs j
LEFT JOIN thema_ads_job_items i ON j.id = i.job_id
GROUP BY j.id
```

Postgres only permits `SELECT j.*` with `GROUP BY j.id` when `j.id` is
declared a PRIMARY KEY (functional-dependency detection). Inspecting the
schema in the remote PG shows `thema_ads_jobs` has **no constraints at all**
— not a PK, not unique — so Postgres rejects every non-aggregated column
from `j`. Adding a PK would be the neat fix, but requires a migration and
may need data cleanup first; reshaping the query is safer.

### File changed
`backend/thema_ads_service.py` — function `list_jobs` (around line 577).

### Diff

```diff
-            cur.execute("""
-                SELECT
-                    j.*,
-                    COALESCE(SUM(CASE WHEN i.status = 'successful' THEN 1 ELSE 0 END), 0) as successful_count,
-                    COALESCE(SUM(CASE WHEN i.status = 'failed' THEN 1 ELSE 0 END), 0) as failed_count,
-                    COALESCE(SUM(CASE WHEN i.status = 'skipped' THEN 1 ELSE 0 END), 0) as skipped_count,
-                    COALESCE(SUM(CASE WHEN i.status = 'pending' THEN 1 ELSE 0 END), 0) as pending_count
-                FROM thema_ads_jobs j
-                LEFT JOIN thema_ads_job_items i ON j.id = i.job_id
-                GROUP BY j.id
-                ORDER BY j.created_at DESC
-                LIMIT %s
-            """, (limit,))
+            # Pre-aggregate item counts per job_id so the outer query doesn't
+            # need a GROUP BY (thema_ads_jobs.id has no PRIMARY KEY, so Postgres
+            # can't infer functional dependency of j.* on j.id).
+            cur.execute("""
+                SELECT
+                    j.*,
+                    COALESCE(c.successful_count, 0) AS successful_count,
+                    COALESCE(c.failed_count,     0) AS failed_count,
+                    COALESCE(c.skipped_count,    0) AS skipped_count,
+                    COALESCE(c.pending_count,    0) AS pending_count
+                FROM thema_ads_jobs j
+                LEFT JOIN (
+                    SELECT
+                        job_id,
+                        SUM(CASE WHEN status = 'successful' THEN 1 ELSE 0 END) AS successful_count,
+                        SUM(CASE WHEN status = 'failed'     THEN 1 ELSE 0 END) AS failed_count,
+                        SUM(CASE WHEN status = 'skipped'    THEN 1 ELSE 0 END) AS skipped_count,
+                        SUM(CASE WHEN status = 'pending'    THEN 1 ELSE 0 END) AS pending_count
+                    FROM thema_ads_job_items
+                    GROUP BY job_id
+                ) c ON c.job_id = j.id
+                ORDER BY j.created_at DESC
+                LIMIT %s
+            """, (limit,))
```

Semantically identical result: one row per job with the same four count
columns. The subquery pre-aggregates on the child table, and the outer query
is a plain `LEFT JOIN` with no aggregation — no GROUP BY, no PK requirement.

### Verification
- Raw query executes cleanly against the live DB (0 jobs currently, but SQL
  is valid).
- Live endpoint `GET /api/thema-ads/jobs?limit=5` returns `{"jobs":[]}` with
  HTTP 200 after uvicorn picked up the change via `--reload`.
- `cleanup_stale_jobs` on startup will now run its `for job in jobs` loop
  without raising.

### Note on `--reload`
`uvicorn` is now launched with `--reload` both in the current process and in
the scheduled-task startup script
`C:\Users\JoepvanSchagen\scripts\start-dm-dashboard.ps1`, so backend edits
hot-swap without needing a manual restart.

## 4. Added missing PKs / sequences / FKs on `thema_ads_*` tables

### Why
The `thema_ads_*` tables in the live DB had **no constraints at all** — no
PRIMARY KEY, no sequence defaults on `id`, no FK from the child tables to
the parent. The schema in `backend/thema_ads_schema.sql` and
`backend/database.py` declares all of these (`id SERIAL PRIMARY KEY`,
`job_id INTEGER REFERENCES thema_ads_jobs(id) ON DELETE CASCADE`) but those
declarations never took effect in the live DB (tables probably created by a
different mechanism at some point). Consequences of the missing PKs:

- Postgres functional-dependency detection can't kick in, so queries like
  `SELECT j.*, SUM(...) FROM t j LEFT JOIN ... GROUP BY j.id` fail with
  "column must appear in GROUP BY" (section 3 above — the workaround works
  but the neater fix is to actually add the PK).
- `INSERT INTO thema_ads_jobs (status) VALUES ('pending')` would fail
  because `id` has no default and is `NOT NULL`.
- Orphaned `thema_ads_job_items` rows could survive a parent-job delete.

### Change (DDL applied in one transaction against the live PG)
All three tables were empty, so this was zero-risk.

```sql
-- Per table (thema_ads_jobs, thema_ads_job_items, thema_ads_input_data):
CREATE SEQUENCE IF NOT EXISTS <table>_id_seq;
ALTER TABLE <table> ALTER COLUMN id SET DEFAULT nextval('<table>_id_seq');
ALTER SEQUENCE <table>_id_seq OWNED BY <table>.id;
ALTER TABLE <table> ADD PRIMARY KEY (id);

-- Then the two child-table FKs:
ALTER TABLE thema_ads_job_items
  ADD CONSTRAINT thema_ads_job_items_job_id_fkey
  FOREIGN KEY (job_id) REFERENCES thema_ads_jobs(id) ON DELETE CASCADE;

ALTER TABLE thema_ads_input_data
  ADD CONSTRAINT thema_ads_input_data_job_id_fkey
  FOREIGN KEY (job_id) REFERENCES thema_ads_jobs(id) ON DELETE CASCADE;
```

### Verification
```
thema_ads_jobs       | PRIMARY KEY | id
thema_ads_job_items  | PRIMARY KEY | id
thema_ads_job_items  | FOREIGN KEY | job_id
thema_ads_input_data | PRIMARY KEY | id
thema_ads_input_data | FOREIGN KEY | job_id

id defaults now set to nextval('<table>_id_seq'::regclass) on all three tables.
```

### Follow-up thoughts
- The query rewrite from section 3 is still fine to keep — subquery
  pre-aggregation is arguably clearer than relying on PK-based functional
  dependency — but reverting to `SELECT j.* ... GROUP BY j.id` would now
  also be valid.
- Live `thema_ads_job_items` schema is missing the `campaign_id` /
  `campaign_name` columns that `backend/database.py` defines. Schema drift
  — worth a separate migration when convenient.

## 5. AI prompt — stijl adjectives (Industriële, etc.) placed before the noun

### Background
For URLs with the `stijl_test~8064049` facet (value "Industriële"), the AI
correctly placed the style adjective before the noun ~97% of the time
(484/498 processed titles), but a small minority ended up with the style at
the very end of the title:

```
"Gouden Stoffen Verstelbare Barkrukken Industriële"
"Oranje Hoge kasten met 2 laden met lades Industriële"
"Zwarte Grote Hoekbureaus 4 laden Industriële"
"... Fauteuils met armleuningen Woonkamer Industriële"
```

Root cause: prompt rule 4 only explicitly calls out colors and materials as
adjectives that must precede the noun. Style facets (`stijl_test`,
`stijl_woonaccessoires`, `stijl`, `stijl_schoenen`, `stijl_tas`,
`stijl_tegels`, `stijl_tuinart` — ~44k URLs total) have no rule, so the
model's placement is inconsistent.

### Change
`backend/ai_titles_service.py` — prompt rule 4 extended (single-line text
edit, around line 735):

```diff
-4. Kleuren en materialen als bijvoeglijk naamwoord VOOR de doelgroep en VOOR het zelfstandig naamwoord (bijv. "blauwe Heren hoodies", NIET "Heren blauwe hoodies").
+4. Kleuren, materialen en stijlen (bv. "Industriële", "Moderne", "Scandinavische") als bijvoeglijk naamwoord VOOR de doelgroep en VOOR de productnaam, NOOIT aan het einde van de titel (bijv. "blauwe Heren hoodies", "Industriële Zwarte tafels", NIET "Heren blauwe hoodies" of "tafels Industriële").
```

Effect applies to all `stijl*` facet families, not just `stijl_test`.

### Data reset
All 1,994 URLs containing `stijl_test~8064049` reset to pending so they get
regenerated under the new prompt:

```sql
UPDATE pa.unique_titles
SET ai_processed    = FALSE,
    ai_processed_at = NULL,
    ai_error        = NULL,
    title           = NULL,
    description     = NULL,
    h1_title        = NULL,
    title_score     = NULL,
    title_score_issue = NULL
WHERE url ILIKE '%stijl_test~8064049%';     -- 1,994 rows
```

Before: 572 processed / 1,422 unprocessed. After: 1,994 pending.
