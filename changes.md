  
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

