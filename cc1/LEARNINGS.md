# LEARNINGS
_Capture mistakes, solutions, and patterns. Update when: errors occur, bugs are fixed, patterns emerge._

## Swapped executemany tuple order silently breaks UPDATE (2026-04-18)
- **The bug**: `cur.executemany("UPDATE ... SET content = %s WHERE url = %s", [(url, content), ...])` — the tuple was `(url, content)` but the positional SQL expected `(content, url)`. psycopg2 binds positionally, so the UPDATE ran as `SET content=<url-string> WHERE url=<html-string>` every time — zero row matches, zero rows updated, conn.commit() succeeds with `rowcount=0`, no exception, no log. Every link-correction run since the code was written was a no-op. Two sites in `main.py` (`/api/validate-links` and `/api/validate-all-links`) had the same swap because the second endpoint was copy-pasted from the first
- **Why it hid**: `executemany` doesn't raise on 0-row updates. The surrounding code counted "URLs corrected" from the in-memory `urls_corrected` counter, which was incremented *regardless of DB outcome*. So the status dashboard reported "corrected 47 URLs" while the DB had 0 updates. Only a content audit would catch it
- **Pattern for any `executemany` UPDATE/DELETE**:
  1. The positional order of the SQL (`SET a=%s, b=%s WHERE c=%s`) must match the tuple order pushed into the list. Write one before the other and match them deliberately
  2. Prefer named parameters (`SET content = %(content)s WHERE url = %(url)s` with list-of-dicts) — psycopg2 supports them and they survive reordering
  3. If you *must* use positional binding in executemany, put a `# tuple order: (content, url)` comment on the list-append line and never edit one without the other
- **Generalisation**: whenever a symptom is "the UI says it worked but the data didn't change," suspect silently-zero-row mutations. Log `cur.rowcount` after every `executemany` that's supposed to mutate, or assert it matches `len(updates)` when you know no concurrent deletes happened
- **File**: `backend/main.py:/api/validate-links`, `backend/main.py:/api/validate-all-links`

## CREATE TABLE IF NOT EXISTS never adds columns to an existing table (2026-04-18)
- **The bug**: Thema Ads service `INSERT INTO thema_ads_jobs (..., batch_size, is_repair_job, theme_name)` against a schema whose `CREATE TABLE IF NOT EXISTS` block never listed those columns. On any fresh DB, job creation fails with `column "batch_size" does not exist`. Live DBs had the columns because someone added them by hand during the original rollout — that manual ALTER never made it into the init script, so the codebase's source-of-truth schema drifted from reality
- **Fix shape**: every column added after initial rollout needs an explicit `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` beside the `CREATE TABLE IF NOT EXISTS`. Columns in the CREATE block are only applied on first creation; they're skipped forever after. Postgres 9.6+ supports `ALTER TABLE ... ADD COLUMN IF NOT EXISTS col TYPE [DEFAULT ...]` in a single statement with multiple columns, so you can stack them
- **Project caveat**: there are *two* db-init paths in this repo — `backend/thema_ads_db.py:init_db` and `backend/database.py:init_db` — and they both define the same tables. Any schema migration has to land in both or the one that runs first "wins" and the other silently diverges. Long term this should be consolidated to one; for now, keep the ADD COLUMN IF NOT EXISTS blocks aligned in both files
- **Pattern**: when you add a column to an already-shipped table, grep the whole repo for `CREATE TABLE IF NOT EXISTS <tablename>` and add the `ALTER ... ADD COLUMN IF NOT EXISTS` next to *each* one. A single source-of-truth schema file would be cleaner, but that's a bigger refactor — until then, discipline
- **Files**: `backend/thema_ads_db.py`, `backend/database.py`

## Pool-returning connection vs raw connection: know which you have (2026-04-18)
- **The bug**: `backend/database.py:init_db` did `conn = get_db_connection()` (pool) then `conn.close()` at the end (close raw). `conn.close()` on a pooled connection destroys it — the pool's `putconn` is what returns the slot. Under frequent dev restarts this slowly drained the 2–20 connection pool until startup-time init couldn't get a connection
- **How to tell them apart in this repo**:
  - `backend/database.py:get_db_connection` → pool-backed, must `return_db_connection(conn)`
  - `backend/thema_ads_db.py:get_db_connection` → direct `psycopg2.connect`, `conn.close()` is correct (pooling was disabled there with a comment explaining why)
- **Pattern**: when a module exports `get_X_connection` and `return_X_connection`, the pair signals pooling. Call `return_X_connection`. If a module exports only `get_X_connection`, read the body — pooling may or may not be in play
- **Bigger lesson**: having two `get_db_connection` functions in sibling modules *with different semantics* is the real trap. Agents/humans pick the wrong one by importing the closest. Consolidating is on the backlog
- **Files**: `backend/database.py:init_db`, `backend/thema_ads_db.py`

## Frontend XSS via `innerHTML = "...${error.message}..."` (2026-04-18)
- **The pattern**: `resultDiv.innerHTML = \`<div class="alert alert-danger">Error: ${error.message}</div>\`` is ~40 copies across app.js / faq.js / thema-ads.js and several HTML files. `error.message` can be anything the backend puts into an HTTPException `detail` — and FastAPI echoes user input into errors (e.g. "Invalid URL: <script>…"). Click-through of a malformed URL + rendered error = stored-XSS-adjacent risk
- **Fix shape**: a shared `escapeHtml` helper at the top of each JS/HTML script tag (no build step in this project, so no single import point), then `${escapeHtml(error.message)}` everywhere the error lands in innerHTML. `data.error`, `data.detail`, `errors.join(', ')` — same treatment because they're all server-echoed strings. The grep query `innerHTML\s*=.*\$\{[^}]*error` surfaces every instance
- **Longer-term**: a real fix is to swap the `<div><alert>` error pattern to `textContent` assignment on a pre-built alert element. But that's a bigger UI refactor; inline `escapeHtml` is good enough until there's a reason to
- **Pattern beyond errors**: any `${foo}` interpolated into `innerHTML` where `foo` comes from the server (filenames, URLs, shop names, custom_id) is a risk. The innocuous-looking `${row.url}` in a results table can be the same bug if the backend echoed a user-uploaded URL

## Prefer structured results over log-parsing when the processor already has them (2026-04-18)
- **Symptom**: `validate_trees` dry-run export only contained campaign names — ad group and tree columns were empty. Root cause was the usual shared-parser mismatch: `validate_trees` logs `📁 Campaign: PLA/X` (no `(N ad group(s))` suffix) and 3-space-indented `   🔧 PLA/ag_a: …` status lines, but `_parse_affected_entities` requires the exclusion-style `(N ad group(s))` header and `\s{4,}` ad-group indentation. Campaigns limped through via the broad `r'campaign.*?(PLA/[^\s,()]+)'` fallback; ad groups and tree descriptions didn't
- **Anti-fix**: changing the log format to appease the parser (add more spaces, add the `(N ad group(s))` suffix) would've worked but is the wrong direction — it bakes parser assumptions into every processor's `print()` statements. Each new processor has to remember the format or regress the export
- **Fix**: `validate_trees` already populates `stats['details']` with `{campaign, ad_group, status, message}` per ad group — a structured table the processor builds naturally. In `_run_operation`, after the default `_parse_affected_entities(full_log)` runs, detect `operation == "validate_trees"` and *override* `affected` by iterating `result_data["details"]`. Split into `campaign_ad_group_pairs` (created/error → main Campaigns sheet) and `skipped_pairs` (skipped → dedicated Skipped sheet), then drop both into `affected` alongside the existing `campaigns/ad_groups/trees` sets. Frontend `exportRow` adds a new Skipped sheet when `aff.skipped_pairs` is present. Files: `backend/dma_plus_service.py:_run_operation` (~30-line override block after the parser call), `frontend/dma-plus.html:exportRow`
- **Pattern**: a log parser exists because some processors don't expose structured output. When a processor *does* have structured details (stats rows, validation results, etc.), bypass the parser for that operation instead of retrofitting the log format. The log stays human-friendly and the export gets richer data. Every `validate_*` operation in this codebase has a `details` list — they're all candidates for this override pattern if their exports look sparse
- **Skip categorisation in validate_trees**: the three skip branches all funnel into `status='skipped'`, differentiated by the `message` field:
  - `"Campaign name doesn't start with PLA/"`
  - `"No _a/_b/_c suffix in campaign name"` (no `_` after `PLA/`)
  - `"Suffix 'X' is not a/b/c"` (last-`_` suffix like `_store`, `_1`, `_d`)
- Giving Skipped its own sheet rather than lumping into Campaigns keeps the "faulty trees you should act on" (errors, dry-run proposals) visually separate from "not our convention, no action needed" rows — the user was looking at 1395 skipped of 12051 total, which would swamp the 990 real errors if intermixed

## Honest dry-run requires stubbing resource names, not just skipping writes (2026-04-17)
- **Shape of the problem**: `process_inclusion_sheet_v2` makes a chain of 5 Google Ads mutation calls — create campaign → attach negative list → create ad group → build listing tree → create product ad. Steps 3, 4, 5 consume a resource name returned by step 1 or 3; downstream code extracts IDs via `.split('/')[-1]`. A naive dry-run that just `if dry_run: return` on each helper would break the chain with `None` substrings
- **Fix**: synthesize plausible fake resource names (`customers/{cid}/campaigns/DRY_RUN_<uuid.hex[:10]>`, same shape for ad groups) and keep them flowing through the code. The `.split('/')[-1]` still yields `DRY_RUN_<hex>` which is a valid string for log lines. Calls that don't return anything load-bearing (negative list, tree build, ad creation) get a simple skip with a `[DRY RUN] Would ...` print so the log still shows what *would* have happened
- **Also skip the `time.sleep()`s between mutation steps in dry-run** — they exist for API rate-limiting, which obviously doesn't matter when no writes happen. Skipping turns a ~30s live run into a sub-second dry-run
- **Pattern**: when retrofitting dry-run onto code that wasn't designed for it, wrap the call sites in the caller, not the helpers. Helpers stay pure and reusable; all the dry-run conditionals live in one place. Only push `dry_run` into a helper when the helper has side-effects that need different code paths (e.g. a batch mutator that needs to construct a plausible success response)
- **File**: `backend/campaign_processor.py:process_inclusion_sheet_v2` (~40 lines of wrapping), same pattern in exclusion / reverse_inclusion / reverse_exclusion

## Google Ads Label description lives on `text_label`, not Label (2026-04-18)
- **Symptom**: `LabelService.mutate_labels` threw `Unknown field for Label: description` when setting `op.create.description = "..."`. Silently caught, so the DM_DASHBOARD label was never created — and therefore never attached to any campaign/ad-group, which the user only noticed via a stray log line
- **Fix**: Description (and background color) live on the `text_label` sub-message. Use `op.create.text_label.description = "..."` instead
- **Pattern**: when the Google Ads proto has a sub-message named after the parent type (e.g. Label has `text_label`, Campaign has `app_campaign_setting`, etc.), most "interesting" fields live inside that sub-message. The bare field on the parent is usually just `name` / `id` / `status` / `resource_name`. Before assuming a field sits directly on the top level, check the swagger / proto definition

## "Successful row" counters can hide zero-action runs (2026-04-18)
- **Bug class**: `process_reverse_exclusion_sheet` marks a workbook row TRUE if the underlying batch call returned `success > 0 OR not_found > 0` AND `errors == 0`. The intent — "the shop is no longer excluded for any reason" — is sensible, but it conflates "we removed N exclusions" with "there were 0 exclusions to remove". User saw "3 ok, 0 failed" and assumed the 282×3 wibra.nl exclusions were gone; they weren't
- **Fix**: keep the per-row counter but add **run-wide action counters** alongside it: `run_total_batch_calls / run_total_removed / run_total_already_not_excluded / run_total_mutate_errors`. Render them as separate summary lines, and emit a clear warning when `removed == 0 && batch_calls > 0`. Also walk `GoogleAdsException.failure.errors` in the batch helper so error messages aren't truncated to 100 chars
- **Pattern**: a counter is honest only if it counts the thing the user thinks it counts. When the underlying API has multiple "success-shaped" outcomes (removed, was-already-removed, no-op, etc.), surface them separately. The single "Rows OK" number is fine for at-a-glance UI but should never be the only signal in the log
- **Verification approach that worked**: when the user reported "the script claimed success but didn't actually mutate," the diagnostic path was (1) replicate the batch helper's read+filter step in isolation against the live ad group → confirms it identifies the right resource_name; (2) call `mutate_ad_group_criteria` directly with that exact resource_name → confirms the API works; (3) call the full batch helper in isolation on a different ad group → confirms the helper end-to-end; (4) call `process_reverse_exclusion_sheet` itself on a small slice → confirms the orchestration. Each step narrows the possible failure surface
- **File**: `backend/campaign_processor.py:reverse_exclusion_batch`, `process_reverse_exclusion_sheet`

## A shared log parser needs coverage for every processor's log format (2026-04-17)
- **Pattern observed three times this session**: new mutating operation added → export rows look empty for that op → root cause is the processor prints its own flavor of `📁 Campaign: …` / `Creating campaign: …` / `CAMPAIGN N/M: …` / `📁 Campaign: PLA/X (N ad group(s))` and my parser only knows a subset. Each one cost a round-trip with the user before I checked the log format
- **Fix cadence that works**: before adding a processor to DMA+, grep its `print(f"...")` statements for campaign-name and ad-group-name lines and cross-check the patterns in `_parse_affected_entities`. Header variants I've now catalogued:
  - `    📁 Campaign: PLA/X (N ad group(s))` — exclusion / reverse_exclusion
  - `CAMPAIGN 1/3: PLA/Klussen store_a` — reverse_inclusion
  - `   Creating campaign: PLA/Klussen store_a` — inclusion
  - `   ──── Ad Group: PLA/wibra.nl_a ────` — reverse_inclusion
  - `   ──── Ad Group 1/3: PLA/wibra.nl_a ────` — inclusion
  - `      ⏭️/✅/❌ PLA/X: ...` — exclusion / reverse_exclusion status lines
- **Pattern**: when a shared downstream consumer (parser, exporter, summarizer) touches output from multiple producers, changes to any producer's output format are a silent regression risk. Worth a single "log-format contract" comment block above `_parse_affected_entities` listing every expected format, so whoever adds the next processor has a checklist
- **File**: `backend/dma_plus_service.py:_parse_affected_entities`

## Log-parsing regexes trip on entity names with spaces (2026-04-17)
- **Bug**: exported DMA+ Affected Campaigns only showed `PLA/Klussen` when the run had processed `PLA/Klussen store_a`, `PLA/Klussen store_b`, `PLA/Klussen store_c`. Three distinct campaigns collapsed to one export row. Parser regex was `r'campaign.*?(PLA/[^\s,()]+)'` — the `[^\s]` stops at the first space, truncating `"PLA/Klussen store_a"` → `"PLA/Klussen"`, and the `campaigns = set()` dedupes to one
- **Fix**: two specific patterns per log format, broad fallback only if they miss. For reverse-inclusion's `CAMPAIGN N/M: PLA/X store_a` header: `r'^\s*CAMPAIGN\s+\d+/\d+:\s+(PLA/.+?)\s*$'` (greedy to EOL). For exclusion's `📁 Campaign: PLA/X (N ad group(s))`: `r'Campaign:\s+(PLA/.+?)\s+\(\d+\s+ad\s+group'`. Both tolerate spaces inside the name because they anchor on a distinctive terminator (end-of-line, or `" ("`)
- **Pattern**: parsers should anchor on *stable boundaries* in the log (distinctive surrounding tokens), not assume the thing in the middle matches a character class. When adding a new log format, write the regex against real log samples — don't guess at field shapes
- **Also**: walk the log linearly when you need to pair entities (campaign → its ad groups). Regex-over-every-line gives you flat sets; a running "current campaign" cursor gives you pairs. This is what made the paired Campaigns export work across two different log formats with the same code
- **File**: `backend/dma_plus_service.py:_parse_affected_entities`

## Persist small dev state (history, cache) even when the "real" store is a DB (2026-04-17)
- **Context**: DMA+ Change History lived in a module-level `deque(maxlen=200)`. Every uvicorn restart wiped it — painful during active development when I'm restarting 3-5 times per session
- **Why not Postgres**: the project *has* a remote PG (`10.1.32.9`) via `backend.database`. But a schema migration + connection pooling + audit of how other modules read history would have been overkill for ~200 small JSON-serializable records
- **What worked**: `backend/data/dma_plus_history.json` — load-on-import via `_load_history_from_disk()`, save-on-mutate via a `_history_append(entry)` helper that holds a `threading.Lock`. `clear_history()` also persists the empty state. Total ~25 lines, works instantly across restarts
- **Pattern**: for small, append-mostly, process-local state, a JSON file in `backend/data/` beats a DB table for dev ergonomics. Promote to PG later if it grows or needs cross-process sharing. The key is not fighting the in-memory deque — keep it as the source of truth during a process's life, and treat disk as the rehydration mechanism
- **Gotcha**: when you forward mutations to helpers, make sure *every* mutation site funnels through the persistent wrapper. I had three `_history.appendleft(...)` call sites (completion / cancellation / error) and had to update all of them to `_history_append(...)` — it's easy to miss one and wonder why some history survives restart but some doesn't
- **File**: `backend/dma_plus_service.py:_load_history_from_disk`, `_history_append`

## Taxonomy API: `isBiddingCategory` exists but DMA campaigns aren't on bidcat level (2026-04-17)
- **Initial hypothesis (WRONG)**: DMA campaigns in Google Ads are named `PLA/{bidcat}_{cl1}`. I built a bidcat walker and swapped the processor to use it — user tested and the deepest_cat lookup was actually correct all along. Reverted the same session
- **What the walker change was doing**: `GET /api/Categories/{id}` returns a CategoryDto with `isBiddingCategory` populated on the TOP-LEVEL object. The embedded `subCategories` array entries carry only `id, parentId, isEnabled, labels` — **no `isBiddingCategory`**. The flat `/api/Categories?rootCategoriesOnly=false` endpoint ignores the param and returns only root cats (tried three boolean encodings, all returned 32). So to enumerate bidcats you'd have to walk the tree and fetch each category's detail individually — Klussen subtree alone = ~284 GETs (~19s with Session reuse; full taxonomy ≈ 10 min cold)
- **Pattern — don't act on architectural hypotheses without verification**: the user mentioned "DMA campaigns are on bidcat level" and I immediately restructured the fetch around that claim. I should have first: (a) sampled a few Google Ads campaign names, (b) checked whether those names match deepest_cat *or* bidcat, and (c) only then made the change. Instead I spent a commit + service restart on an assumption that turned out wrong
- **If reviving this**: the bidcat walker idea works mechanically but isn't the right semantic for this processor. Keep `_fetch_subcategories_recursive` (deepest-cat) as the authoritative walker
- **File**: `backend/dma_plus_service.py:215-265` (the original deepest-cat walker, restored)

## Log truncation for display ≠ log truncation for parsing (2026-04-17)
- **Bug**: DMA+ exclusion export showed only ~22 campaigns when the run processed hundreds. Affected-entity regex was being applied to `result_data["log"]` which had already been sliced to the last 5000 chars for UI display
- **Fix**: Keep a separate `full_log = captured.getvalue()` local variable per branch; let the display `"log"` field stay truncated, but parse downstream artifacts (affected campaigns, counts, anything for export) from `full_log`
- **Pattern**: whenever you truncate stdout capture for a UI field, the parser must run on the pre-truncation value. Name the two explicitly (`full_log` vs `display_log`) so it's obvious which is which and future branches can't accidentally reuse the truncated one
- **File**: `backend/dma_plus_service.py:380-523`

## FastAPI async handlers block the event loop when they call sync I/O (2026-04-17)
- **Symptom**: DMA+ Start button felt dead — POST `/api/dma-plus/start` took minutes to return because `start_operation` synchronously called `_build_exclusion_workbook → _populate_cat_ids_sheet → _fetch_all_cat_ids_from_taxonomy_api` (a recursive Taxonomy API walk). Worse: while the event loop was blocked the Cancel button was also useless — frontend only sets `currentTaskId` *after* the start response, so cancel had nothing to target
- **Fix**: `start_operation` now only seeds the task record (`status:"queued"`) and spawns the background thread. ALL heavy work (maincat resolve, workbook build, Google Ads client init) moved into `_run_operation`. POST returns in <10ms with a task_id that Cancel can immediately target
- **Pattern**: `async def` handlers that do blocking I/O are worse than sync handlers — sync handlers get a threadpool slot, async handlers hog the loop. If a handler must do slow work, it should hand it off to a thread/task and return an ID. "The response doesn't need the result" is the tell that background execution is right
- **Cancel plumbing**: flip a flag on the task record and have the worker `_check_cancelled(task_id)` at every phase boundary. Raising a custom `TaskCancelled` exception keeps the control flow clean. Widen the allowed-to-cancel statuses to include `queued`/`initializing`, not just `running`, or users can't cancel during the slow init phase
- **Still missing**: once control is inside a monolithic sync function (e.g. `cp.process_exclusion_sheet_v2`), you can't cancel without a callback threaded through that function. Flagging this as a known limitation is better than pretending cancel works everywhere
- **File**: `backend/dma_plus_service.py:570-616, 307-395`

## xlsx over CSV kills Excel's Windows-1252 mojibake (2026-04-17)
- **Symptom**: DMA+ CSV export displayed `→` as `â†’`, `✅` as `âœ…`, etc. Backend data was correct UTF-8; Excel was opening the CSV as Windows-1252 and mis-decoding every multi-byte character
- **Fix**: Switch output to `.xlsx` via SheetJS (`XLSX.utils.aoa_to_sheet → book_append_sheet → writeFile`). Xlsx stores strings as UTF-8 inside XML parts of the zip; Excel always decodes them correctly. No BOM needed, no locale-dependence
- **Bonus**: multi-sheet support came for free — split the one noisy CSV into `Status` + `Campaigns` tabs
- **Alt fix** (kept in back pocket): prepend `\ufeff` BOM to the CSV Blob. Works but fragile — some tools strip BOMs, and it doesn't fix column-formatting issues
- **Pattern**: for any user-facing tabular export containing non-ASCII characters (arrows, emoji, accented names), default to xlsx. CSV is fine for ASCII or data pipelines that explicitly declare encoding

## Blocking `<script src>` stalls the whole page when the CDN is unreachable (2026-04-17)
- **Symptom**: Adding `<script src="https://cdn.sheetjs.com/...">` before the inline `<script>` made the Start button do nothing after page load. The inline script couldn't parse until SheetJS finished downloading, so `startOperation` was undefined when the onclick fired — silently no-op, no console error
- **Fix**: add `async` to the CDN script. It loads in parallel and doesn't hold up the inline script. Protect any callsite that needs the library with a guard: `if (typeof XLSX === 'undefined') { alert('Excel library still loading — try again in a second.'); return; }`
- **Pattern**: third-party CDN scripts should always be `async` or `defer` unless the inline script directly needs a symbol from them. The worst-case failure mode (corp network blocks the CDN) is "nothing on the page works, no error" which is painful to debug

## Two-stage cap: validate before you truncate (2026-04-17)
- **Bug**: n8n IndexNow submitter fetched top-10K URLs from Redshift, then validated supplier count. If N of those 10K failed validation, the run submitted only `10K − N` URLs — URLs 10001+ that might have passed were never considered
- **Fix**: fetch 15K (headroom), run validation on ALL of them, THEN cap to 10K. Report `rejected (<3 suppliers)` and `truncated (daily cap, post-validation)` as separate numbers in the Slack summary
- **Pattern**: when a pipeline has (1) a natural input limit and (2) a filter that discards some inputs, always filter-then-truncate, not truncate-then-filter. The other ordering silently wastes quota and is invisible unless you instrument both counts

## One repo serving two environments — env-gated features beat two forks (2026-04-15)
- **Context**: `dm-tools` (localhost, 8003, no auth) and `dm-dashboard` (networked, 3003, password-protected, Windows Task Scheduler UI) had drifted into two parallel repos with ~11 differing files. Same project, different deployment constraints. The old workflow was "commit to both repos after every change" which is error-prone and was already producing divergent features
- **Consolidation approach**: dm-tools absorbed every dashboard feature, but behind env vars:
  - `DASHBOARD_PASSWORD` empty → middleware is a pass-through; set → login required
  - `ENABLE_TASK_SCHEDULER=false` (default) → router not mounted, `/api/config` returns `task_scheduler_enabled:false`, and a small frontend script hides the Automation card in `dashboard.html`
  - `CORS_ORIGINS` unset or `*` → permissive; comma-separated hosts → restricted
  - `BASE_URL` and `DISABLE_SSL_VERIFY` in `daily_automation.py` → same script works for http://localhost:8003 and https://win-htz-006.colo.beslist.net:3003
- **Why this is better than two forks**: one code path to test, one set of commits, no "which repo has the latest fix" ambiguity. Every new feature only has to be written once
- **Gotcha**: creating tables (e.g. `scheduled_tasks`) unconditionally is fine — they're dormant when the feature is off. Mounting routers conditionally is cleaner than nesting `if env_var:` inside every handler
- **Frontend feature flags via `/api/config`**: any new env-gated UI now just `fetch('/api/config').then(cfg => { if (cfg.feature) show(el) })`. No build step needed for a vanilla-JS frontend
- **Pattern**: when two deploys of the same app diverge, unify via env flags before the drift becomes unmanageable. Each additional month of "two forks" makes the merge harder

## Git remote swap for a canonical-repo switch (2026-04-15)
- **Goal**: make `joep-1993/dm-dashboard` the canonical push target without moving files or re-cloning
- **Steps used**:
  1. Force-pushed consolidated history to dm-dashboard with `--force-with-lease=main:<old-hash>` (safety catch — fails if the remote moved since last fetch)
  2. `git remote rename origin dm-tools-old`
  3. `git remote rename dm-dashboard origin`
  4. `git branch --set-upstream-to=origin/main main`
- **Now plain `git push` / `git pull` target dm-dashboard**. The old remote is kept under a new name for reference (will be removed when dm-tools GitHub repo is archived)
- **Pitfall hit**: `--force-with-lease=main:21eb8dc` (short hash) failed with "stale info". Had to use the full 40-char hash from `git rev-parse dm-dashboard/main`. `--force-with-lease` needs the exact SHA string
- **Pattern**: use `--force-with-lease` (never plain `--force`) when rewriting shared remote history — it refuses to overwrite if someone else pushed in between

## FAQ prompt — missing facet context for filtered pages (2026-04-14)
- **Issue**: FAQs on faceted URLs (e.g. `/c/merk~819441`) had generic category questions instead of brand/facet-specific ones. Example: "AEG boormachines" page got "Wat is het voordeel van een accuboormachine?" with no mention of AEG
- **Root cause**: `selected_facets` was returned by `fetch_products_api` but never included in the FAQ prompt. The AI only saw the h1_title (which often contained the brand) but had no explicit instruction to write facet-specific questions
- **Fix**: Added `facet_context` (list of active filters) and a conditional instruction to both `faq_service.py` and `batch_api_service.py` — only injected when `selected_facets` is non-empty, so non-faceted pages are unaffected
- **Scale**: ~18K of 222K faceted FAQs (8%) had clearly generic questions (title words absent from all questions). True number likely higher since heuristic is conservative
- **Pattern**: When data is available in page_data but unused in the prompt, the AI can't be expected to know the page's filtering context. Always pass relevant metadata explicitly

## Kopteksten prompt — repetitive opening phrases (2026-04-14)
- **Issue**: Nearly all generated kopteksten started with "Bij het kiezen van een..." — monotonous output
- **Root cause**: The system prompt already banned "Als je op zoek bent naar" etc. but the model defaulted to another formulaic opener
- **Fix**: Added a soft discouragement (not a hard ban) to both subcategory and main category system prompts: "Vermijd ook om te vaak te openen met 'Bij het kiezen van' — gebruik dit hooguit af en toe, niet standaard"
- **Pattern**: When banning specific openings, the model often shifts to a new default. Soft variation rules ("use sparingly") work better than hard bans for preventing monotony without over-constraining

## Link validator — V4 UUID lookup used the wrong ES field (2026-04-13)
- **Bug**: `backend/link_validator.py:query_elasticsearch_by_plpurl` phase-1 lookup queried ES on `pimId`, but V4 UUIDs live in the `id`/`groupId` fields. `pimId` stores `nl-nl-gold-...` values, never `V4_...`. So the terms query *always* returned 0 hits
- **Impact**: *Every* V4 product link on content and FAQ pages was silently skipped by the validator — never replaced when slugs changed, never flagged `gone`, never triggering regeneration. This has been true for as long as the V4 branch existed in this file. Phase-2 wildcard fallback was disabled (it timed out), so the bug had no safety net
- **Fix**: Changed `"pimId"` → `"id"` in the terms query and source list. Also repurposed the phase-1 miss path: V4 UUIDs not returned by the `id` lookup are now marked gone (previously left out of the result dict entirely). ES request failures still skip the batch in the `except` branch, so transient errors can't cause spurious regeneration
- **Verification**: tested on `/products/mode/mode_432356/` (4 V4 links) — before: 0/0/0, after: 2 slug replacements + 2 gone URLs matching ground truth
- **Files**: `backend/link_validator.py` (one function, ~50 line diff). Same fix benefits `validate_faq_links` because it delegates to the same helper
- **Pattern**: when a schema sample disagrees with query assumptions, always verify field names against a live document before debugging around the symptoms

## thema_ads_* tables missing PKs → GROUP BY errors + broken inserts (2026-04-13)
- **Symptom**: startup log showed `Error cleaning up stale jobs: column "j.status" must appear in the GROUP BY clause`. `cleanup_stale_jobs` in `thema_ads_router.py` calls `thema_ads_service.list_jobs` which did `SELECT j.*, SUM(...) ... GROUP BY j.id`
- **Root cause**: `thema_ads_jobs` in the live DB had zero constraints — no PK, no unique, nothing. Postgres functional-dependency detection only accepts `SELECT j.*` with `GROUP BY j.id` when `id` is declared PK. The schema files (`thema_ads_schema.sql`, `thema_ads_db.py`, `database.py`) all declare `id SERIAL PRIMARY KEY`, but the live table was created without those constraints
- **Fix 1 — query**: rewrote `list_jobs` to pre-aggregate counts in a subquery on `thema_ads_job_items` and LEFT JOIN, eliminating GROUP BY on the outer query. Semantically identical
- **Fix 2 — schema**: added sequences, `SET DEFAULT nextval(...)`, PRIMARY KEYs on all three `thema_ads_*` tables (`thema_ads_jobs`, `thema_ads_job_items`, `thema_ads_input_data`), and FKs from the two child tables to `thema_ads_jobs(id)` with ON DELETE CASCADE. All three were empty so zero risk
- **Side effect fixed**: inserts without explicit id would have failed (column NOT NULL, no default). Now they auto-increment as intended
- **Pattern**: periodically verify that `CREATE TABLE IF NOT EXISTS` declarations in the code match actual live schema — the "IF NOT EXISTS" silently skips structural mismatches

## AI Titles — facets that should act as the category name (2026-04-13)
- **Context**: for some URLs the facet value is already the product noun (e.g. `t_wanddeco` → "wandplaten"), so appending the generic `category_name` ("Wanddecoratie") produces redundant titles like *"Acryl Metalen wandplaten Wanddecoratie kopen?..."*
- **Implementation**: added `CATEGORY_OVERRIDE_FACETS` set (currently `{'t_wanddeco'}`) in `improve_h1_title`. When any such facet is in `selected_facets`, the code strips `category_name` from the H1 (both prefix and suffix, case-insensitive) and clears the local `category_name` variable so downstream logic can't re-append it
- **Why a set, not a generic `t_*` rule**: some `t_*` facets are genuinely descriptors, not category-equivalents. Explicit opt-in keeps behavior predictable
- **Reset**: 61 URLs containing `t_wanddeco` reset to pending
- **Related pattern**: similar to the existing "Soort facet with product-type suffix" logic at lines 535-544, but generalized and explicit

## AI Titles — stijl adjectives must precede the noun (2026-04-13)
- **Issue**: with `stijl_test~8064049` (value "Industriële"), the AI placed the style adjective correctly ~97% of the time but sometimes at the end: *"Gouden Stoffen Verstelbare Barkrukken Industriële"*, *"Zwarte Grote Hoekbureaus 4 laden Industriële"*
- **Root cause**: prompt rule 4 only explicitly covered colors and materials. Style facets (`stijl_test`, `stijl_woonaccessoires`, `stijl`, `stijl_schoenen`, `stijl_tas`, `stijl_tegels`, `stijl_tuinart` — ~44k URLs across 7 families) had no explicit rule, so the model's placement was inconsistent
- **Fix**: extended rule 4 to name stijl adjectives with examples ("Industriële", "Moderne", "Scandinavische") and an explicit "NOOIT aan het einde van de titel" clause
- **Reset**: 1,994 URLs containing `stijl_test~8064049` (572 processed + 1,422 unprocessed) reset to pending
- **Pattern**: the AI follows explicit, example-backed rules far more reliably than implicit category-like behavior. When one facet family has a recurring placement issue, check whether the prompt names that family

## Uvicorn was running without --reload (2026-04-13)
- **Symptom**: code edits to `backend/*.py` didn't take effect until process restart
- **Cause**: scheduled-task startup script `C:\Users\JoepvanSchagen\scripts\start-dm-dashboard.ps1` ran `uvicorn backend.main:app --host 0.0.0.0 --port 8003` with no `--reload`
- **Fix**: added `--reload` to the ps1 script and restarted the process. Future edits hot-swap via WatchFiles
- **Also**: scheduled task `DM Tools Dashboard` logon trigger got a 10-minute delay (`<Delay>PT10M</Delay>`) so WSL/Ubuntu has time to be ready before uvicorn tries to bind :8003

## OpenAI Batch API — File Size Limit & Chunking (2026-04-10)
- **200MB limit** per batch file for gpt-4o-mini. 29K FAQ prompts with product data exceeded this
- **Fix**: Split into chunks of 5,000 requests each. For 29K URLs = 6 chunks, processed sequentially
- **Queue time**: OpenAI batch queue can be very slow — first run took ~8 hours (mostly queue waiting, not processing)
- **Chunk size**: 5,000 is conservative. Could potentially go higher but 200MB is hard limit

## Faulty URLs in unique_titles (2026-04-10)
- **158,742 URLs removed** from all 6 DB tables, exported to `~/faulty_unique_title_urls.xlsx`
- `/r/` URLs (143,626): product redirect URLs like `/products/fietsen/r/accu-slot/` — Product Search API can't parse these (no `/c/` facet path)
- `populaire_themas_accessoires` (8,134): invalid facet — API returns 400 "facet is not valid"
- `type_parfum` (6,901): invalid facet — same 400 error
- `pl_pennen` (90): invalid facet — same 400 error
- **Pattern**: When batch processing shows many `api_failed` or `facet_not_available` errors, check for systematic bad URL patterns to clean from DB

## Unique Titles Batch UI Race Condition (2026-04-10)
- **Problem**: Process All button turned yellow briefly, progress bar didn't show, `undefined` text appeared
- **Root cause**: `loadAiStatus()` polls `/api/ai-titles/status` every 2 seconds and resets the UI when `is_running: false`. The batch uses a separate state (`/api/ai-titles/batch-status`), so the normal status always shows idle → UI gets reset
- **Fix**: Set `aiBatchPolling = true` immediately on click (before fetch), then `loadAiStatus` returns early when flag is set. Also hide batch/workers inputs during batch run and restore in `resetAiBatchUI`

## Frontend Consistency Standards (2026-04-10)
- **Page widths**: All tools use `col-md-10 mx-auto`. Was inconsistent: unique-titles had `col-lg-8`, redirects/keyword-planner/url-checker/redirect-checker had `col-md-11`
- **Input fields**: Use `input-group` with inline label prefix (e.g., `<span class="input-group-text">Batch</span>`) — consistent across Kopteksten, FAQ, Unique Titles
- **Button alignment**: Inputs left, buttons right via `d-flex justify-content-between align-items-center`
- **Publishing section**: Dropdowns + last push timestamp left, Refresh Stats + Publish buttons right
- **Badge color**: `.badge.bg-success` was overridden to grey in `css/style.css` via `--color-section`. Fixed to `#198754` (Bootstrap green)
- **FAQ recent results X-button overflow**: Fixed with CSS grid (`grid-template-columns: 1fr auto`) + `overflow: hidden` on content div. Flexbox approach failed because long URLs could push the button out

## OpenAI Batch API Integration (2026-04-10)
- **Service**: `backend/batch_api_service.py` — bulk processing for FAQ and kopteksten via OpenAI Batch API
- **Endpoints**: `POST/GET /api/batch-start`, `POST/GET /api/faq/batch-start`, `/api/batch-status`, `/api/faq/batch-status`
- **Frontend**: "Bulk API" checkbox on FAQ (`faq.html`) and Kopteksten (`index.html`). When checked, greys out batch size, workers, single-batch button. Process All triggers batch pipeline
- **Flow**: Fetch pending URLs → Product Search API (50 threads) → build JSONL → upload to OpenAI → poll every 15s → download results → save to DB
- **Cost**: 50% cheaper than real-time API
- **Speed**: Prepare phase ~5-15 min (API calls), OpenAI processing ~15-60 min
- **State tracking**: Global `_batch_state` dict with thread lock, phases: preparing → uploading → processing → saving → complete/error

## Query Performance — LEFT JOIN vs NOT EXISTS (2026-04-10)
- **Issue**: FAQ URL selection query took 4.2s per batch due to LEFT JOIN across 3 large tables (werkvoorraad 280K, faq_tracking 255K, url_validation_tracking 86K)
- **Fix**: Converted to NOT EXISTS subqueries — 190ms (16.5x faster). Also fixed kopteksten query (7.7s → 2.9s)
- **Affected queries**: 4 total in main.py — FAQ URL selection, FAQ pending count, kopteksten URL selection, kopteksten pending count
- **Pattern**: Always prefer `NOT EXISTS` over `LEFT JOIN ... WHERE x IS NULL` on PostgreSQL for anti-joins

## Worker Limits & Connection Pool (2026-04-10)
- **OpenAI rate limits**: 30,000 RPM, 150,000,000 TPM for gpt-4o-mini — extremely generous
- **Bottleneck**: Each OpenAI call takes ~30s. With 20 workers = ~40 URLs/min. With 50 workers = ~100 URLs/min
- **Changes**: DB pool maxconn 20→60, worker limit 20→100 (backend + frontend), frontend defaults 20→50
- **Files changed**: `database.py`, `main.py` (6 validation checks), `index.html`, `faq.html`, `app.js` (4 checks), `faq.js` (3 checks)

## Winkel Facet URLs — No API Data (2026-04-10)
- **Issue**: 29,632 URLs with `winkel~` facet had titles that were just bare category names (e.g., "Zoogcompressen" instead of "Bol.com Zoogcompressen")
- **Root cause**: Product Search API returns empty facets array for winkel-filtered requests. The shop filter is applied but no facet metadata comes back
- **Decision**: Removed all winkel URLs from all 6 tables. Shop-specific pages don't need SEO titles
- **Tables cleaned**: unique_titles, content_urls_joep, kopteksten_check, faq_content, faq_tracking, werkvoorraad

## Title Scoring — Full Run Completed (2026-04-09)
- **Script**: `scripts/score_titles.py` — GPT-4o-mini, 25 titles/batch, 20 concurrent workers
- **Result**: 1,023,808 titles scored, avg 8.00. Distribution: 7.8% score 10, 29% score 9, 33.4% score 8, 17.5% score 7, 12.1% score ≤6
- **Runtime**: ~4.4 hours for 684K titles (two parallel processes), ~40-43 titles/sec, 0 errors
- **Export**: `scripts/export_scored_titles.py` → `~/unique_titles_scored.xlsx` (41MB, color-coded scores, distribution sheet)
- **Decision**: All titles scoring <7 (125,436) were reset to pending for regeneration
- **Pool exhaustion**: `maxconn=20` in `database.py` matches worker count — under peak load the pool can exhaust. Not currently blocking but could be bumped to 25

## FAQ/Kopteksten Tracking Ghost Records
- **Issue**: 45,004 URLs in `pa.faq_tracking` had status='success' but no content in `pa.faq_content`. Also 373 ghost success records in kopteksten tracking
- **Impact**: Ghost records prevented URLs from being picked up for generation (pipeline thinks they're done)
- **Fix**: Reset ghost success records to 'pending'. This explains why FAQ content count (200K) was lower than kopteksten (218K) despite same URL pool
- **Check query**: `SELECT count(*) FROM pa.faq_tracking t WHERE t.status = 'success' AND NOT EXISTS (SELECT 1 FROM pa.faq_content c WHERE c.url = t.url AND c.faq_json IS NOT NULL AND c.faq_json != '')`

## AI Title Kinder+Meisjes/Jongens Dedup Fix
- **Issue**: Titles like "Kinder Meisjes Panty's" — "Kinder" not stripped when "Meisjes" present
- **Root cause**: Dedup logic at `ai_titles_service.py:509` was facet-name-based (only matched `doelgroep`, `doelgroep mode`, `doelgroep schoenen`) but actual facets use names like `doelgroep_feestkleding`, `doelgroep_fietsen`, `doelgroep_horloge`, `dg_kind_horloge`, etc.
- **Fix**: Changed to value-based approach — any facet with value "Kinder"/"Kinderen"/"Baby" is dropped when any facet has value "Meisjes"/"Jongens". Also strips "Kinder" prefix from category names in H1 (e.g., "Kinderfietsen" → "fietsen")
- **Affected**: 403 URLs reset

## AI Title English Hallucination — "vases"
- **Issue**: GPT translated Dutch "vazen" to English "vases" in 9 titles
- **Pattern**: Original H1 had "vazen", AI output had "vases". Only found for this one word so far but worth watching
- **Fix**: Reset 9 URLs to pending. Consider a post-processing check for common Dutch→English mistranslations if pattern recurs

## Bad URL Patterns in unique_titles
- **Removed**: 1,944 URLs containing "pricemax" (2) or "+" (1,943) — these are invalid/malformed facet URLs

## Docker-Free Dashboard (dm-dashboard)
- **Repo**: `https://github.com/joep-1993/dm-dashboard` — standalone version of dm-tools without Docker
- **Key changes vs dm-tools**: Added `load_dotenv()` to `main.py`, changed default DB DSN from `db:5432` to `localhost:5432`, moved hardcoded API keys to env vars (`UNIQUE_TITLES_API_KEY`, `CONTENT_API_KEY_*` in `content_publisher.py`)
- **Password protection**: Cookie-based auth via `DASHBOARD_PASSWORD` env var. HMAC session token, 30-day cookie, middleware blocks all routes except `/login` and `/api/health`. Leave empty to disable
- **Missing files from original gitignore**: `themes.py` + `thema_ads_optimized/` (was mounted as Docker volume from `/projects/theme_ads/`), `themes/` dir (headline/description templates), `categories.xlsx` (was excluded by `*.xlsx` gitignore rule — added exception)
- **Credentials file**: `~/dm-dashboard.env` — NOT in repo, copy as `.env` on target machine
- **Both repos must be kept in sync**: When editing dashboard code, commit to both `dm-tools` and `dm-dashboard`
- **Date**: 2026-04-03

## n8n IndexNow Submitter — Slack Message Bug
- **Issue**: Slack message showed "New URLs submitted: 0" and "API response: 0" despite 10K URLs being successfully submitted to IndexNow
- **Root cause**: `build_summary1` node used `$input.all()` which receives output from `log_to_tracking_table` (Postgres node) — returns query execution results, NOT the `url_count`/`response_code` fields from `build_tracking_insert1`
- **Fix**: Changed to `$('build_tracking_insert1').all()` for url_count and `$('submit_to_indexnow1').first().json.statusCode` for response code — reference upstream nodes directly instead of relying on passthrough data
- **File**: `C:\Users\JoepvanSchagen\Downloads\indexnow_submitter (1).json`
- **Date**: 2026-04-03

## MC ID Finder Tool
- **Backend**: `backend/mc_id_finder_service.py` + `backend/mc_id_finder_router.py` — 1 endpoint under `/api/mc-id-finder/`
- **Frontend**: `frontend/mc-id-finder.html` — search by shop names (textarea, one per line) + country checkboxes (NL/BE/DE), dynamic table columns based on checked countries, CSV export
- **Redshift query**: Joins `beslistbi.hda.efficy_shop_dm` (MC IDs) with `bt.shop_main_attributes_by_day` (shop names) on `k_shop = efficy_k_shop`
- **Gotcha: `shop_name` is on `shop_main_attributes_by_day` (alias `r`), NOT on `efficy_shop_dm` (alias `m`)**. Initial query used `m.shop_name` which doesn't exist — caused "column does not exist" error
- **Gotcha: `f_mc_id_nl/be/de` are strings, not integers**. Using `> 1` worked by accident but proper filtering is `NOT IN ('','0','1')`
- **Gotcha: Many shops have `efficy_k_shop = NULL`** in `shop_main_attributes_by_day`, so the inner JOIN drops them. ~1,888 shops have working joins with MC IDs. Shops like bol.com won't appear because they lack the Efficy link
- **Date**: 2026-04-02

## GSD Campaigns Tool
- **Backend service**: `backend/gsd_campaigns_service.py` (1,247 lines) — ported from `C:\Users\JoepvanSchagen\Downloads\Python\scripts_def\create GSD-campaigns WB.py` (2,757 lines)
- **Router**: `backend/gsd_campaigns_router.py` — 7 endpoints under `/api/gsd-campaigns/`
- **Frontend**: `frontend/gsd-campaigns.html` — stats, sortable/paginated campaign table, run script, activity log
- **GAQL gotcha**: `FROM campaign_label` resource cannot be combined with metrics (clicks, cost, impressions). Must use two queries: first get campaign IDs from `campaign_label`, then fetch metrics from `campaign` with `WHERE campaign.id IN (...)`.
- **Google Ads accounts**: NL CPR=7938980174, BE CPR=2454295509, DE CPR=4192567576, NL CPC=7938980174, BE CPC=7565255758. MCC=3011145605
- **Creation label**: `GSD_SCRIPT` — used to identify campaigns created by the script
- **Credentials**: All from env vars (GOOGLE_DEVELOPER_TOKEN, GOOGLE_REFRESH_TOKEN, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_LOGIN_CUSTOMER_ID, REDSHIFT_*). Service account for Merchant Center via `GSD_SERVICE_ACCOUNT_FILE` env var
- **Date**: 2026-03-31

## Frontend Redesign — Dropdown Menu + Dashboard
- **Menu system**: 4 dropdown categories (Generators, Indexation, Google Ads, SEO tools) + Dashboard button. CSS in `frontend/css/style.css` with `.nav-dropdown*` classes
- **Dashboard**: `frontend/dashboard.html` — categorized tool cards with color-coded icon backgrounds (purple=Generators, blue=Indexation, coral=Google Ads, green=SEO tools), stroke-style SVG icons, hover effects (icon becomes outlined, shadow intensifies)
- **Responsive**: Top-level menu items and dropdown items scale down at breakpoints (1600/1200/992px)
- **Tool renames**: Kopteksten Generator→Kopteksten, FAQ Generator→FAQ's, 301 Generator→Redirects, Canonical Generator→Canonicals, SEO Index Checker→Index Checker
- **Date**: 2026-03-31

## FAQ Structured Data — Item Name Fix
- **Issue**: Google Rich Results Tool / Search Console showed "item name: N/A" for FAQ structured data
- **Root cause**: `FAQPage` JSON-LD had no `"name"` property at the top level — only the nested `Question` objects had `"name"`
- **Fix (new FAQs)**: Added `"name": self.page_title` to `FAQPage.to_schema_org()` in `backend/faq_service.py:95`
- **Fix (existing FAQs)**: Pure SQL migration using `replace()` to inject `"name": "<page_title>"` after `"@type": "FAQPage"` in the `schema_org` column. Script: `backend/fix_faq_sql.py`. Updated 204,216 rows
- **Gotcha**: Python `-c` inline scripts have double-quote escaping issues with SQL — use a `.py` file instead
- **Gotcha**: Row-by-row UPDATE of 204K rows over remote DB is extremely slow — use single SQL `replace()` statement for bulk string manipulation in JSON columns
- **Date**: 2026-03-31

## DMA Bid Strategy — ROAS Condition
- **Change**: Added ROAS >= 130% as additional condition for bid strategy increases (L1→L2, L2→L3)
- **ROAS formula**: DMA/CLA omzet (from "Omzet DMA en CLA" conversion action) / cost
- **Verified**: Campaign Auto's_a (21806762283) on 2026-03-19 returned ROAS 71.49% — matched Google Ads UI
- **Files**: `DMA_verhogingen_verlagingen.py` — added `roas` to data collection, increase conditions, email headers, CSV
- **Date**: 2026-03-31

## Basements Homepage — Simplified n8n Workflow
- **File**: `C:\Users\JoepvanSchagen\Downloads\basements_homepage_simple.json`
- **Purpose**: Simpler version of `basements_homepage.json` that skips DB table writes and posts directly to keywords API
- **Key difference**: Original writes to `pa.basements_hp_joep` table, checks redirects via DB updates, then reads back and posts. Simplified version processes everything in-memory
- **Homepage basement**: ALL URLs must be posted as `deepestCategoryId: 0` (homepage), not per cat_id from Redshift
- **n8n SplitInBatches gotcha**: "done" output only passes through items from the last batch, not all accumulated items. For ~100 items, skip SplitInBatches entirely — HTTP Request node iterates over all items automatically
- **n8n HTTP body gotcha**: `contentType: "raw"` with `body: "={{$json}}"` may not serialize objects correctly. Pre-stringify with `JSON.stringify()` in a Code node and pass the string as body
- **Redshift query returns strings**: `cat_id` and `order` come back as strings — cast with `Number()` before posting to keywords API
- **Date**: 2026-03-31

## DMA Bid Strategy Automation Script
- **Location**: `C:\Users\JoepvanSchagen\Downloads\Python\scripts_def\DMA_verhogingen_verlagingen.py`
- **Purpose**: Automatically adjust DMA campaign bid strategies (Level 1/2/3) based on profit, OPB, and clicks
- **Account**: 3800751597 (DMA NL), MCC: 3011145605
- **Bid strategies**: `DMA: Level 1 - 0,07`, `DMA: Level 2 - 0,11`, `DMA: Level 3 - 0,15` — defined in MCC account but referenced by sub-account campaigns with different customer prefix in resource name (must match on strategy ID, not full resource name)
- **DMA/CLA Profit formula**: Conversion value of "Omzet DMA en CLA" conversion action - cost. Queried via Google Ads API `metrics.all_conversions_value` filtered by `segments.conversion_action_name = 'Omzet DMA en CLA'`
- **OPB (Conv.-waarde/klik)**: Standard metric calculated as `conversions_value / clicks` — despite appearing as custom column in Google Ads UI, it's derivable from standard metrics
- **Google Ads API custom columns**: NOT queryable via GAQL (`custom_column` resource doesn't exist in Google Ads API v22). `CustomColumnService` also removed in v22. Custom columns are UI-only; replicate their formulas using standard metrics instead
- **SA360 API**: Can query custom columns via `custom_columns.id[{id}]` syntax, but only for columns defined in SA360 (not Google Ads custom columns)
- **Cross-account bid strategies**: MCC bid strategies have resource names like `customers/3011145605/biddingStrategies/123`, but campaigns reference them as `customers/3800751597/biddingStrategies/123`. Match on the numeric strategy ID only, not the full resource name
- **Credentials**: Uses env vars `GOOGLE_CLIENT_ID`/`GOOGLE_CLIENT_SECRET` + hardcoded refresh token (different from GSD script credentials)
- **DRY_RUN mode**: Set `DRY_RUN = True` to skip actual bid strategy changes but still fetch data, evaluate rules, and send email report
- **Test script**: `test_dma_profit.py` — verifies DMA/CLA Profit calculation matches Google Ads UI for a specific campaign/date
- **Date**: 2026-03-30

## IndexNow n8n Workflow Fix
- **File**: `docs/indexnow_n8n.json`
- **Issue**: `submit_to_indexnow` HTTP Request node showed no output — IndexNow API returns HTTP 200 with empty body on success
- **Fix**: Enabled `fullResponse` option on HTTP Request node so status code is visible in output. Fixed `build_tracking_insert` to read URLs from upstream `has_urls?` node (not from HTTP response body, which is empty)
- **Date**: 2026-03-30

## CloudFront Log Downloader (cloudfront-logs project)
- **Location**: `/home/joepvanschagen/projects/cloudfront-logs/`
- **Purpose**: Python script to download CloudFront access logs from S3 bucket
- **S3 Bucket**: `production-projectstack-1hts6sh41-logbucketbucket-10tf48d8lt2pt`
- **AWS Credentials**: stored in `dm-tools/scripts/amazon_cred` (access key + secret)
- **Script**: `download_cloudfront_logs.py` — self-contained, no config file needed (credentials + bucket as constants)
- **Features**: date filter, from_date (all logs from date onwards), days (last N days), list_only mode, skip already-downloaded files (resume-safe), configurable download_dir
- **Default download dir**: `C:\Users\JoepvanSchagen\Downloads\Cloudfront`
- **Run from**: PyCharm (WSL-based), call `main()` with keyword args
- **Scale**: ~67K log files in bucket (as of 2026-03-26)
- **Date**: 2026-03-26

## Shared URL Validation Tracking (pa.url_validation_tracking)
- **Purpose**: Unified table for tracking skipped URLs (`no_products_found`) across both kopteksten and FAQ features
- **Problem**: Previously, kopteksten used `pa.jvs_seo_werkvoorraad_kopteksten_check` and FAQ used `pa.faq_tracking` separately for tracking skipped URLs. This caused different "Skipped" counts in the frontend dashboard (e.g., kopteksten showing 58K skipped, FAQ showing 62K skipped)
- **Solution**: Created `pa.url_validation_tracking` with columns: `url` (PK), `status` (skipped/rechecked), `skip_reason`, `feature_source` (kopteksten/faq/both), `created_at`, `updated_at`
- **What goes in shared table**: Only `no_products_found` skips — these are URL-level issues (URL has no products), not feature-specific
- **What stays in feature tables**: Feature-specific failures (`no_valid_links`, `ai_generation_error`, `faq_generation_failed`) remain in `pa.jvs_seo_werkvoorraad_kopteksten_check` and `pa.faq_tracking`
- **Status endpoints**: Both `/api/status` and `/api/faq/status` now read skipped count from the shared table, ensuring identical numbers
- **Total count formula**: `total = processed + skipped + failed + pending` — always adds up
- **Recheck**: Both recheck endpoints operate on the shared table. FAQ recheck delegates to kopteksten recheck
- **Migration**: `backend/migrate_shared_validation.py` merges existing skipped URLs from both feature tables into the shared table
- **Files changed**: `backend/schema.sql`, `backend/database.py`, `backend/main.py`, `backend/link_validator.py`, `backend/migrate_shared_validation.py` (new)
- **Date**: 2026-03-20

## Taxonomy API v2 — Facet & Category Management
- **Base URL**: `http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl`
- **Auth**: None needed from internal network (JWT Bearer in spec but not enforced)
- **Spec**: `scripts/swagger_taxv2.json`
- **Key fields**: `noIndexNoFollow` (on facet), `seoPriority` (on category-facet setting or facet value)
- **seoPriority status** (2026-03-17): Not set anywhere in production — all null/inherit across 3,575 categories
- **Upsert seoPriority**: `PUT /api/CategoryFacetSettings` with `{"categoryId": int, "facetId": int, "seoPriority": bool}`
- **Categories have nl-NL labels** with `name` + `urlSlug` — fetch via `GET /api/Categories/{id}`
- **Facets have nl-NL labels** — fetch via `GET /api/Facets/{id}` or search `GET /api/Facets?searchTerm=...`
- **Full docs**: See `docs/ARCHITECTURE.md` → "Beslist Taxonomy API v2" section
- **PUT /api/Facets/values/{id} clears omitted fields**: When updating a facet value, always include ALL fields in the body (nameInColumn, nameOnDetail, seoPriority). Omitted fields are reset to empty strings. Always GET the current value first, then merge your changes before PUTting.
- **Date**: 2026-03-17, updated 2026-03-25

## categories.xlsx is gitignored — regenerate from DB if missing
- **File**: `backend/categories.xlsx` — loaded at startup by `category_keyword_service.py`
- **Source**: `SELECT main_category_name, category_id, category_name FROM category_descriptions` (remote DB)
- **Columns**: maincat, maincat_id (MIN category_id per main_category), deepest_cat, cat_id
- **Impact**: Backend crash-loops with `FileNotFoundError` if missing
- **Date**: 2026-02-20

## pa.unique_titles had no UNIQUE constraint — duplicates accumulated
- **Root cause**: No PK or unique index on `url` column → same URLs inserted multiple times
- **Fix**: Deduped via `CREATE TABLE ... AS SELECT DISTINCT ON (url)`, swapped tables, added `CREATE UNIQUE INDEX idx_unique_titles_url ON pa.unique_titles (url)`
- **Scale**: 361,861 duplicate rows removed (1,016,763 → 654,902), then restored 267,031 missing from local DB → final 1,035,455
- **Date**: 2026-02-20

## Feb 19 migration missed unique_titles data
- Local DB (`seo_tools_db` container, `pa.unique_titles`) had 843,812 URLs
- Remote DB only received 654,902 — **267,031 URLs were never synced**
- Also 113,522 URLs in werkvoorraad were never in unique_titles at all
- **Always check local DB** (`docker exec seo_tools_db psql -U postgres -d seo_tools`) for missing data after migration issues
- **Date**: 2026-02-21

## Content publishing: timeout and serialization
- Full payload is ~1.36 GB (252K items) — takes ~10 min to upload
- DB fetch takes ~7 min (FULL OUTER JOIN content_urls_joep + faq_content)
- **Must use `data=payload_json`** not `json=payload` (avoids requests double-serialization)
- **Timeout must be ≥1800s** (old 600s was killing uploads mid-way)
- Staging environment may be down (504) — test against production directly
- The unique_titles publish (`/api/unique-titles/publish`) is a SEPARATE endpoint from content publish — uploads CSV to `custom-title-description/import-per-url`
- **Date**: 2026-02-21

## Database Connection Quick Reference

### Primary Database (used by BOTH dm-tools app AND n8n workflows)
- **Host**: `10.1.32.9` (internal: `n8n-vector-db-rw.n8n.svc.cluster.local`)
- **Database**: `n8n-vector-db`
- **User**: `dbadmin` / **Password**: `Q9fGRKtUdvdtxsiCM12HeFe0Nki0PvmjZRFLZ9ArmlWdMnDQXX8SdxKnPniqGmq6`
- **Schema**: `pa`
- **Access**: `docker exec -e PGPASSWORD='Q9fGRKtUdvdtxsiCM12HeFe0Nki0PvmjZRFLZ9ArmlWdMnDQXX8SdxKnPniqGmq6' seo_tools_db psql -h 10.1.32.9 -U dbadmin -d n8n-vector-db -c "SELECT ..."`
- **Changed**: 2026-02-19 — migrated from local seo_tools_db to remote DB so n8n can run without laptop

### Local Docker DB (still running but no longer primary)
- **Container**: `seo_tools_db` (exposed on port 5433)
- **Database**: `seo_tools`
- **User**: `postgres` / **Password**: `postgres`

### Redshift (LEGACY - not actively used, USE_REDSHIFT_OUTPUT=false)
- **Credentials**: See `.env` file in dm-tools project

**IMPORTANT**: Frontend, backend, AND n8n all use the remote DB at 10.1.32.9. The local seo_tools_db container is still running but is no longer the primary database.

## Category Lookup from CSV (cat_urls.csv)
- **Problem**: Category name was derived from first product's `categories` array in API response — could be wrong (e.g., robot vacuum cleaners instead of regular vacuum cleaners)
- **Solution**: `backend/data/cat_urls.csv` (3,557 rows, `;`-delimited) maps URL parts to category names. Loaded lazily by `backend/category_lookup.py`
- **CSV columns**: `maincat;deepest_cat;url_name;cat_id` — `url_name` like `/meubilair_389369/` matched against parsed URL `category` variable
- **Usage**: Both `scraper_service.py` (kopteksten) and `faq_service.py` (FAQ) call `lookup_category(main_category, category)` before falling back to API-derived category
- **Fallback**: Top-level pages (`category=None`) and unknown URL parts fall back to old behavior (first product's categories)
- **Files**: `backend/category_lookup.py`, `backend/data/cat_urls.csv`, `backend/scraper_service.py`, `backend/faq_service.py`
- **Date**: 2026-03-19

## Main Category URLs: Separate Content Generation Path
- **Problem**: 31 main category URLs (`/products/{maincat}/`) have no subcategory — products span many different subcategories, so the API-derived h1_title/product_subject is wrong (picks deepest category from first product)
- **Solution**: Added `MAIN_CATEGORY_H1` mapping in `scraper_service.py` with fixed H1 titles from `maincaturls.xlsx`. `is_main_category_url()` detects these URLs (no subcategory, no filters). `generate_main_category_content()` in `gpt_service.py` uses a broader introductory prompt
- **Prompt rules**: No "Welkom op de ... pagina", no "ons/onze/wij/we", broader overview mentioning subcategories, 2-4 product links from diverse subcategories
- **Database**: URLs in `pa.jvs_seo_werkvoorraad` (for kopteksten processing), excluded from FAQ via `pa.faq_tracking` with `status='skipped', skip_reason='main_category_url'`
- **Files**: `scraper_service.py`, `gpt_service.py`, `main.py`
- **Date**: 2026-03-17

## Beslist.nl Product Count Extraction
- **Pattern**: `"productCount":(\d+),"selected":true` — finds the product count of selected facets in the embedded JSON
- **Context**: Beslist pages embed facet data as JSON in the HTML. Every facet value has a `productCount`, but only the `selected:true` ones reflect the current page's result count
- **Multiple matches**: When multiple facets are selected, they all share the same productCount — take the first match
- **File**: `backend/main.py` — function `extract_product_count()`
- **Date**: 2026-02-13

## N8N Workflow Conversion from Python Scripts
- **Pattern**: When converting Python backend logic to n8n workflows, use Code nodes for complex logic (URL parsing, API response processing, content post-processing) and native n8n nodes for simple operations (DB queries, HTTP requests, OpenAI calls)
- **Node type versions**: Match existing workflows for compatibility (postgres v2.6, openAi v1.8, code v2, if v2.2, splitInBatches v3, httpRequest v4.2, scheduleTrigger v1.2)
- **Error handling**: Use `onError: "continueRegularOutput"` on all nodes — WARNING: this silently swallows errors! Check node outputs carefully when debugging
- **ES in n8n**: Use `fetch()` API in Code nodes for Elasticsearch HTTP queries (n8n Code nodes support fetch natively)
- **n8n Code node limitations**: `URLSearchParams` is NOT available — use manual `encodeURIComponent()` + string concatenation. `new URL()` IS available for URL parsing.
- **Bulk DB writes**: Don't use per-item Postgres nodes inside SplitInBatches loops — use pure SQL with `INSERT INTO ... SELECT` or run bulk queries in parallel from the source node. Data references like `$('nodeName').all()` don't work from the loop's "done" branch.
- **n8n vector DB was missing PKs/sequences**: Tables synced from Redshift had no primary keys, unique constraints, or auto-increment sequences. Had to add these manually for INSERT operations to work.
- **Files**: `docs/kopteksten_generator_n8n.json`, `docs/link_validator_n8n.json`
- **Date**: 2026-02-19

## N8N Code Node Capabilities and Limitations
- **Available in Code nodes**: `$input`, `$json`, `$env`, `fetch()`, `DateTime`, `console`, `new URL()`, `$('nodeName').all()`
- **NOT available in Code nodes**: `$helpers`, `$helpers.httpRequestWithAuthentication`, credential access, `URLSearchParams`
- **For HTTP requests**: Must use `fetch()` with manual auth headers (no credential helper access)
- **For OpenAI in Code nodes**: Call the API directly via `fetch('https://api.openai.com/v1/chat/completions', {...})` with manual `Authorization: Bearer ${apiKey}` header
- **Environment variables**: Access via `$env.VARIABLE_NAME` — OPENAI_API_KEY must be set as n8n environment variable on the server
- **Parallel HTTP pattern**: Use `async function` + `Promise.all()` with a concurrency limiter:
  ```javascript
  async function withConcurrency(items, limit, fn) {
    const results = [];
    let index = 0;
    async function worker() {
      while (index < items.length) {
        const i = index++;
        results[i] = await fn(items[i], i);
      }
    }
    await Promise.all(Array.from({length: Math.min(limit, items.length)}, () => worker()));
    return results;
  }
  ```
- **Date**: 2026-02-19

## N8N Flow Optimization: Bulk Operations Pattern
- **Problem**: SplitInBatches loops with per-item DB queries and API calls are extremely slow in n8n
- **Solution**: Replace loops with single Code nodes that do ALL work internally using bulk operations
- **Link Validation optimization**:
  - ONE Elasticsearch query per maincat instead of per URL (was ~100+ queries, now ~31 max)
  - All DB operations use bulk SQL (7 queries instead of ~100 per-item queries)
  - Removed 14 nodes, replaced with 7 bulk nodes
- **Kopteksten Generation optimization**:
  - `fetch_all_products`: Parallel Product Search API calls (5 concurrent) in single Code node
  - `generate_all_content`: Parallel OpenAI API calls (3 concurrent) via fetch() in single Code node
  - Bulk DB writes for write_result and remove_from_check
  - Removed SplitInBatches loop entirely
  - Reduced maxTokens from 2000 to 1000
  - Total nodes reduced from 35 to 20
- **Key lesson**: n8n is much faster when heavy logic is inside Code nodes with parallel processing, rather than using SplitInBatches with many sequential nodes
- **Date**: 2026-02-19

## N8N Production Push to Beslist API
- **Purpose**: Push generated SEO content (content_top, content_bottom, content_faq) to production website
- **API endpoint**: `POST https://website-configuration.api.beslist.nl/automated-content`
- **Auth header**: `X-Api-Key: Sectional~Publisher~Dumpling1`
- **Data transformation** (handled by `content_publisher.py`):
  - `content_top`: From `content_urls_joep.content`, sanitized ('' → ' → &#39;)
  - `content_bottom`: From FAQ Q&As with internal beslist.nl links only
  - `content_faq`: From FAQ schema.org JSON-LD
- **CRITICAL: Send ALL items in a SINGLE request** — the API replaces ALL content per call
- **n8n approach**: `push_to_production` Code node calls FastAPI `POST /api/content-publish?environment=production`, polls for completion (backend handles DB fetch + payload build + API call)
- **Date**: 2026-02-19, updated 2026-02-23

## N8N Postgres Node: NEVER use queryBatching "independently" with dynamic content
- **Problem**: `queryBatching: "independently"` naively splits SQL on semicolons — including semicolons inside string literals (HTML, CSS, JSON content)
- **Impact**: INSERT/UPDATE statements with HTML content (e.g., `style="color: red; font-size: 12px"`) get split into broken fragments
- **Fix**: Remove `queryBatching: "independently"` from ALL exec nodes that run dynamically built SQL. These nodes each execute a single SQL statement, so batching is unnecessary
- **Date**: 2026-02-21

## N8N Postgres Node: Chained exec nodes lose $json context
- **Problem**: When Postgres exec nodes are chained (A → B → C), node B's output replaces `$json` for node C. So `$json.field` in C references B's query result, not the original Code node output
- **Fix**: Use `$node["sourceCodeNode"].json.field` instead of `$json.field` for all chained Postgres nodes after the first one
- **Exception**: The FIRST exec node after a Code node CAN use `$json.field` since it receives data directly
- **Date**: 2026-02-21

## Unique Titles Publish: Case-sensitive duplicates cause API failure
- **Problem**: `pa.unique_titles` uses PostgreSQL (case-sensitive PK), but the API's MySQL has case-insensitive unique constraint on `url`
- **Impact**: URLs like `/c/dGVsZXZpc2` and `/c/dgvszxzpc2` coexist in PG but MySQL rejects the CSV with "Duplicate entry" error
- **Fix**: Deleted 422 mixed-case duplicates, lowercased remaining 72 URLs with caps. The `upsert_title()` function already lowercases on insert
- **All URLs in unique_titles should be lowercase**
- **Date**: 2026-02-21

## N8N Workflows: 5 individual flows + 1 combined pipeline
- **Individual workflows** in `Downloads/flows/`:
  1. `1_content_generator.json` — 50K URL content generation (10:00)
  2. `2_seo_link_validator.json` — 50K SEO link validation (14:00)
  3. `3_faq_link_validator.json` — 50K FAQ link validation (15:00)
  4. `4_publisher.json` — Publish SEO+FAQ to production (18:00)
  5. `5_faq_generator.json` — 50K FAQ generation (12:00)
- **Combined workflow**: `seo_content_pipeline.json` — 30 nodes, all 5 phases sequential in one flow (Schedule 10:00 → SEO validate → SEO generate → FAQ validate → FAQ generate → publish → Slack)
- **Date**: 2026-02-21, combined 2026-02-23

## N8N Publishing: Delegate to FastAPI backend to avoid OOM
- **Problem**: Publishing 244K rows creates ~1GB JSON payload. n8n Code node OOMs building contentItems array + JSON.stringify + fetch body (~3-4GB peak). PostgreSQL `json_agg` also OOMs at 1GB text buffer limit
- **Solution**: n8n Code node calls `POST http://app:8003/api/content-publish?environment=production`, then polls `GET /api/content-publish/status/{taskId}` every 15s (up to 40 min). Backend (`content_publisher.py`) handles DB fetch, payload build, and API call in Python where memory management is better
- **Key**: The Beslist API replaces ALL content per call — no append/upsert. Must send everything in one request
- **Output fields**: `success`, `total_urls`, `total_published` — matches Slack message template
- **Date**: 2026-02-23

## N8N OpenAI API Key: Hardcode in Code nodes
- **Problem**: `process.env.OPENAI_API_KEY` in n8n Code nodes may throw ReferenceError (process not available) or return undefined
- **Fix**: Hardcode the key directly: `const OPENAI_API_KEY = 'sk-proj-...'`
- **Updated in**: `generate_all_content`, `generate_all_faqs` (both combined and individual workflows)
- **Date**: 2026-02-23

## ON CONFLICT Requires UNIQUE Constraint in PostgreSQL
- **Problem**: `INSERT ... ON CONFLICT (url) DO UPDATE` silently does a plain INSERT when there is no UNIQUE constraint or index on the `url` column
- **Root cause**: PostgreSQL's ON CONFLICT clause needs a unique index/constraint to detect conflicts. Without it, no conflict is ever detected, so every INSERT succeeds — creating duplicates
- **Context**: Tables migrated from Redshift to PostgreSQL lose all constraints (primary keys, unique indexes, foreign keys). Redshift supports these syntactically but doesn't enforce them, so they're often missing in the source DDL
- **Impact**: `faq_content` had 79,523 duplicate rows (241,033 total, 161,510 unique). `faq_tracking` had 94,387 duplicate rows (243,671 total, 149,284 unique)
- **Fix**:
  1. Deduplicate existing data: `DELETE FROM table WHERE ctid NOT IN (SELECT MIN(ctid) FROM table GROUP BY url)`
  2. Add UNIQUE constraint: `ALTER TABLE table ADD CONSTRAINT table_url_unique UNIQUE (url)`
  3. Add ON CONFLICT to INSERT: `INSERT INTO table (url, ...) VALUES (...) ON CONFLICT (url) DO UPDATE SET ...`
- **Key lesson**: After migrating tables from Redshift (or any system), ALWAYS verify and re-add: primary keys, unique constraints, indexes, and foreign keys
- **Date**: 2026-02-19

## AI Title Generation: Met-Feature Duplication Fix
- **Problem**: Met-feature values (e.g., "Korte mouwen") appeared twice in titles — once in the base H1 from the API (e.g., "Korte mouwen nachthemden") and again as a "met" clause ("met Korte mouwen")
- **Root Cause**: Size and suffix values were stripped from `ai_h1` before sending to OpenAI, but met-feature values were NOT stripped. The AI saw "Korte mouwen nachthemden" AND received instructions to add "met korte mouwen"
- **Fix**: After classifying met-features, strip them from `ai_h1` using case-insensitive regex replace (handles "met "/"zonder " prefixed values too). Applied to ALL met-features: mouwen, capuchon, rits, knopen, veters, strepen, print, etc.
- **Scale**: 106 affected URLs found and reset (17 for korte mouwen + 89 for other met-features)
- **File**: `backend/ai_titles_service.py` — lines 593-610
- **Date**: 2026-02-12

## AI Title Generation: Met-Feature by Facet Name (Materiaal Band)
- **Problem**: Facet `m_band` (URL parameter) has API facet name "Materiaal band" — not "m_band". Code was checking `fname == 'm_band'` which never matched
- **Lesson**: Always check the actual API facet name (via `fetch_products_api()`) rather than assuming it matches the URL parameter name
- **Fix**: Changed check to `fname == 'materiaal band'`
- **Scale**: 356 URLs reset
- **File**: `backend/ai_titles_service.py`
- **Date**: 2026-02-12

## AI Title Generation: Vermogen/Power Facets as Spec Values
- **Problem**: Facet `watt_frituurpannen` (API name "Vermogen (Watt)") with values like "2001 tot 3000" wasn't detected as a spec value. The range format without unit didn't match the number+unit regex
- **Fix**: Added `fname.startswith('vermogen')` to `is_spec_value()` — catches all power/output facets regardless of value format
- **Scale**: 121 URLs reset
- **File**: `backend/ai_titles_service.py` — function `is_spec_value()`
- **Date**: 2026-02-12

## AI Title Generation: Soort Facet Category Replacement
- **Problem**: "Soort" facets with product-type values (e.g., "Parka jassen", "Bomberjacks") created redundant titles: "G-Star Parka jassen jacks" where "jacks" is the generic category already superseded by the specific Soort value
- **Detection**: The API returns `category_name` (e.g., "Jacks", "Winterjassen") separately. When a Soort facet's value ends with a product type suffix, the trailing category name in the H1 is redundant
- **Fix**: Detect Soort facets whose last word ends with a product type suffix (jassen, jacks, broeken, shirts, schoenen, jurken, truien, etc.). Strip trailing `category_name` from `api_h1` using case-insensitive regex
- **Key**: Uses `endswith()` on the last word of the Soort value against a tuple of ~30 common Dutch product type suffixes
- **File**: `backend/ai_titles_service.py` — between facet dedup and facet classification sections
- **Date**: 2026-02-12

## IndexNow: Migrated from Redshift to Local PostgreSQL
- **Problem**: IndexNow service used Redshift for URL deduplication tracking. `SELECT DISTINCT url FROM pa.index_now_joep` on 800K+ rows was slow and competed with other Redshift queries, causing the frontend to become unresponsive
- **Fix**: Switched all IndexNow DB operations from `get_redshift_connection()` to `get_db_connection()` (local PostgreSQL). Added proper indexes (`idx_indexnow_url`, `idx_indexnow_date`). Migrated 813,978 rows from Redshift
- **Daily limit**: Added 10K daily URL limit with counter, progress bar in UI, and enforcement in `submit_urls()`
- **UI improvements**: Auto-loading history on page load, submission details (new/submitted/skipped/truncated), auto-refresh history after submission
- **File**: `backend/indexnow_service.py`
- **Date**: 2026-02-12

## Winkel (Shop) Facet URLs Are Useless for AI Titles
- **Problem**: URLs with `winkel~` facets return no selected facets from the Product Search API — only the bare category name. Results in empty titles like "bedden"
- **Action**: Deleted 48,578 winkel URLs from `pa.unique_titles`. These should not be processed for AI titles
- **Date**: 2026-02-12

## AI Title Generation: Code-Level Facet Classification
- **Problem**: OpenAI (gpt-4o-mini) persistently adds "met" before sizes ("met Maat L", "met Grote maten") despite extensive prompt rules forbidding it. Prompt-only fixes failed after 5+ iterations.
- **Solution**: Moved facet handling from prompt rules to Python code preprocessing in `generate_title_from_api()`:
  1. **Spec/size auto-detection** (`is_spec_value()`): Automatically detects values that belong at the end of the title using regex pattern matching — number+unit (liter, watt, cm, kg, persoons, etc.), bare numbers, size abbreviations (S/M/L/XL), "Maat X"/"Wijdte X", "Grote/Kleine maten", and maat/wijdte facet name fallback. No hardcoded facet name list needed.
  2. **Bare number "Maat" prefix**: Numbers from maat facets get "Maat" prepended (e.g., "57" → "Maat 57")
  3. **Met-feature pre-combination**: Feature values are pre-combined into a ready-made clause (e.g., "met korte mouwen, print en borstzak") and passed as an exact string for the AI to use.
  4. **Conditional met rule**: When no features exist, prompt says "Voeg NOOIT 'met' toe". When features exist, provides exact clause to copy.
  5. **Value-based met-classification** (not facet-name-based):
     - API `detail_value` starting with "met "/"zonder " → automatic met_values
     - Small hardcoded set of feature values needing "met" added: mouwen, capuchon, hals, rits, knopen, veters, draaiplateau, grill, strepen
     - Values ending with "print" (e.g., "panterprint", "dierenprint") get "met" automatically; without "print" suffix they're treated as adjectives (e.g., "panter t-shirt" not "t-shirt met panter")
     - Opties/functies/features facets are NOT blanket-classified (some are adjectives like "Ademende", "Hittebestendige")
     - Everything else → regular (adjective before product name)
  6. **Brand deduplication**: If Merk value appears inside another facet (e.g., Merk="Epson" + Productlijn="Epson EcoTank"), standalone brand facet is dropped
  7. **Color deduplication**: If both Kleur and Kleurtint*/Kleurcombinati* are present, base color dropped in favor of specific shade/combination
  8. **Audience deduplication**: If general audience (Kinder/Baby) + specific (Meisjes/Jongens) both present, general is dropped
  9. **Hallucination removal**: Post-processing strips Heren/Dames/Kinderen/Nieuwe etc. from output if not present in input. Recognizes inflected forms (Nieuw→Nieuwe) to avoid stripping valid adjective inflections.
  10. **Trailing "met" safety net**: Strips dangling " met" from AI output before appending
  11. **Suffix values**: Color combinations (Kleurcombinaties), "Volwassenen" (levensfase), and "Vanaf X jaar" (geschikte_leeftijd) appended after title but before size values
  12. **First letter capitalization**: Ensures title starts with capital, checks against `lead_values` (brand/productlijn) for intentional lowercase (e.g., "iPhone")
  13. **Category name fallback**: Appends deepest category name when missing from H1 (e.g., "Vrijstaande 23 liter" → "Vrijstaande 23 liter magnetrons")
  14. **Adjective inflection prompt rule**: Rule 10 tells AI to inflect adjectives correctly ("Nieuw" → "Nieuwe")
  15. **Brand/productlijn strip-and-prepend**: Brand and productlijn are stripped from AI input and prepended in code after, preventing AI from misplacing multi-word brands like "The Indian Maharadja"
  16. **Color before audience in prompt**: Prompt rules 4+5 specify colors/materials come before audience ("blauwe Heren hoodies" not "Heren blauwe hoodies")
- **Key lesson**: When LLM prompt rules fail repeatedly for a specific pattern, move that logic to deterministic code. Code-level preprocessing is 100% reliable vs prompt rules being probabilistic.
- **Key lesson 2**: Don't classify entire facet groups (opties/functies) as met-features — they contain both adjectives ("Ademende", "Hittebestendige") and nouns ("Draaiplateau"). Use value-based classification.
- **Key lesson 3**: Auto-detect spec/size values with regex (number+unit) instead of hardcoding facet names — more robust across categories.
- **File**: `backend/ai_titles_service.py` — function `generate_title_from_api()`
- **Date**: 2026-02-11

## Category Depth-Based Extraction for AI Titles
- **Problem**: AI titles used wrong category name for parent-level URLs. E.g., `/products/huis_tuin/huis_tuin_505061/c/...` (Badkameraccessoires) got title saying "douchegordijnen" (a child subcategory)
- **Root Cause**: `faq_service.py` extracted `categories[-1]` (deepest product category) instead of the URL's own category level. Products belong to deep subcategories, but the URL targets a higher level.
- **Fix**: Count numeric sub-IDs in URL's category segment to determine depth, then use `categories[url_depth]` instead of `categories[-1]`
  - `huis_tuin_505061` → 1 sub-ID → `categories[1]` = "Badkameraccessoires"
  - `huis_tuin_505061_505308` → 2 sub-IDs → `categories[2]` = "Douchegordijnen"
- **Exception**: URLs with type facets (t_, type_) are correct because type facets replace the category name (e.g., `t_transportwagens` overrides the URL's category)
- **Reset**: 8,109 parent-level URLs without type facets reset to pending for reprocessing
- **File**: `backend/faq_service.py` — function `fetch_products_api()`
- **Date**: 2026-02-11

## Bad URL Detection: facet_not_available (400) from Product Search API
- **Problem**: ~3-5% of pending URLs in `unique_titles` return HTTP 400 (`facet_not_available`) from the Product Search API — facets or categories that no longer exist
- **Detection**: `backend/find_bad_urls.py` — checks each URL against `productsearch-v2.api.beslist.nl` with 20 parallel workers, flags 400 responses
- **Scale**: Partial scan of 155K/916K URLs found ~4,589 bad URLs. Bad rate varies by alphabetical range (0.1-18% per batch)
- **Pattern**: These are redirect source URLs (old facet IDs, renamed categories) that should have been caught by the Redirects Admin sheet
- **Tables to clean**: `pa.unique_titles`, `pa.jvs_seo_werkvoorraad`, `pa.jvs_seo_werkvoorraad_kopteksten_check`, `pa.faq_tracking`, `pa.faq_content`
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
- **UNIT→SUBDIVISION conversion pattern** — when a node (e.g. CL4) is a UNIT but needs children added (e.g. CL1), you must: (1) REMOVE the old UNIT, (2) CREATE a SUBDIVISION with same dimension/parent, (3) CREATE children under the new SUBDIVISION — all in one atomic mutate. Used in both `_add_cl0_exclusion_to_ad_group` (CL0 under leaf) and `validate_cl1_targeting_for_ad_group` (CL1 under CL4). **Date**: 2026-03-19

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
- **Tracking Table**: `pa.url_validation_tracking` (shared across kopteksten and FAQ since 2026-03-20)
  - Previously: SEO used `pa.jvs_seo_werkvoorraad_kopteksten_check`, FAQ used `pa.faq_tracking` separately
  - Now: Both features read/write skipped URLs from the shared table
  - FAQ recheck delegates to kopteksten recheck endpoint
- **API Endpoints**:
  - `POST /api/recheck-skipped-urls` - Recheck skipped URLs (used by both SEO and FAQ)
  - `POST /api/faq/recheck-skipped-urls` - Recheck FAQ skipped URLs (delegates to kopteksten recheck)
  - `DELETE /api/recheck-skipped-urls/reset` - Reset recheck markers to allow rechecking again
  - `DELETE /api/faq/recheck-skipped-urls/reset` - Reset FAQ recheck markers
- **Parameters**: `parallel_workers` (1-20), `batch_size` (configurable via UI)
- **UI**: "Recheck Skipped" button next to "Validate All" on both SEO and FAQ pages
- **Date**: 2026-02-01, updated 2026-03-20 (shared tracking table)

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

## Dutch Singular/Plural Form Rules (category_forms.json)
- **Dutch words never end in 'z'**: Always wrong — should be s, es, uis, ars, oos, aus, etc.
  - Examples: hoez→hoes, doz→doos, kluiz→kluis, laarz→laars, sauz→saus, muiz→muis
- **Dutch words never end in 'v'**: Always wrong — should be f
  - Examples: schroev→schroef, schijv→schijf, bruv→bruf
- **-el endings need case-by-case analysis**: Many -el words are correct (artikel, stoel, meubel, wiel, doel)
  - Only fix genuine errors: onderdel→onderdeel, panel→paneel (doubled vowel before -el)
- **File**: `backend/category_forms.json` — 148 fixes applied (94 z-endings, 14 v-endings, 40 -el forms)
- **Date**: 2026-02-17

## AI Title Generation: doelgroep_drogisterij as Voor-Facet
- **Problem**: Target group facet values (mannen, vrouwen, kinderen) were placed directly before product name
- **Fix**: Added `voor_values`/`voor_originals` lists in `ai_titles_service.py`. When `fname == 'doelgroep_drogisterij'`, values are wrapped as "voor {value}" and appended after the title
- **Flow**: Detected in facet classification → stripped from H1 → appended as "voor mannen" etc. after title
- **Reset**: 765 URLs containing this facet reset to pending
- **File**: `backend/ai_titles_service.py`
- **Date**: 2026-02-17

## AI Title Generation: aantal_puzzelstukjes as Spec Value
- **Problem**: Puzzle piece count facet (e.g., "500 Stukjes") was placed before product name instead of after
- **Fix**: Added `fname == 'aantal_puzzelstukjes'` check to `is_spec_value()` function
- **Reset**: 432 URLs reset to pending
- **File**: `backend/ai_titles_service.py`
- **Date**: 2026-02-17

## Kopteksten Generator Uses jvs_seo_werkvoorraad Table
- **Problem**: New URLs appeared in unique titles generator but not in kopteksten generator
- **Root Cause**: Kopteksten generator pulls from `pa.jvs_seo_werkvoorraad`, NOT `pa.content_urls_joep`
- **Lesson**: `content_urls_joep` is for unique titles tracking only; `jvs_seo_werkvoorraad` is for kopteksten
- **Date**: 2026-02-17

## Facet Volume Batch Processing (New Excel Input)
- **Script**: `backend/run_facet_volumes_new.py` — processes facet/category search volumes from Excel input
- **Input**: Excel with 'facets' sheet (maincat IDs in col B, facet values in col G) and 'cats' sheet (maincat IDs in col B, deepest cats in col C)
- **SIC/SOD handling**: HTML comment format `<!--SIC-->value<!--/SIC-->` for after-category form, `<!--SOD-->value<!--/SOD-->` for before-category form
- **Resume-capable**: Uses progress file to track completed maincats, saves Excel after each maincat
- **Output**: Search volume written to column K of the facets sheet
- **Scale**: 236,232 facets across 31 maincats, 2.1B total search volume
- **Date**: 2026-02-17

## UTF-8 Mojibake Fix for Excel Files
- **Problem**: "KÃ¤rcher" displayed instead of "Kärcher" — UTF-8 bytes read as latin-1
- **Fix**: `val.encode('latin-1').decode('utf-8')` — re-encode as latin-1 then decode as UTF-8
- **Scale**: Fixed 2,928 values in faet_values_new.xlsx
- **Date**: 2026-02-17

## Visits/Revenue Aggregation per Facet from Redshift
- **Script**: `backend/add_visits_revenue.py` — aggregates URL visits/revenue per facet bucket
- **Data source**: Redshift query extracting URLs with `/c/` path, visits count, and CPC+WW revenue since 2024
- **Matching logic**: URLs split on `/c/` then on `~~` to extract individual facet buckets
- **Scale**: 1.56M URLs → 74,145 facets matched, 33.6M total visits, 3.15M total revenue
- **Output**: Visits in column I, revenue in column J of output Excel
- **Date**: 2026-02-17

## Docker-Owned File Permission Workaround
- **Problem**: Output Excel file created by Docker (root) couldn't be overwritten by user process
- **Workaround**: Save to a different filename (e.g., `_final.xlsx`) owned by the user, then copy
- **Date**: 2026-02-17

## Syncing dm-dashboard → dm-tools
- **dm-dashboard repo**: https://github.com/joep-1993/dm-dashboard — Docker-free version of the dashboard
- **Dual commit policy**: Dashboard changes must be committed to both dm-tools and dm-dashboard repos
- **Sync process**: `git pull` in dm-dashboard, compare diffs, apply to dm-tools manually
- **Date**: 2026-04-03

## PostgreSQL Stale Connection Recovery
- **Problem**: Long-lived PG pool connections go stale, causing queries to fail silently
- **Fix**: Added TCP keepalives (`keepalives=1, keepalives_idle=30`) to PG pool, plus `SELECT 1` health check in `get_db_connection()` before returning connections — matches the existing Redshift pool pattern
- **Files**: `backend/database.py`
- **Date**: 2026-04-03

## Last Push Timestamp Feature
- **Purpose**: Shows when content was last published to production, next to the Publish button
- **Components**: `pa.publish_log` table, `GET /api/content-publish/last-push` endpoint, `fetchLastPushTimestamp()` JS function (in both app.js and faq.js)
- **Format**: DDMMYYYY HH:MM, right-aligned next to Publish button
- **Auto-refresh**: Fetched on page load and after each successful publish
- **Date**: 2026-04-03

## Running dm-tools Without Docker
- **Script**: `./run_local.sh setup` (first time) then `./run_local.sh` to start
- **Requirements**: `python3.12-venv` apt package, Python venv at `dm-tools/venv/`
- **Key difference**: `load_dotenv()` added to top of `main.py` — loads `.env` file for env vars (no-ops in Docker since vars already set)
- **thema_ads_optimized**: Docker mounts from `../theme_ads/thema_ads_optimized`; locally, `run_local.sh` creates a symlink automatically
- **DATABASE_URL**: `.env` must point to remote DB (`10.1.32.9`) instead of Docker's `db:5432`
- **Auto-start**: Windows Task Scheduler task "DM Tools Dashboard" runs `wsl.exe -d Ubuntu -e bash -c "cd ... && source venv/bin/activate && uvicorn ..."` at logon
- **Date**: 2026-04-03
