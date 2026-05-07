# Big Bang refactor — Step 3a: Unique Titles cutover

Code refactor. Switches the Unique Titles tool from `pa.unique_titles` to the
new `pa.urls` + `pa.unique_titles_jobs` + `pa.unique_titles_content` schema.

## Files changed

| File | Change |
|---|---|
| `backend/url_catalog.py` | NEW — `canonicalize_url()` + `get_url_id()` + `bulk_upsert_urls()` |
| `backend/unique_titles.py` | Rewritten to use new tables |
| `backend/ai_titles_service.py` | DB-touching functions rewritten (`init_ai_titles_columns` is now a no-op; `get_unprocessed_urls`, `get_unprocessed_count`, `update_title_record`, `get_ai_titles_stats`, `get_recent_results`, `analyze_and_flag_failures` all switched to new tables) |
| `pa.canonicalize_url()` (SQL) | Updated to reject non-Beslist hosts (matches Python helper) |

## Files NOT yet changed (deferred to step 4)

These admin scripts still read/write the old `pa.unique_titles`. They'll
operate on the frozen pre-cutover snapshot; rerun after step 4 to point
them at the new tables.

- `backend/find_bad_urls.py`
- `backend/check_unique_titles_urls.py`
- `scripts/score_titles.py`
- `scripts/export_scored_titles.py`

## How to deploy

1. Restart uvicorn so the new `backend/unique_titles.py` and
   `backend/ai_titles_service.py` modules load:
   ```
   pkill -f 'uvicorn backend.main:app'
   uvicorn backend.main:app --reload --port 8003
   ```
2. Open `http://localhost:8003/static/unique-titles.html`. Sanity-check:
   - "Stats" tile populates (total / processed / pending / errors).
   - "Recent Results" lists titles.
   - "Add URL for Title Generation" queues a test URL → expect `added: 1`.
   - "Search" returns rows.
3. Optional: kick off a small AI batch (e.g. 5 URLs) to verify the
   write path through `update_title_record()`.

## Rollback

If something is wrong, revert the three Python files via git and restart
uvicorn. Data in the new tables is independent of the old table; both
remain populated until step 4.
