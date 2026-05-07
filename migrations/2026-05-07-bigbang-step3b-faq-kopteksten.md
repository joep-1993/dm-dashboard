# Big Bang refactor — Step 3b/3c: FAQ + Kopteksten cutover

Code refactor. Switches the FAQ + Kopteksten tools (and the content
publisher that joins their data) from the old per-tool URL-keyed tables
to the new `pa.urls` + per-tool `*_jobs` / `*_content` schema.

## Why this is bundled (3b + 3c together)

The content publisher (`backend/content_publisher.py`) does a `FULL OUTER
JOIN` of the two tools' content tables to publish them together. If FAQ
were migrated alone, the publisher's join would mix new + old schemas
and read stale data on whichever side wasn't migrated yet. Migrating
both together avoids that — it's a single coherent cutover for the
publisher path.

## Files changed

| File | Change |
|---|---|
| `backend/main.py` | All 98 FAQ + Kopteksten endpoint references migrated. Used `bulk_upsert_urls()` for batch writes (FAQ + recheck loops); `get_url_id()` for single-URL look-ups. Two `pa.content_history` INSERTs deferred (audit-only, append-only). |
| `backend/content_publisher.py` | The 4 `FULL OUTER JOIN content_urls_joep + faq_content` queries replaced with a `LEFT JOIN`-from-`pa.urls` pattern over `kopteksten_content` + `faq_content_v2`. |
| `backend/link_validator.py` | `update_content_in_redshift`, `add_urls_to_werkvoorraad`, `reset_faq_to_pending` rewritten. `add_urls_to_werkvoorraad` now writes to `pa.kopteksten_jobs(status='pending')` only (was the implicit eligibility marker for both tools — see eligibility backfill below). |
| `backend/batch_api_service.py` | FAQ + Kopteksten batch worker DB writes (skip / failed tracking + content upserts) moved to new tables; `bulk_upsert_urls()` keeps per-batch DB round-trip count down. |
| `backend/url_catalog.py` | (already in place from step 3a) — used throughout. |

## Schema additions (in this step, late-discovered)

Two link-validation tables that pre-step1 hadn't included:

```sql
CREATE TABLE pa.kopteksten_link_validation (
    url_id BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    total_links INTEGER, valid_links INTEGER, broken_links INTEGER,
    broken_link_details JSONB,
    validated_at TIMESTAMP NOT NULL DEFAULT now()
);
CREATE TABLE pa.faq_link_validation (
    url_id BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    total_links INTEGER, valid_links INTEGER, gone_links INTEGER,
    validated_at TIMESTAMP NOT NULL DEFAULT now()
);
```

Backfilled from `pa.link_validation_results` (240,376 rows) and
`pa.faq_validation_results` (238,879 rows). Step 1 SQL file amended
to include these so a fresh re-run produces identical schema.

## Eligibility backfill (data — not code)

The old `pa.jvs_seo_werkvoorraad` (390,596 rows) was the universe of
URLs eligible for both Kopteksten AND FAQ. Per-tool tracking tables
were sparser — only URLs that had been processed (or attempted) had a
row. After collapsing `werkvoorraad` into `pa.urls` (which is now the
universal catalog with 980k rows shared across all tools), we needed an
explicit eligibility marker per tool — otherwise the FAQ batch worker
would either run on every URL in the catalog (wrong, includes
unique-titles-only URLs) or only on URLs already in `pa.faq_jobs`
(wrong, drops 92k werkvoorraad URLs that had never been queued).

Solution: pre-populate `pa.kopteksten_jobs(status='pending')` and
`pa.faq_jobs(status='pending')` for every werkvoorraad URL not already
present. Backfill rows: 92,740 faq_jobs + 133,940 kopteksten_jobs.
After this, both job tables contain exactly 390,022 rows (the
canonicalized werkvoorraad universe).

Going forward, "URL is eligible for FAQ generation" means "URL has a
row in `pa.faq_jobs`" — explicit. Same for kopteksten.

## NOT migrated in this step

Deferred to step 4 (final-cleanup):

- `pa.content_history` — append-only audit trail, still keyed on URL
  string. Two INSERT references in main.py still write to this. Safe
  to migrate later; no read paths look at it cross-tool.
- `pa.publish_log` — environment / payload tracking. No URL column.
- `backend/database.py` `init_db()` — still has `CREATE TABLE IF NOT
  EXISTS` for the old tables. Harmless (tables exist; calls are
  no-ops); cleanup later when we drop the old tables.
- 12+ admin / utility scripts (`fix_faq_*.py`, `import_*.py`,
  `sync_*.py`, `deduplicate_content.py`, `compare_prompts.py`,
  `migrate_shared_validation.py`). They read/write the OLD tables
  which still exist with frozen-at-cutover-time data. Acceptable for
  rarely-run admin tools.

## Smoke-test results (live DB, post-cutover)

```
Kopteksten /api/status:  total=390,022  processed=242,629
                         skipped=152,805  failed=14,248  pending=638
FAQ        /api/faq/status:
                         total=390,022  processed=250,248
                         skipped=152,805  failed=22,311  pending=0
Combined publish stats:  kopteksten_count=242,629
                         faq_count=250,248  total_unique=267,577
```

The "FAQ pending=0" was investigated — it's correct. All 117,761
status='pending' faq_jobs rows have an `is_valid=FALSE` row in
`pa.url_validation` from past `no_products_found` outcomes. The OLD
query had identical semantics (excluded URLs in
`url_validation_tracking`).

## Deployment

```
pkill -f 'uvicorn backend.main:app'
cd ~/projects/dm-tools && uvicorn backend.main:app --reload --port 8003
```

Then click through:
- `/static/index.html` (Kopteksten dashboard) — Stats tile, Recent
  Results, Failure Reasons, Export buttons.
- `/static/faq.html` — same panels.
- `/static/content-publish.html` — Stats + Preview should show the
  united counts.

## Rollback

If anything's wrong: revert the four Python files via git, restart
uvicorn. The new tables remain populated independently of the old
tables (which kept getting old writes from any code we missed). Both
schemas are current up to the cutover, so a partial rollback is safe.
