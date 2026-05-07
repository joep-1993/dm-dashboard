"""
OBSOLETE — one-shot migration superseded by the Big Bang refactor (2026-05-07).

Original purpose: created pa.url_validation_tracking and merged 'no_products_found'
skip rows from pa.jvs_seo_werkvoorraad_kopteksten_check + pa.faq_tracking into
that one shared table.

After Big Bang:
- The shared validation table is now pa.url_validation (keyed on url_id).
- The old source tables don't exist.

If you somehow ended up here looking to consolidate validation, look at
migrations/2026-05-07-bigbang-step2-backfill.sql instead. This stub is
kept so a stale `python -m backend.migrate_shared_validation` invocation
fails loudly with an explanation rather than a SQL error.
"""

def main():
    print("This migration is obsolete — see migrations/2026-05-07-bigbang-step2-backfill.sql.")
    print("pa.url_validation_tracking has been replaced by pa.url_validation (url_id-keyed).")

if __name__ == "__main__":
    main()
