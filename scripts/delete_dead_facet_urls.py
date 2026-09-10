#!/usr/bin/env python3
"""Delete URLs whose facet value no longer exists from pa.urls (cascade).

WHY (2026-09-10)
A URL like /products/meubilair/meubilair_389369/c/merk~7199316 carries a facet value that
is gone from the taxonomy. Three consequences, measured that day:
  * url_validator_service reports VALUE_NOT_FOUND (410 of 412 candidates; the rest are a
    dead category and a structurally broken path).
  * The Search API refuses the call with errorCode 300 "The given facet value is not
    valid", so neither the koptekst nor the FAQ generator can ever produce content — every
    cycle re-queues them and every cycle fails, ~46 new ones a day.
  * The website already 301s these URLs (dead facet dropped, valid facets kept, else the
    bare category). So there is no SEO gap to repair: this delete is about stopping the
    pointless regeneration, not about fixing search results.
Their live content was unpublished first (the FAQs and, on Joep's request, the 57
kopteksten that still described the dropped filter), so this delete removes rows whose
live counterpart is already gone.

METHOD — the scope is MEASURED, never read off the stored error
Candidates are pa.urls rows that either pipeline flagged for an invalid facet
(faq_jobs.skip_reason='facet_not_available', or kopteksten_jobs.last_error containing
'invalid facet'). Each is re-probed live via faq_service.fetch_products_api and only URLs
that STILL answer facet_not_available are deleted. Facet values come back: this probe
spared 89 of 501 candidates on 2026-09-10, and 213 of 7,367 on 2026-08-31. Anything that
fails the probe for another reason (api_failed, timeout, exception) is spared too —
absence of proof is not proof.

SAFETY (recipe: cc1/LEARNINGS.md "contentrijen verwijderen uit de shared Postgres")
  * scope pinned in a real table, WITH the url — pa.del_targets_invalidfacet_20260831 kept
    only url_id, and since pa.urls went with it that set was nearly unaddressable
  * one backup table per affected table, discovered from the FK graph instead of a
    hardcoded list, plus the two push_state tables that have no FK
  * one transaction, asserts before the DELETE and verification after; any mismatch rolls
    back
  * afterwards the unpublish queue is drained: the BEFORE DELETE trigger on pa.urls
    tombstones anything that still had content, with the url text (a trigger on the
    content table cannot resolve it at that point, the parent row is already gone)

NOT COVERED
Unique titles. Those live in the /page-titles CSV store, which is re-uploaded in full from
the DB by the Unique Titles tool, and pa.content_unpublish_queue does not know that store.
Whether a deleted row disappears on the next Publish All depends on whether that endpoint
replaces or upserts — unverified. The count is printed so it stays visible.

Usage:
    delete_dead_facet_urls.py            # dry run: scope, probe, what would go
    delete_dead_facet_urls.py --commit   # do it
"""
import argparse
import os
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date

REPO = "/home/joepvanschagen/projects/dm-dashboard"
sys.path.insert(0, REPO)
os.chdir(REPO)

from dotenv import load_dotenv  # noqa: E402

load_dotenv()

import psycopg2.extras  # noqa: E402

from backend.database import get_db_connection, return_db_connection  # noqa: E402
from backend.faq_service import fetch_products_api  # noqa: E402

CANDIDATES_SQL = """
    SELECT u.url_id, u.url
      FROM pa.urls u
     WHERE u.url_id IN (
             SELECT url_id FROM pa.faq_jobs
              WHERE skip_reason = 'facet_not_available'
             UNION
             SELECT url_id FROM pa.kopteksten_jobs
              WHERE last_error ILIKE '%invalid facet%')
     ORDER BY u.url_id
"""

# These have no FK to pa.urls, so the FK graph does not list them.
NO_FK_TABLES = ["pa.kopteksten_push_state", "pa.faq_v2_push_state"]

PROBE_WORKERS = 10


def fk_children(cur):
    """Tables with a FK on pa.urls(url_id) — the rows the cascade takes."""
    cur.execute("""
        SELECT DISTINCT n.nspname || '.' || c.relname AS tbl
          FROM pg_constraint con
          JOIN pg_class c ON c.oid = con.conrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE con.contype = 'f'
           AND con.confrelid = 'pa.urls'::regclass
         ORDER BY 1
    """)
    return [r['tbl'] for r in cur.fetchall()]


def probe(rows):
    """Re-check every candidate. Returns (still_invalid, spared, reasons)."""
    def one(r):
        try:
            d = fetch_products_api(r['url'], include_related=False) or {}
        except Exception as e:
            return r, f"exception:{type(e).__name__}"
        return r, (d.get("error") or "ok")

    still, spared, reasons = [], [], Counter()
    with ThreadPoolExecutor(PROBE_WORKERS) as ex:
        for r, outcome in ex.map(one, rows):
            reasons[outcome] += 1
            (still if outcome == "facet_not_available" else spared).append(dict(r))
    return still, spared, reasons


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--commit", action="store_true", help="without this it is a dry run")
    ap.add_argument("--tag", default=f"deadfacet_{date.today():%Y%m%d}",
                    help="suffix for the scope + backup tables")
    args = ap.parse_args()

    conn = get_db_connection()      # DATABASE_URL from .env, same pool as the app
    cur = conn.cursor()
    cur.execute(CANDIDATES_SQL)
    cands = cur.fetchall()
    print(f"kandidaten: {len(cands)}")
    if not cands:
        return_db_connection(conn)
        return

    still, spared, reasons = probe(cands)
    print("re-probe:", dict(reasons))
    print(f"  nog ongeldig (verwijderen): {len(still)}")
    print(f"  gespaard (weer geldig of onbewijsbaar): {len(spared)}")

    ids = [r['url_id'] for r in still]
    children = fk_children(cur) + NO_FK_TABLES
    print("\ngeraakte tabellen (rijen binnen scope):")
    counts = {}
    for t in children:
        cur.execute(f"SELECT count(*) c FROM {t} WHERE url_id = ANY(%s)", (ids,))
        counts[t] = cur.fetchone()['c']
        if counts[t]:
            print(f"   {counts[t]:7d}  {t}")

    cur.execute("SELECT count(*) c FROM pa.unique_titles_content WHERE url_id = ANY(%s)",
                (ids,))
    ut = cur.fetchone()['c']
    if ut:
        print(f"\nLET OP: {ut} van deze URL's hebben een unique title. Die store wordt als"
              "\n        volledige CSV opnieuw geupload uit de DB, dus of ze bij de volgende"
              "\n        Publish All verdwijnen hangt af van replace-vs-upsert daar. Ongetoetst.")

    if not args.commit:
        print("\ndry run — niets geschreven. Voeg --commit toe.")
        return_db_connection(conn)
        return

    scope_tbl = f"pa.del_targets_{args.tag}"
    try:
        cur.execute(f"CREATE TABLE {scope_tbl} (url_id bigint PRIMARY KEY, url text)")
        psycopg2.extras.execute_values(
            cur, f"INSERT INTO {scope_tbl} (url_id, url) VALUES %s",
            [(r['url_id'], r['url']) for r in still])
        cur.execute(f"SELECT count(*) c FROM {scope_tbl}")
        assert cur.fetchone()['c'] == len(still), "scope table size mismatch"

        for t in children:
            bak = f"{t}_bak_{args.tag}"
            cur.execute(f"""CREATE TABLE {bak} AS
                            SELECT * FROM {t}
                             WHERE url_id IN (SELECT url_id FROM {scope_tbl})""")
            cur.execute(f"SELECT count(*) c FROM {bak}")
            got = cur.fetchone()['c']
            assert got == counts[t], f"{bak}: backed up {got}, expected {counts[t]}"

        # The push_state tables have no FK, so the cascade cannot reach them — they have
        # to go explicitly, or the delete leaves rows claiming a vanished URL is live
        # with content. (Noted in TASKS for 2026-08-31 and asserted below.)
        for t in NO_FK_TABLES:
            cur.execute(f"""DELETE FROM {t}
                             WHERE url_id IN (SELECT url_id FROM {scope_tbl})""")
            print(f"   {cur.rowcount} rijen uit {t} (geen FK, dus handmatig)")

        cur.execute(f"""DELETE FROM pa.urls
                         WHERE url_id IN (SELECT url_id FROM {scope_tbl})""")
        deleted = cur.rowcount
        assert deleted == len(still), f"deleted {deleted}, expected {len(still)}"

        for t in children:
            cur.execute(f"""SELECT count(*) c FROM {t}
                             WHERE url_id IN (SELECT url_id FROM {scope_tbl})""")
            left = cur.fetchone()['c']
            assert left == 0, f"{t}: {left} rijen over na de delete"

        conn.commit()
        print(f"\n{deleted} URL's verwijderd, backups op *_bak_{args.tag}, "
              f"scope in {scope_tbl}")
    except Exception as e:
        conn.rollback()
        return_db_connection(conn)
        sys.exit(f"\nTERUGGEDRAAID — {type(e).__name__}: {e}")

    return_db_connection(conn)

    from backend.content_unpublish_queue import drain, stats
    print("queue na de delete:", stats())
    for kind in ("koptekst", "faq"):
        print(f"drain {kind}:", drain(kind))


if __name__ == "__main__":
    main()
