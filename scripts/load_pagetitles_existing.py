#!/usr/bin/env python3
"""
Load the current tblPageTitles blueprint export into Postgres so the "SEO titles"
tool can dedup new (cat_id, key) combos WITHOUT touching read-only MySQL at request
time.

Source : Downloads/claude/tblPageTitles.xlsx
         columns: cat_id, key, title, h1_title, description, country_code
Target : pa.page_titles_existing  (adds a normalized `canon_key` column)

Idempotent: TRUNCATE + bulk reload. Re-run whenever a newer export is produced.

Run under the venv that has openpyxl + psycopg2:
    ~/.mysql-venv/bin/python scripts/load_pagetitles_existing.py [path/to/tblPageTitles.xlsx]
"""
import sys
import psycopg2
from psycopg2.extras import execute_values

# Same Postgres DSN the sibling blueprint scripts use (n8n-vector-db, schema pa).
PG_DSN = ("postgresql://dbadmin:Q9fGRKtUdvdtxsiCM12HeFe0Nki0PvmjZRFLZ9ArmlWdMnDQXX8SdxKnPniqGmq6"
          "@10.1.32.9:5432/n8n-vector-db")

DEFAULT_XLSX = '/mnt/c/Users/JoepvanSchagen/Downloads/claude/tblPageTitles.xlsx'

DDL = """
CREATE SCHEMA IF NOT EXISTS pa;

CREATE TABLE IF NOT EXISTS pa.page_titles_existing (
    cat_id       INTEGER NOT NULL,
    key          TEXT    NOT NULL,        -- raw key as stored in the export
    canon_key    TEXT    NOT NULL,        -- '~'.join(sorted(lower(split('~'))))
    title        TEXT,
    h1_title     TEXT,
    description  TEXT,
    country_code TEXT DEFAULT 'NL',
    cat_name     TEXT                     -- deepest category name, backfilled by
                                          -- scripts/backfill_page_titles_existing_catname.py
);
ALTER TABLE pa.page_titles_existing ADD COLUMN IF NOT EXISTS cat_name TEXT;
CREATE INDEX IF NOT EXISTS ix_pte_combo ON pa.page_titles_existing (cat_id, canon_key);

-- Blueprints this tool builds/pushes. Doubles as preview source, push queue,
-- dedup set and push-log. Created here too so a fresh DB is ready before the
-- backend starts.
CREATE TABLE IF NOT EXISTS pa.seo_titles_blueprints (
    cat_id       INTEGER NOT NULL,
    key          TEXT    NOT NULL,        -- canon_key
    cat_name     TEXT,
    title        TEXT,
    h1_title     TEXT,
    description  TEXT,
    country_code TEXT DEFAULT 'NL',
    source_url   TEXT,
    visits       INTEGER,
    revenue      NUMERIC,
    status       TEXT DEFAULT 'built',    -- built / pushed / failed
    last_error   TEXT,
    created_at   TIMESTAMP DEFAULT now(),
    pushed_at    TIMESTAMP,
    PRIMARY KEY (cat_id, key)
);
"""


def canon_key(s):
    """MUST match backend/seo_titles_service.py::canon_key exactly."""
    return '~'.join(sorted(t for t in (s or '').lower().split('~') if t))


def main():
    xlsx = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_XLSX
    import openpyxl

    print(f"[load] opening {xlsx}", flush=True)
    wb = openpyxl.load_workbook(xlsx, read_only=True)
    ws = wb.worksheets[0]
    it = ws.iter_rows(values_only=True)
    hdr = next(it)
    ci = {h: i for i, h in enumerate(hdr) if h is not None}
    for req in ('cat_id', 'key', 'title', 'h1_title', 'description', 'country_code'):
        if req not in ci:
            raise SystemExit(f"missing expected column {req!r} in {hdr}")

    rows = []
    skipped = 0
    for r in it:
        cat = r[ci['cat_id']]
        key = r[ci['key']]
        if cat is None or key is None:
            skipped += 1
            continue
        try:
            cat = int(cat)
        except (TypeError, ValueError):
            skipped += 1
            continue
        rows.append((
            cat,
            str(key),
            canon_key(str(key)),
            r[ci['title']],
            r[ci['h1_title']],
            r[ci['description']],
            r[ci['country_code']] or 'NL',
        ))
    wb.close()
    print(f"[load] parsed rows={len(rows)} skipped={skipped}", flush=True)

    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    try:
        cur.execute(DDL)
        cur.execute("TRUNCATE pa.page_titles_existing")
        execute_values(cur, """
            INSERT INTO pa.page_titles_existing
                (cat_id, key, canon_key, title, h1_title, description, country_code)
            VALUES %s
        """, rows, page_size=10000)
        conn.commit()
        cur.execute("SELECT count(*), count(DISTINCT (cat_id, canon_key)) FROM pa.page_titles_existing")
        total, distinct = cur.fetchone()
        print(f"[done] pa.page_titles_existing rows={total} distinct(cat_id,canon_key)={distinct}", flush=True)
        print("[note] TRUNCATE cleared cat_name — re-run "
              "scripts/backfill_page_titles_existing_catname.py to repopulate it.", flush=True)
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
