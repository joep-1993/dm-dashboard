#!/usr/bin/env python3
"""One-off: dedupe pa.mc_ids_efficy to one row per (shop_id, domain).

RAN 2026-08-05: 583 -> 520 rows, 63 surplus removed, 0 keys lost, 0 rows invented.
Pre-state backed up to pa.mc_ids_efficy_bak_20260805 (583 rows) and to
Downloads/claude/mc_ids_efficy_snapshot_20260805.csv. Kept for the record and because the
same command re-verifies the invariant; a second --commit run is a no-op beyond the checks
(the backup is not overwritten).

The source of the duplicates is fixed separately in gsd_campaigns_service.push_mc_ids_to_redshift
(eb17d60) — without that, this table refills.

Joep's rule (2026-08-05): the table is STATE — one row per shop+country holding that shop's
current Merchant Center id. It currently carries 63 surplus rows because the old write path
was a bare INSERT.

WHICH ROW SURVIVES: the EARLIEST date. Measured, not assumed — against
pa.jvs_gsd_campaign_created (the authoritative creation date) the earliest logged date matches
in 39 of 49 duplicated groups and the latest in ZERO. That is also exactly what the new
"insert once, then leave alone" rule produces, so the deduped table equals the table the fixed
code would have built.

Redshift cannot delete "all but one" of byte-identical rows (no row id), so the table is
rebuilt inside one transaction from a temp table.

Usage:  dedup_mc_ids.py            # dry run, writes nothing
        dedup_mc_ids.py --commit   # backup, then rebuild
"""
import os
import sys

REPO = "/home/joepvanschagen/projects/dm-dashboard"
sys.path.insert(0, REPO)
os.chdir(REPO)

from dotenv import load_dotenv  # noqa: E402

load_dotenv()
from backend.gsd_campaigns_service import _get_redshift_connection  # noqa: E402

COMMIT = "--commit" in sys.argv
BACKUP = "pa.mc_ids_efficy_bak_20260805"

# Earliest date wins; mc_created as a deterministic tie-break for the one group that has two
# distinct MC ids on the SAME date (Kamera-express.nl 182 NL, reported separately).
KEEP_SQL = """
    SELECT shop_name, shop_id, mc_created, domain, date FROM (
        SELECT shop_name, shop_id, mc_created, domain, date,
               ROW_NUMBER() OVER (PARTITION BY shop_id, UPPER(domain)
                                  ORDER BY date ASC, mc_created ASC) rn
        FROM pa.mc_ids_efficy
    ) WHERE rn = 1
"""

conn = _get_redshift_connection()
conn.autocommit = False
cur = conn.cursor()

# NOTE on reading back after a write: keep verification on THIS connection's transaction, or
# open a fresh one per read. Redshift is serializable, so a long-lived transaction stays
# pinned to the snapshot of its first statement — a committed write from another connection
# reads back as unchanged, and the next write in the stale transaction dies with
# "Serializable isolation violation". That cost a confusing false failure once already.

cur.execute("SELECT COUNT(*) FROM pa.mc_ids_efficy")
before = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM (SELECT shop_id, UPPER(domain) FROM pa.mc_ids_efficy GROUP BY 1,2)")
keys = cur.fetchone()[0]
print(f"before: {before} rows over {keys} (shop_id, domain) keys  -> {before - keys} surplus")

cur.execute(f"SELECT COUNT(*) FROM ({KEEP_SQL})")
keep_n = cur.fetchone()[0]
print(f"keep  : {keep_n} rows")
assert keep_n == keys, f"keeper count {keep_n} != key count {keys}"

print("\nrows that would be DROPPED (grouped; earliest kept):")
cur.execute("""
    SELECT shop_name, shop_id, domain, mc_created, MIN(date) kept, COUNT(*) n
    FROM pa.mc_ids_efficy
    WHERE (shop_id || '|' || UPPER(domain)) IN (
        SELECT shop_id || '|' || UPPER(domain) FROM pa.mc_ids_efficy
        GROUP BY 1 HAVING COUNT(*) > 1)
    GROUP BY shop_name, shop_id, domain, mc_created
    ORDER BY shop_id, domain
""")
for r in cur.fetchall():
    print(f"    {(r[0] or '')[:28]:28} {r[1]:>8} {r[2]:3} mc={r[3]:<12} keep={r[4]} rows={r[5]}")

if not COMMIT:
    print("\nDRY RUN — nothing written. Re-run with --commit.")
    conn.rollback()
    conn.close()
    raise SystemExit

# ---- write path -------------------------------------------------------------------
cur.execute(f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema='pa' AND table_name='{BACKUP.split('.')[1]}'")
if cur.fetchone()[0]:
    print(f"\nbackup {BACKUP} already exists — leaving it alone")
else:
    cur.execute(f"CREATE TABLE {BACKUP} AS SELECT * FROM pa.mc_ids_efficy")
    conn.commit()
    cur.execute(f"SELECT COUNT(*) FROM {BACKUP}")
    n = cur.fetchone()[0]
    print(f"\nbackup {BACKUP}: {n} rows")
    assert n == before, f"backup has {n} rows, expected {before}"

cur.execute(f"CREATE TEMP TABLE mc_keep AS {KEEP_SQL}")
cur.execute("SELECT COUNT(*) FROM mc_keep")
staged = cur.fetchone()[0]
assert staged == keep_n, f"staged {staged} != planned {keep_n}"
cur.execute("DELETE FROM pa.mc_ids_efficy")
cur.execute("""INSERT INTO pa.mc_ids_efficy (shop_name, shop_id, mc_created, domain, date)
               SELECT shop_name, shop_id, mc_created, domain, date FROM mc_keep""")
cur.execute("SELECT COUNT(*) FROM pa.mc_ids_efficy")
after = cur.fetchone()[0]
assert after == keep_n, f"after {after} != planned {keep_n}"
conn.commit()
print(f"committed: {before} -> {after} rows")

# ---- verify against the backup ---------------------------------------------------
print("\nverification")
cur.execute("SELECT COUNT(*) FROM (SELECT shop_id, UPPER(domain) FROM pa.mc_ids_efficy "
            "GROUP BY 1,2 HAVING COUNT(*) > 1)")
print("  keys still holding >1 row      :", cur.fetchone()[0], "(must be 0)")
cur.execute(f"""SELECT COUNT(*) FROM pa.mc_ids_efficy a
                LEFT JOIN {BACKUP} b
                  ON a.shop_id = b.shop_id AND a.domain = b.domain
                 AND a.mc_created = b.mc_created AND a.date = b.date
                WHERE b.shop_id IS NULL""")
print("  surviving rows NOT in the backup:", cur.fetchone()[0], "(must be 0 — nothing invented)")
cur.execute(f"""SELECT COUNT(*) FROM (
                  SELECT shop_id AS sid, UPPER(domain) AS dom
                  FROM {BACKUP} GROUP BY 1,2) b
                WHERE (b.sid || '|' || b.dom) NOT IN (
                  SELECT shop_id || '|' || UPPER(domain) FROM pa.mc_ids_efficy)""")
print("  keys lost entirely              :", cur.fetchone()[0], "(must be 0 — no shop dropped)")
conn.close()
print("\nDONE")
