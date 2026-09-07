#!/usr/bin/env python3
"""Backfill pa.facet_watch_events.value_name from the event payload.

WHY: `value_name` was only ever filled from `vmap`, the live value-to-facet cache, so it
stayed NULL for every value the Taxonomy API does not (or no longer) know. Measured
2026-09-07: 4.099 `Facet Value Label` events had `value_name IS NULL` while the name sat
in `changes.NameInColumn` — a column promising something it does not deliver, which is why
the Facet Watch values module reads from `changes` instead.

The ingest is fixed separately (`_name_from_changes`, same commit); this repairs the rows
that were already written. Both call THAT ONE function — the shape rule must not exist
twice, because it is subtle:

    INSERT  "NameInColumn": "Ford Bronco"                 -> a string   (4.019 rows)
    UPDATE  "NameInColumn": {"New": "Yoni", "Old": "YONI"} -> an object  (   80 rows)

A plain `changes->>'NameInColumn'` returns the JSON TEXT for that second shape and would
write `{"New": ...}` into the column. That is the trap this script exists to not fall into.

WHAT IT DOES NOT DO: rows whose payload carries no name are left NULL — `Facet Value`
INSERT/DELETE events carry only FacetId, CreatedAt and SeoPriority, and for a DELETE the
name lives in the garbage bin (handled at read time in facet_watch_service._bin_names).

Dry run by default; `--commit` writes.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from psycopg2.extras import execute_values

from backend.database import get_db_connection, return_db_connection
from backend.facet_watch_service import _name_from_changes

BATCH = 2000


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true", help="write (default: dry run)")
    ap.add_argument("--limit", type=int, default=0, help="cap rows, for a smoke test")
    ap.add_argument("--snapshot", default="", help="where to write the pre-state CSV")
    args = ap.parse_args()

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT audit_id, entity_name, action, changes
            FROM pa.facet_watch_events
            WHERE value_name IS NULL
              AND changes ? 'NameInColumn'
            ORDER BY audit_id
            """
            + (" LIMIT %s" if args.limit else ""),
            (args.limit,) if args.limit else None,
        )
        rows = [dict(r) for r in cur.fetchall()]

        updates, skipped = [], Counter()
        by_kind = Counter()
        for r in rows:
            name = _name_from_changes(r["changes"])
            kind = f'{r["entity_name"]} / {r["action"]}'
            if name:
                updates.append((r["audit_id"], name))
                by_kind[kind] += 1
            else:
                skipped[kind] += 1

        print(f"candidate rows (value_name IS NULL, payload has NameInColumn): {len(rows)}")
        print(f"resolvable: {len(updates)}   left NULL: {sum(skipped.values())}\n")
        for kind, n in by_kind.most_common():
            print(f"  fill   {kind:<28}{n:>6}")
        for kind, n in skipped.most_common():
            print(f"  skip   {kind:<28}{n:>6}")
        if updates:
            print("\n  samples:")
            for aid, nm in updates[:5]:
                print(f"    {aid} -> {nm!r}")

        if not args.commit:
            print("\nDRY RUN — nothing written. Re-run with --commit.")
            return 0
        if not updates:
            print("\nNothing to write.")
            return 0

        # Pre-state snapshot before touching a shared production table, same practice as
        # dedup_mc_ids_efficy.py. Every row below is NULL right now, so the file IS the
        # undo: `UPDATE ... SET value_name = NULL WHERE audit_id IN (<column 1>)`.
        # In logs/ and not data/: `.gitignore` ignores logs/ but explicitly UN-ignores
        # data/*.csv (`!data/*.csv`), and a one-time undo artefact should not land in the
        # folder meant for CSVs that belong in the repo.
        snap = Path(args.snapshot) if args.snapshot else \
            Path(__file__).resolve().parents[1] / "logs" / "backfill_value_names_prestate.csv"
        snap.parent.mkdir(parents=True, exist_ok=True)
        with snap.open("w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["audit_id", "value_name_before", "value_name_after"])
            for aid, nm in updates:
                w.writerow([aid, "", nm])
        print(f"\npre-state snapshot: {snap} ({len(updates)} rows, all NULL before)")

        written = 0
        for i in range(0, len(updates), BATCH):
            chunk = updates[i:i + BATCH]
            execute_values(
                cur,
                """
                UPDATE pa.facet_watch_events AS e
                SET value_name = v.name
                FROM (VALUES %s) AS v(audit_id, name)
                WHERE e.audit_id = v.audit_id
                  AND e.value_name IS NULL
                """,
                chunk,
                # WITHOUT this, execute_values splits the chunk into pages of 100 and
                # `cur.rowcount` reports only the LAST page — the first run of this script
                # printed "written: 299" for 4.099 rows actually written. One statement per
                # chunk keeps the number honest.
                page_size=len(chunk),
            )
            conn.commit()
            written += cur.rowcount
        print(f"\nwritten: {written} rows (expected {len(updates)})")

        # Authoritative check on the rows this run touched, not on the counter above.
        cur.execute(
            """
            SELECT count(*) AS n, count(value_name) AS filled
            FROM pa.facet_watch_events WHERE audit_id = ANY(%s)
            """,
            ([aid for aid, _ in updates],),
        )
        chk = dict(cur.fetchone())
        print(f"of the {chk['n']} touched rows, {chk['filled']} now carry a name")

        # Verify against the same predicate the script selects on: nothing that this
        # rule can resolve may be left behind.
        cur.execute(
            """
            SELECT count(*) AS n FROM pa.facet_watch_events
            WHERE value_name IS NULL AND changes ? 'NameInColumn'
            """
        )
        left = dict(cur.fetchone())["n"]
        cur.execute(
            """
            SELECT count(*) AS n FROM pa.facet_watch_events
            WHERE value_name = '' OR value_name LIKE '{%'
            """
        )
        junk = dict(cur.fetchone())["n"]
        print(f"still NULL with a NameInColumn payload: {left}  "
              f"(expected: only the unresolvable shapes, {sum(skipped.values())})")
        print(f"rows holding JSON text or an empty name: {junk}  (expected 0)")
        return 0 if junk == 0 else 1
    finally:
        cur.close()
        return_db_connection(conn)


if __name__ == "__main__":
    sys.exit(main())
