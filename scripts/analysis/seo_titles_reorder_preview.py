#!/usr/bin/env python3
"""Side-by-side of stored vs rebuilt SEO-title blueprints, for judging a reorder.

Rebuilds each blueprint from (cat_id, cat_name, key) with the CURRENT
pa.facet_position_rules and diffs it against what is stored. Only rows whose phrase
actually changes are reported, so the output is the real blast radius of a facet-order
edit rather than a dump of everything.

Placeholders are also rendered with a real example value per facet (pulled from the
Taxonomy API, one value per facet, cached) so the two versions can be read as Dutch
instead of as !!placeholder!! soup. The rendering is illustrative only — the live page
substitutes the visitor's actual facet selection.

Read-only. Pass --apply to write the rebuilt title/h1_title/description back for the
changed status='built' rows (backup CSV first, never touches 'pushed').

Usage:
    venv/bin/python scripts/analysis/seo_titles_reorder_preview.py
    venv/bin/python scripts/analysis/seo_titles_reorder_preview.py --apply
"""
import argparse
import csv
import os
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backend.database import get_db_connection, return_db_connection  # noqa: E402
from backend.seo_titles_service import build_blueprint, load_rules  # noqa: E402

API = "http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl"
HEADERS = {"X-User-Name": "SEO_JOEP"}
OUT = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
       "seo_titles_reorder_sidebyside.csv")
BACKUP = ("/mnt/c/Users/JoepvanSchagen/Downloads/claude/"
          "seo_titles_reorder_backup.csv")


def facet_index():
    r = requests.get(f"{API}/api/Facets", headers=HEADERS, timeout=120)
    r.raise_for_status()
    by_slug = {}
    for f in r.json():
        for lab in (f.get("labels") or []):
            s = (lab.get("urlSlug") or "").strip()
            if s and s not in by_slug:
                by_slug[s] = {"id": f["id"], "name": lab.get("name") or s}
    return by_slug


def example_value(fid, fallback):
    """First facet value label, for a readable render. Falls back to the facet name."""
    try:
        r = requests.get(f"{API}/api/Facets/{fid}/values", headers=HEADERS, timeout=60)
        if not r.ok:
            return f"<{fallback}>"
        body = r.json()
        # /values returns {"total": N, "items": [...]} — NOT a bare array, unlike
        # /value-dependencies. Handle both so a shape change cannot silently blank
        # every rendered example (which is exactly what happened first time round).
        items = body.get("items") if isinstance(body, dict) else body
        for v in (items or [])[:1]:
            for lab in (v.get("labels") or []):
                n = (lab.get("nameInColumn") or lab.get("nameOnDetail") or "").strip()
                if n:
                    return n
    except (requests.RequestException, ValueError, AttributeError):
        pass
    return f"<{fallback}>"


def render(phrase, cat_name, examples):
    out = []
    for tok in (phrase or "").split(" "):
        if not tok:
            continue
        if tok == "!!sub_category!!":
            out.append(cat_name or "<categorie>")
        elif tok.startswith("!!") and tok.endswith("!!"):
            slug = tok.strip("!")
            out.append(examples.get(slug, f"<{slug}>"))
        else:
            out.append(tok)
    return " ".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="write the rebuilt values back for changed status='built' rows")
    args = ap.parse_args()

    rules = load_rules()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT cat_id, cat_name, key, status, title, h1_title, description
                       FROM pa.seo_titles_blueprints""")
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)
    print(f"[1/4] blueprints: {len(rows):,}")

    changed = []
    for r in rows:
        types = [t for t in (r["key"] or "").split("~") if t]
        if not types:
            continue
        bp = build_blueprint(r["cat_id"], r["cat_name"] or "", types, rules)
        if bp["h1_title"] != r["h1_title"] or bp["title"] != r["title"]:
            changed.append((r, bp))
    print(f"[2/4] rows whose blueprint changes under the current rules: {len(changed):,}")
    if not changed:
        print("      nothing to preview")
        return

    slugs = sorted({t for r, _ in changed for t in r["key"].split("~") if t})
    idx = facet_index()
    print(f"[3/4] fetching one example value for {len(slugs)} facets")
    examples = {}
    for s in slugs:
        meta = idx.get(s)
        examples[s] = example_value(meta["id"], s) if meta else f"<{s}>"

    print("[4/4] side by side (h1, rendered with example values)\n")
    for r, bp in changed[:40]:
        print(f"  {r['status']:6} {(r['cat_name'] or '?')[:26]:26} {r['key'][:52]}")
        print(f"     OLD  {render(r['h1_title'], r['cat_name'], examples)}")
        print(f"     NEW  {render(bp['h1_title'], r['cat_name'], examples)}")
    if len(changed) > 40:
        print(f"  … and {len(changed) - 40:,} more (see the CSV)")

    with open(OUT, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["cat_id", "cat_name", "status", "key",
                    "old_h1", "new_h1", "old_h1_rendered", "new_h1_rendered",
                    "old_title", "new_title"])
        for r, bp in changed:
            w.writerow([r["cat_id"], r["cat_name"], r["status"], r["key"],
                        r["h1_title"], bp["h1_title"],
                        render(r["h1_title"], r["cat_name"], examples),
                        render(bp["h1_title"], r["cat_name"], examples),
                        r["title"], bp["title"]])
    print(f"\nwrote {OUT}")

    if not args.apply:
        print("dry run — pass --apply to write the changed built rows back")
        return

    todo = [(r, bp) for r, bp in changed if r["status"] == "built"]
    skipped = len(changed) - len(todo)
    print(f"\napplying to {len(todo):,} built rows ({skipped:,} pushed left alone)")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        with open(BACKUP, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["cat_id", "key", "title", "h1_title", "description"])
            for r, _ in todo:
                w.writerow([r["cat_id"], r["key"], r["title"], r["h1_title"], r["description"]])
        print(f"    backup -> {BACKUP}")
        n = 0
        for r, bp in todo:
            # status='built' re-asserted so a row pushed since the read is not rewritten.
            cur.execute("""UPDATE pa.seo_titles_blueprints
                           SET title=%s, h1_title=%s, description=%s
                           WHERE cat_id=%s AND key=%s AND status='built'""",
                        (bp["title"], bp["h1_title"], bp["description"], r["cat_id"], r["key"]))
            n += cur.rowcount
        conn.commit()
        print(f"    updated: {n:,} rows")
    finally:
        cur.close()
        return_db_connection(conn)


if __name__ == "__main__":
    main()
