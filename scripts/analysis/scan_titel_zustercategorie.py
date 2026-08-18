#!/usr/bin/env python3
"""Scan unique_titles op titels die een ZUSTERCATEGORIE noemen in plaats van hun eigen.

Aanleiding (2026-08-18): Joep zag 'Speedo Gezondheidsslippers' als h1_title op
/products/schoenen/schoenen_430879_430974/c/merk~1435006 -- een Teenslippers-URL.
Teenslippers en Gezondheidsslippers zijn allebei children van Slippers (430879) en
horen elkaars pagina's niet te benoemen.

OORZAAK (niet de kopteksten): `fetch_products_api` leidt de categorienaam af uit
cat_urls.csv, en valt bij een misser terug op de categorie van het EERSTE product.
Op een pagina waarvan de producten over meerdere zustercategorieen verdeeld zijn
bepaalt dus het bovenste product de naam. Voor 2026-07-21 was cat_urls.csv stuk
(mojibake, commit bc68056), dus die terugval vuurde vaak. De code is inmiddels
correct -- getoetst: Teenslippers/Gezondheidsslippers/Badslippers leveren nu alle
drie de juiste naam. Wat overblijft is VEROUDERDE DATA, op te lossen met een
hergeneratie via ai_titles_service.process_single_url(url, True).

DETECTOR: eigen categorienaam (of stam) komt NIET in de titel voor, maar de naam
van een zuster -- een andere child van dezelfde parent -- wel. De stamtest houdt
"Koekenpannenset" op een Pannensets-pagina buiten de lijst: dat is een terechte
samenstelling, geen verwisseling.

Draaien: ./venv/bin/python scripts/analysis/scan_titel_zustercategorie.py [--csv pad]
"""
import argparse
import collections
import csv as _csv
import io
import re
import sys
import unicodedata

sys.path.insert(0, ".")
from backend.category_lookup import CAT_URLS_CSV  # noqa: E402
from backend.database import get_db_connection  # noqa: E402


def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[-\s']+", "", s.lower())


def stems(name: str):
    """De eigen naam plus enkelvoudsvormen, zodat 'Pannensets' ook matcht op
    'koekenpannenset'."""
    n = norm(name)
    out = {n}
    for suf in ("en", "s"):
        if n.endswith(suf) and len(n) - len(suf) >= 5:
            out.add(n[: -len(suf)])
    return out


def load_categories():
    rows = list(_csv.DictReader(io.open(CAT_URLS_CSV, encoding="utf-8-sig"), delimiter=";"))
    slug2name = {r["url_name"].strip().strip("/"): r["deepest_cat"].strip() for r in rows}
    kids = collections.defaultdict(set)
    for slug, name in slug2name.items():
        if slug.count("_") >= 2:                       # child: maincat_parentid_childid
            kids["_".join(slug.split("_")[:-1])].add(name)
    return slug2name, kids


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="", help="schrijf de treffers hierheen")
    args = ap.parse_args()

    slug2name, kids = load_categories()
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""SELECT u.url, t.h1_title
                   FROM pa.urls u JOIN pa.unique_titles_content t ON t.url_id = u.url_id
                   WHERE coalesce(t.h1_title, '') <> '' AND u.url LIKE '/products/%%'""")
    data = [(dict(r)["url"], dict(r)["h1_title"]) for r in cur.fetchall()]
    cur.close()
    conn.close()

    hits, paar = [], collections.Counter()
    for url, h1 in data:
        seg = url.split("/")
        if len(seg) < 4 or seg[1] != "products":
            continue
        cat = seg[3]
        if cat not in slug2name:
            continue
        eigen = slug2name[cat]
        h = norm(h1)
        if any(st in h for st in stems(eigen)):
            continue                                    # eigen naam staat er gewoon in
        parent = "_".join(cat.split("_")[:-1]) if cat.count("_") >= 2 else None
        zussen = [n for n in kids.get(parent, ()) if norm(n) != norm(eigen)] if parent else []
        genoemd = [n for n in zussen if norm(n) in h]
        if genoemd:
            hits.append((url, h1, eigen, genoemd[0]))
            paar[(eigen, genoemd[0])] += 1

    print(f"getoetst : {len(data):,} titels")
    print(f"treffers : {len(hits):,}  ({100*len(hits)/max(len(data),1):.2f}%)")
    print("\ntop-15 verwisselingen (eigen -> genoemd):")
    for (e, g), n in paar.most_common(15):
        print(f"   {n:>5}x  {e:<28} -> {g}")

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as fh:
            w = _csv.writer(fh)
            w.writerow(["url", "h1_title", "eigen_categorie", "genoemde_zuster"])
            w.writerows(hits)
        print(f"\nweggeschreven: {args.csv} ({len(hits):,} rijen)")


if __name__ == "__main__":
    main()
