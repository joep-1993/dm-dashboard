#!/usr/bin/env python3
"""Blueprints for a NEW facet crossed with a category's other top facets.

Traffic-driven gap-finding (seo_titles_gap_from_query.py) cannot see a brand-new facet:
no URL uses it yet, so it scores zero visits and never appears in a gap list. This
builds the combos up front instead — the new facet plus every subset of the category's
other top facets, up to --max-depth deep.

Every combo CONTAINS the new facet. Subsets of the partner facets alone are not built:
those are the existing top-N power set (pagetitles_top5_allchannel_combos.py) and
mixing them in here would silently widen the deliverable.

Combos are ranked by taxonomy, not traffic. A category's facet list from the Taxonomy
API is dominated by the ~280 globally-attached product-line facets, which are all
merk-dependent and are never the "top" facets of anything, so they are filtered out by
facet NAME (see _is_auto_facet -- a slug-prefix rule drops real facets); --partners
overrides the pick entirely.

Guards, all live rather than trusted from a file: combos already held locally
(pa.seo_titles_blueprints) or by the LIVE /page-titles store are skipped, and
impossible_reason() blocks a dependent facet whose parent is absent.

Rows land as status='built'. Nothing is pushed — Publish stays a deliberate click.

Usage:
    venv/bin/python scripts/analysis/seo_titles_build_new_facet_combos.py \
        --cat-id 9003066 --new-facet t_tegelacc            # dry run
    ... --apply
    ... --partners merk,materiaal,kleur,kleurtint --max-depth 3 --apply
"""
import argparse
import itertools
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from backend.database import get_db_connection, return_db_connection  # noqa: E402
from backend.seo_titles_service import (  # noqa: E402
    build_blueprint, canon_key, impossible_reason, load_facet_deps,
    load_local_combos, load_rules, store_has_combos, _upsert_blueprint,
)

# The product-line family: attached to nearly every category, never a category's own
# top facet, and merk-dependent anyway. Identified by the facet NAME, not the slug --
# the slug prefixes that look like they mark this family do not: `p_pennenbakken` is
# "Plaatsing" and `pl_klussen` is "Serie", both real facets that a `p_`/`pl_` prefix
# rule silently drops. Measured on a 285-facet category: name rule 0 false positives,
# slug rule 2.
AUTO_FACET_NAMES = ('Productlijn',)
AUTO_FACET_NAME_PREFIX = 'Productlijnen:'


def _is_auto_facet(name):
    name = (name or '').strip()
    return name in AUTO_FACET_NAMES or name.startswith(AUTO_FACET_NAME_PREFIX)


def pick_partners(cat_id, new_facet, top_n, deps=None):
    """The category's own top facets, minus the new one, capped at top_n. Taxonomy
    order is the category's facet order.

    Two filters, in order of how much they can be trusted:

    1. **A facet with a dependency parent is skipped.** pa.facet_dependencies is real
       data, not a name guess: a facet that is only selectable once a specific parent
       value is chosen cannot be a standalone top facet, and every combo naming it
       without its parent would be blocked downstream anyway. This is what removes
       the ~280 product-line facets on a category like Tegelaccessoires -- all of
       them need `merk` -- along with `pl_klussen` ("Serie"), which the earlier
       slug-prefix rule caught by accident and `_is_auto_facet` does not catch at all.
    2. **The product-line family by name**, as a backstop for a member that has no
       dependency row yet.

    Both beat the slug-prefix rule this used to apply: `p_pennenbakken` is
    "Plaatsing" and has no parent, so a `p_` prefix rule threw away a real facet.
    """
    from backend.url_validator_service import _cache as taxonomy_cache
    if deps is None:
        deps = load_facet_deps()
    out, seen = [], set()
    for f in taxonomy_cache.get_category_facets(cat_id) or []:
        slug = f.get('slug')
        if not slug or slug in seen:
            continue
        seen.add(slug)
        if slug == new_facet:
            continue
        if deps.get(slug):                      # dependent -> not a standalone facet
            continue
        if _is_auto_facet(f.get('name')):
            continue
        if not f.get('enabled') or f.get('noindex'):
            continue
        out.append(slug)
    return out[:top_n]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cat-id', type=int, required=True)
    ap.add_argument('--new-facet', required=True)
    ap.add_argument('--partners', default=None,
                    help='comma-separated; default = the category\'s own top facets')
    ap.add_argument('--top-n', type=int, default=5,
                    help='how many partner facets to take when --partners is absent')
    ap.add_argument('--max-depth', type=int, default=3,
                    help='max facets per combo, the new facet included')
    ap.add_argument('--skip-store', action='store_true')
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()

    from backend.url_validator_service import _cache as taxonomy_cache
    cat = taxonomy_cache.get_category_detail(args.cat_id) or {}
    cat_name = next((l.get('name') for l in cat.get('labels') or []
                     if l.get('locale') == 'nl-NL'), '') or ''

    deps = load_facet_deps()
    partners = ([p.strip() for p in args.partners.split(',') if p.strip()]
                if args.partners
                else pick_partners(args.cat_id, args.new_facet, args.top_n, deps))
    print(f"[1/4] cat {args.cat_id} {cat_name!r} · new facet {args.new_facet!r}")
    print(f"      partners ({len(partners)}): {', '.join(partners) or '(none)'}")
    print(f"      max depth {args.max_depth}")

    rules = load_rules()
    if rules.get(args.new_facet) is None:
        print(f"\n  !! {args.new_facet} has NO row in pa.facet_position_rules. It will fall")
        print(f"     back to UNKNOWN_ORDER and render BEHIND the category noun. Add a rule")
        print(f"     first if that is not what you want.\n")

    combos = []
    for k in range(0, min(args.max_depth, len(partners) + 1)):
        for c in itertools.combinations(partners, k):
            combos.append(canon_key('~'.join((args.new_facet,) + c)))
    combos = sorted(set(combos), key=lambda s: (s.count('~'), s))
    print(f"[2/4] {len(combos)} combos containing {args.new_facet}")

    local = load_local_combos(force=True)
    covered = {c for c in combos if (args.cat_id, c) in local}
    if not args.skip_store:
        todo = [(args.cat_id, c) for c in combos if c not in covered]
        in_store = store_has_combos(todo)
        covered |= {c for (_cid, c) in in_store}
        print(f"      held locally or by the live store: {len(covered)}")
    else:
        print(f"      held locally: {len(covered)} (--skip-store: store NOT asked)")

    todo, blocked = [], []
    for c in combos:
        if c in covered:
            continue
        types = [t for t in c.split('~') if t]
        bad = impossible_reason(types, deps)
        (blocked if bad else todo).append((c, types, bad))

    print(f"[3/4] to build: {len(todo)} · blocked: {len(blocked)} · "
          f"already covered: {len(covered)}")
    for c, types, bad in blocked:
        print(f"      BLOCKED {c:52} {bad}")
    print()
    for c, types, _ in todo:
        bp = build_blueprint(args.cat_id, cat_name, types, rules)
        print(f"      d{len(types)}  {c:46} h1: {bp['h1_title']}")

    if not args.apply:
        print(f"\n[4/4] dry run — pass --apply to create them as status='built'")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    made = 0
    try:
        for c, types, _ in todo:
            bp = build_blueprint(args.cat_id, cat_name, types, rules)
            # No source_url: the facet is new, so no real URL for the combo exists yet.
            # visits/revenue stay NULL rather than 0 — 0 would read as "measured, empty".
            _upsert_blueprint(cur, bp, None, None, None)
            made += 1
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)
    print(f"\n[4/4] created/refreshed {made} blueprints as status='built'")
    keys = ', '.join(repr(c) for c, _, _ in todo)
    print("\n      undo:")
    print(f"        DELETE FROM pa.seo_titles_blueprints")
    print(f"        WHERE status='built' AND cat_id={args.cat_id} AND key IN ({keys});")


if __name__ == '__main__':
    main()
