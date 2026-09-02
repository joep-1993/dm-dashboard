"""
SEO Titles Service
==================

Generates (cat_id, key) page-title BLUEPRINTS for the top SEO-visited faceted
`/c/` URLs that don't have a blueprint yet, and pushes them to the
website-configuration `/page-titles` API (upsert-per-record).

Flow (see /home/joepvanschagen/.claude/plans/proud-singing-lecun.md):
  1. Redshift: top-X SEO-visited faceted /c/ URLs (ordered by visits desc).
  2. parse_url -> (leaf_slug, {facet types}); slug -> cat_id via TaxonomyCache.
  3. canon_key = '~'.join(sorted(lower(types))).
  4. DEDUP, two steps:
       a. drop combos pa.seo_titles_blueprints already holds (built or pushed);
       b. ask the store itself for the rest — GET /page-titles/{cat_id}/record?key=.
     Until 2026-08-24 step (b) was a lookup in pa.page_titles_existing, a July
     snapshot of an Excel export of MySQL beslist.tblPageTitles. That table has
     since been dropped, the snapshot cannot see records added after it was taken,
     and 387.277 of its 539.214 rows are a "shifted" layout whose `key` column
     holds a CATEGORY NAME, not a facet key (their real key sits in `title`), so
     they never matched a combo anyway. The API answer is authoritative; the
     snapshot table stays only as the "existing" tab in the frontend.
  5. For each NEW combo: build a deterministic placeholder blueprint AND (best
     effort) an AI unique title for the source URL (reused ai_titles_service).
  6. Publish: POST blueprints -> /page-titles. Blueprints ONLY — unique titles are
     published from the Unique Titles tool. (Until 2026-07-31 this also called
     upload_titles_to_api(), which re-uploads a CSV of ALL ~1,02M unique titles —
     the same thing Unique Titles' own Publish All does — adding ~20 minutes to a
     10-minute blueprint push. `push_unique_titles=True` still opts in.)

Blueprint templates are ported verbatim from
scripts/pagetitles_blueprint_from_urls.py so generated keys stay byte-identical
to the historical deliverable.
"""
import os
import re
import time
import threading
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor

import requests

from backend.database import (
    get_db_connection, return_db_connection,
    get_redshift_connection, return_redshift_connection,
)

# ---------------------------------------------------------------------------
# API config
# ---------------------------------------------------------------------------
PAGE_TITLES_API = {
    "production": "https://website-configuration.api.beslist.nl/page-titles",
    "staging": "https://website-configuration-staging.api.beslist.nl/page-titles",
}
# Prod authenticates with UNIQUE_TITLES_API_KEY; staging with CONTENT_API_KEY_STAGING
# (verified live: prod key -> 401 on staging and vice-versa).
PAGE_TITLES_KEY = {
    "production": lambda: os.getenv("UNIQUE_TITLES_API_KEY", ""),
    "staging": lambda: os.getenv("CONTENT_API_KEY_STAGING", ""),
}
PUSH_BATCH = 5000

# ---- Reading the store back -----------------------------------------------
# GET /page-titles/{cat_id}/record?key=<key> -> 200 + the record, or 404
# "Record not found". There is no list endpoint, so existence can only be asked
# one combo at a time; that is what STORE_WORKERS and the cache table are for.
# The key is matched LITERALLY and is order-sensitive (merk~type_plantenbakken
# is a different lookup than type_plantenbakken~merk), so always send canon_key().
STORE_WORKERS = 12
# A cached "exists" never expires: /page-titles has no delete verb, so a record
# cannot disappear. A cached "missing" does — anyone can add a record — and is
# re-checked once it is this old.
STORE_MISS_TTL_DAYS = 7

# ---------------------------------------------------------------------------
# Blueprint building (ported from scripts/pagetitles_blueprint_from_urls.py)
# ---------------------------------------------------------------------------
SUBCATEGORY_ORDER = 1700
SUBCATEGORY_PH = '!!sub_category!!'
# Fallback order for a facet with no row in pa.facet_position_rules.
#
# This used to be 1500, i.e. BELOW SUBCATEGORY_ORDER (1700), so any facet nobody had
# written a rule for silently rendered BEFORE the category noun — "Rood <newfacet>
# Sneakers" instead of "Rood Sneakers <newfacet>". The default now sits just above
# the category, so an unruled facet trails the noun, which is the safe reading for a
# facet whose meaning we do not know yet. New facets appear in URLs before anyone
# writes a rule, so this default IS the behaviour for them until someone does.
UNKNOWN_ORDER = 1750
IGNORE_FACETS = {'pricemin', 'pricemax'}

# ---- Position pins (pa.facet_position_rules.position) ---------------------
#
# The SAME column the unique-titles generator honours in
# ai_titles_service._build_v3_h1 — until 2026-07-31 the blueprint builder read only
# order_index and silently ignored it, so a facet pinned "after the noun" for the AI
# H1 could still render in front of the noun in its blueprint. Now both paths obey it.
#
# A pin beats order_index for POSITION only; order_index still breaks ties inside a
# pinned group, so two 'end' facets keep their relative order (thema_speelgoed 1929
# before mobiel_k 2066).
#
#   'pre_noun'        immediately BEFORE the noun — whatever the noun is that combo
#                     (the !!sub_category!! placeholder, or the type-facet that
#                     replaced it). This is the one thing order_index cannot express:
#                     the noun's position is not fixed.
#   'end'             after every other placeholder in the phrase.
#   'end_before_size' degraded to 'end' here. The AI path can separate sizes because
#                     it sees the VALUE (is_spec_value("Maat 42")); a blueprint holds
#                     only !!placeholders!!, so "before the sizes" is not decidable at
#                     build time. Direction (post-noun, near the end) is still right.
#
# Ignored on a type facet: that facet IS the noun, so "before/after the noun" is
# meaningless for it (is_type_facet wins, and the pin is skipped).
POS_PRE_NOUN = 'pre_noun'
POS_END = 'end'
POS_END_BEFORE_SIZE = 'end_before_size'
# Sorts after any real order_index (the table's max is 2400) without being infinite,
# so pinned facets stay comparable among themselves.
END_PIN_ORDER = 9000
# Pinned facets keep their relative order via order_index/PIN_TIE_SCALE, which must be
# large enough that the fraction can never cross into the next integer slot.
PIN_TIE_SCALE = 1e6
COUNTRY_CODE = 'NL'
TAIL_TITLE = 'kopen? ✔️ Tot !!DISCOUNT!! korting! | beslist.nl'
# Scheidingsteken in de description. MOET een echt karakter zijn, geen HTML-entity.
#
# Hier stond de letterlijke tekst '&#10062;'. Live nagemeten op 2026-09-02: de site
# substitueert eerst de !!placeholders!! en HTML-escapet daarna de hele string bij het
# injecteren in de meta-tag — terecht, dat hoort zo. Gevolg: de `&` werd `&amp;` en de
# meta-tag bevatte `&amp;#10062;`, oftewel Google las drie keer per description de
# letterlijke tekens `&#10062;`. In de nog-onvervangen JSON-blob op diezelfde pagina
# staat de entity wél rauw; het is dus puur de meta-tag-route.
#
# Keuze van hét teken: &#10062; is U+274E, oftewel ❎. Joep heeft op 2026-09-02 bevestigd
# dat dat het bedoelde teken is — dus geen inhoudelijke wijziging, alleen de codering:
# het echte karakter in plaats van de entity. (TAIL_TITLE hierboven gebruikt ✔️; die
# twee mogen verschillen, titel en description zijn niet hetzelfde blok.)
#
# De 86.123 bestaande rijen zijn op 2026-09-02 omgezet met een gerichte
# REPLACE(description, '&#10062;', '❎') en opnieuw gepusht (18 batches, 0 fouten).
# Backup: pa.seo_titles_blueprints_bak_20260902. NOG NIET omgezet: de legacy-templates
# in pa.page_titles_existing, die dezelfde entity 8.791 keer dragen — dat is een andere
# tabel en een andere beslissing.
DESC_BULLET = '❎'
# /page-titles rejects a title over this many characters (400 "too long").
MAX_TITLE_LEN = 200
# /page-titles enforces this on h1_title as well (learned the hard way: one 205-char
# h1 got a whole 5000-record batch rejected with 400 "Invalid record values").
MAX_H1_LEN = 200


def canon_key(s):
    """Canonical comparable form of a '~'-joined facet key: lowercase each type
    and re-sort. MUST match scripts/load_pagetitles_existing.py::canon_key."""
    return '~'.join(sorted(t for t in (s or '').lower().split('~') if t))


def parse_url(url):
    """url -> (leaf_slug, set_of_facet_types) or None when not a faceted /c/ url.
    Caller must lowercase the url first."""
    if '/c/' not in url:
        return None
    path, fstr = url.split('/c/', 1)
    segs = [s for s in path.split('/') if s]
    leaf = segs[-1] if segs else ''
    types = set()
    for pair in fstr.split('~~'):
        bits = pair.split('~')
        # A facet needs BOTH a name and a VALUE. Requiring only bits[0] let URLs
        # ending in "<name>~" through — real traffic contains junk/bot URLs like
        # .../c/merkm~ , .../c/me~ , .../c/kleur_mode_accessoi~ — and the generator
        # turned each into a (cat_id, key) combo, producing blueprints whose phrase
        # holds a placeholder like !!merkm!! that can never resolve. 321 such rows
        # existed (one already pushed) before this guard; see LEARNINGS 2026-07-30.
        if len(bits) >= 2 and bits[0] and bits[1]:
            t = unquote(bits[0])
            if t not in IGNORE_FACETS:
                types.add(t)
    return leaf, types


def _resolve_cat(taxonomy_cache, leaf):
    """Leaf slug -> {'cat_id', 'cat_name'} or None. Tries the sub-category map
    first, then falls back to the maincat map (bare-maincat faceted pages, whose
    ids also appear in tblPageTitles)."""
    c = taxonomy_cache.get_category(leaf)
    if c:
        return {"cat_id": c['cat_id'], "cat_name": c.get('deepest_cat', '')}
    m = taxonomy_cache.get_maincat(leaf)
    if m:
        return {"cat_id": m['id'], "cat_name": m.get('name', '')}
    return None


# ---------------------------------------------------------------------------
# Facet dependencies: a child facet is only selectable once a specific parent
# facet value is chosen, so a combo naming the child WITHOUT the parent describes
# a URL nobody can reach. type_parfum ("Collectie") needs merk; every
# pl_*/productlijnen-* facet needs merk; the kleurtint_* family needs kleur.
#
# The map is cached in Postgres rather than fetched per run: deriving it costs one
# Taxonomy API call per distinct facet (~2.200), which is fine as an occasional
# refresh and far too slow inside a generation run. Refresh with
#   venv/bin/python scripts/analysis/seo_titles_dependency_audit.py --refresh-cache
# ---------------------------------------------------------------------------
# NOTE THE PRIMARY KEY: (child_slug, parent_slug), not child_slug alone. One slug can
# belong to SEVERAL facet ids — 551 of 7.910 slugs do — and each id can depend on a
# different parent. `kleurtint_bruin` exists as six facets: under `kleur` in most
# categories and under `kleur_mode_accessoires` in the mode tree. A single-parent table
# picked whichever id came first and then called the other categories' valid combos
# impossible; live URL .../c/kleur_mode_accessoires~457466~~kleurtint_bruin~7742283 is
# exactly such a combo.
FACET_DEPS_DDL = """
CREATE TABLE IF NOT EXISTS pa.facet_dependencies (
    child_slug   TEXT NOT NULL,
    parent_slug  TEXT NOT NULL,
    child_id     INTEGER,
    parent_id    INTEGER,
    refreshed_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (child_slug, parent_slug)
);
"""
FACET_DEPS_MIGRATE = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint
                WHERE conrelid = 'pa.facet_dependencies'::regclass
                  AND contype = 'p'
                  AND pg_get_constraintdef(oid) = 'PRIMARY KEY (child_slug)') THEN
        ALTER TABLE pa.facet_dependencies DROP CONSTRAINT facet_dependencies_pkey;
        ALTER TABLE pa.facet_dependencies ADD PRIMARY KEY (child_slug, parent_slug);
    END IF;
END $$;
"""


def load_facet_deps():
    """child_slug -> SET of acceptable parent slugs. Empty dict when the cache table is
    absent/empty, which degrades to the old behaviour (build everything) rather than to
    a run that silently drops every combo."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(FACET_DEPS_DDL)
        cur.execute(FACET_DEPS_MIGRATE)
        conn.commit()
        cur.execute("SELECT child_slug, parent_slug FROM pa.facet_dependencies")
        out = {}
        for r in cur.fetchall():
            out.setdefault(r["child_slug"], set()).add(r["parent_slug"])
        return out
    finally:
        cur.close()
        return_db_connection(conn)


def impossible_reason(types, deps):
    """'<child> needs <parent>' when the combo names a dependent facet and NONE of its
    acceptable parents is present, else None.

    ANY parent satisfies it, because the same child slug exists as several facets with
    different parents depending on the category tree (kleurtint_bruin sits under `kleur`
    in most categories and under `kleur_mode_accessoires` in mode). Requiring one
    specific parent marked live, reachable URLs as impossible.
    """
    have = set(types)
    for t in types:
        parents = deps.get(t)
        if parents and not (parents & have):
            return f"{t} needs " + " or ".join(sorted(parents))
    return None


def load_rules():
    """facet_slug -> (order_index, is_type_facet, position) from pa.facet_position_rules.

    Only the unscoped (scope_category IS NULL) row per slug is loaded, matching
    ai_titles_service._load_facet_position_rules — scoped rules are a future feature and
    a scoped row must not silently override the global one here.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""SELECT facet_slug, order_index, is_type_facet, position
                       FROM pa.facet_position_rules WHERE scope_category IS NULL""")
        rules = {}
        for row in cur.fetchall():
            slug = row['facet_slug']
            order = row['order_index']
            pos = (row['position'] or '').strip().lower() or None
            rules[slug] = (order if order is not None else UNKNOWN_ORDER,
                           bool(row['is_type_facet']), pos)
        return rules
    finally:
        cur.close()
        return_db_connection(conn)


def _rule(rules, slug):
    """(order, is_type, position) for a slug, tolerant of the pre-2026-07-31 2-tuple.

    Callers hand `rules` in from load_rules(), but scripts and long-lived processes can
    still be holding a dict built before `position` existed; unpacking blind would raise
    there instead of just losing the pin.
    """
    r = rules.get(slug)
    if r is None:
        return UNKNOWN_ORDER, False, None
    if len(r) == 2:
        return r[0], r[1], None
    return r[0], r[1], r[2]


# ---- Dependent facets: the child already implies the parent's VALUE -------
#
# kleurtint_blauw IS a kleur, houtsoort_materiaal IS a materiaal,
# doelgroep_kind_baby_mode IS a doelgroep_mode — naming both renders "Blauwe
# Lichtblauwe Sneakers" / "Houten Eikenhouten Kasten", so the parent is dropped
# from the phrase. The KEY keeps every facet type: it identifies the URL combo.
#
# An IDENTITY parent is the exception. `pl_kleding -> merk` / `type_kranen -> merk`
# means the facet only becomes AVAILABLE once a brand is picked; the child value
# does not imply which brand, and "Nike Air Max Sneakers" wants it spelled out.
# The same holds for a series/productline parent (populaire_serie, serie_*, pl_*):
# a name is not a value dimension you can infer from a child.
# (Legacy blueprints agree: of 6.661 omitted parents only 310 were `merk`.)
_deps_cache = {"deps": None, "loaded_at": 0.0}
_DEPS_TTL = 600  # seconds

# Een identity-parent is een NAAM (merk, serie, productlijn, personage, model) en
# geen dimensie die je uit het kind kunt afleiden. Die moet in de titel blijven staan.
#
# Dit was een prefix-test, en die matchte 9 van de 55 parent_slugs in
# pa.facet_dependencies: `'serie'.startswith('serie_')` is False, en
# speelgoed_series / voertuigmerken / nerf_series / personage vielen er ook buiten.
# Gevolg: merk~speelgoed_series~thema_little_people shipte als
# `!!merk!! !!sub_category!! !!thema_little_people!!` — serienaam weg.
#
# Fout in de identity-richting kost hooguit een iets redundante titel; fout in de
# andere richting wist stil een naam. Dus bij twijfel identity.
# Getoetst tegen alle 55 parent_slugs op 2026-09-02: 21 identity, 34 dimensie.
# Nieuwe naamfamilie erbij? Voeg een token toe (of de slug aan _IDENTITY_PARENT_SLUGS)
# en draai de split opnieuw tegen pa.facet_dependencies.
_IDENTITY_PARENT_TOKENS = frozenset({
    'merk', 'merken', 'automerk', 'automerken', 'voertuigmerken',
    'serie', 'series', 'productlijn', 'productlijnen', 'prodl', 'pl',
    'personage', 'spellen', 'model',
})
_IDENTITY_PARENT_SUBSTRINGS = ('merk', 'serie')
# Namen die geen herkenbaar token dragen (de `s_`-familie = serie-<merk>).
_IDENTITY_PARENT_SLUGS = frozenset({'s_bouwstenen', 's_clementoni', 's_voer'})


def _identity_parent(slug):
    s = (slug or '').lower()
    if s in _IDENTITY_PARENT_SLUGS:
        return True
    if set(re.split(r'[_\-]', s)) & _IDENTITY_PARENT_TOKENS:
        return True
    return any(sub in s for sub in _IDENTITY_PARENT_SUBSTRINGS)


def deps_cached(force=False):
    """load_facet_deps() behind a TTL cache — facet_phrase() runs once per combo
    and load_facet_deps() re-runs its DDL on every call."""
    now = time.time()
    if not force and _deps_cache["deps"] is not None \
            and now - _deps_cache["loaded_at"] < _DEPS_TTL:
        return _deps_cache["deps"]
    _deps_cache["deps"] = load_facet_deps()
    _deps_cache["loaded_at"] = now
    return _deps_cache["deps"]


# Generieke koppen: `processor_type_laptop`.endswith('_type_laptop') is waar, maar dat
# maakt type_laptop nog geen parent van processor_type_laptop — ze delen alleen een
# achtervoegsel. Voor deze koppen eisen we een echte rij in pa.facet_dependencies.
_GENERIC_SUFFIX_HEADS = ('type', 'soort', 'model', 'vorm', 'kleur', 'maat')


def covered_parents(types, deps, rules=None):
    """Parent slugs in `types` whose VALUE is already implied by a dependent child
    that is also in `types`. Two dependency sources, because pa.facet_dependencies
    is incomplete (it knows houttype_materiaal -> materiaal but not
    houtsoort_materiaal): the table, plus the slug-suffix convention
    (`<child>_<parent>`), which can only fire when both sit in the same key.

    Twee dingen mogen NOOIT als gedekte parent wegvallen:
      * een type-facet — dat is het zelfstandig naamwoord. Viel dat weg, dan zette
        facet_phrase() er het generieke !!sub_category!! voor in de plaats. Live
        raakten 99 gepushte blueprints zo hun noun kwijt.
      * een identity-parent (naam), zie _identity_parent().
    """
    pool = set(types)
    out = set()
    for child in pool:
        parents = set(deps.get(child, ()))
        for p in pool:
            if p == child or not child.endswith('_' + p):
                continue
            # Blinde suffixmatch alleen toestaan als de kop niet generiek is;
            # anders moet de dependency-tabel het bevestigen.
            head = p.split('_')[0].lower()
            if head in _GENERIC_SUFFIX_HEADS and p not in deps.get(child, ()):
                continue
            parents.add(p)
        for p in parents:
            if p not in pool or _identity_parent(p):
                continue
            if rules is not None and _rule(rules, p)[1]:
                continue                      # type-facet = de noun, nooit schrappen
            out.add(p)
    return out


def facet_phrase(types, rules, deps=None):
    """Ordered placeholder phrase for a set of facet types. Inserts
    !!sub_category!! at SUBCATEGORY_ORDER when the set has no type-facet.

    A parent facet whose dependent child is in the same set is dropped — see
    covered_parents(). Pass `deps` to override the cached dependency map.

    Placement is order_index against SUBCATEGORY_ORDER, with two overrides:
      * a `position` pin from pa.facet_position_rules (see POS_* above) — the only way
        to say "directly in front of / after THE NOUN" when the noun's own position
        moves with the type-facet;
      * geschikte_leeftijd is always rendered AFTER the noun regardless of its
        order_index. An explicit pin on it wins over this hardcoded rule.
    """
    if deps is None:
        deps = deps_cached()
    types = [t for t in types if t not in covered_parents(types, deps, rules)]
    items = []  # (order, slug, placeholder)
    has_type = False
    type_orders = []
    pins = {}   # slug -> position, non-type facets only
    for t in types:
        order, is_type, pos = _rule(rules, t)
        if is_type:
            has_type = True
            type_orders.append(order)
        elif pos:
            pins[t] = pos
        items.append((order, t, f'!!{t}!!'))
    if not has_type:
        items.append((SUBCATEGORY_ORDER, '', SUBCATEGORY_PH))
    # The noun is the last type-facet, or the sub_category placeholder when there is
    # none. Everything pinned relative to "the noun" hangs off this number.
    noun_order = max(type_orders) if type_orders else SUBCATEGORY_ORDER
    placed = []
    for order, slug, ph in items:
        pos = pins.get(slug)
        if pos == POS_PRE_NOUN:
            # Just under the noun; the fraction keeps two pre_noun facets in
            # order_index order and can never reach the noun itself.
            order = noun_order - 1 + order / PIN_TIE_SCALE
        elif pos in (POS_END, POS_END_BEFORE_SIZE):
            order = END_PIN_ORDER + order / PIN_TIE_SCALE
        elif slug == 'geschikte_leeftijd':
            order = noun_order + 0.5
        placed.append((order, slug, ph))
    placed.sort(key=lambda x: (x[0], x[1]))
    return ' '.join(ph for _, _, ph in placed)


def _compose_title(phrase):
    """Assemble the page title from the (possibly trimmed) facet phrase.
    Skips an empty phrase so no double space slips in."""
    parts = ['!!current_query!!']
    if phrase:
        parts.append(phrase)
    parts.append(TAIL_TITLE)
    return ' '.join(parts)


def build_blueprint(cat_id, cat_name, types, rules):
    """Return a blueprint dict for a (cat_id, {types}) combo."""
    key = '~'.join(sorted(types))
    phrase = facet_phrase(types, rules)
    title = _compose_title(phrase)
    # /page-titles caps the title at MAX_TITLE_LEN chars. When a deep facet
    # combo overflows, drop trailing (lowest-priority) facet placeholders until
    # it fits — never split a !!placeholder!! and always keep !!current_query!!
    # and the branding tail. h1/description keep the full phrase (no such cap).
    if len(title) > MAX_TITLE_LEN:
        tokens = phrase.split(' ')
        while tokens and len(_compose_title(' '.join(tokens))) > MAX_TITLE_LEN:
            tokens.pop()
        title = _compose_title(' '.join(tokens))
    # /page-titles caps h1_title at MAX_H1_LEN too -- and it validates the whole
    # POST atomically, so ONE overlong h1 gets a 5000-record batch rejected with
    # 400 "Invalid record values" and flips all 5000 to 'failed'. Trim the same way
    # as the title (drop trailing placeholders, never split one). `description` is
    # deliberately NOT capped: the API accepts long ones (37k+ pushed rows exceed
    # 200 chars), so leave that alone.
    h1 = phrase
    if len(h1) > MAX_H1_LEN:
        tokens = phrase.split(' ')
        while tokens and len(' '.join(tokens)) > MAX_H1_LEN:
            tokens.pop()
        h1 = ' '.join(tokens)
    desc = (f'Zoek je {phrase}? {DESC_BULLET} Vergelijk !!NR!! aanbiedingen en bespaar op je '
            f'aankoop {DESC_BULLET} Shop {phrase} met !!DISCOUNT!! korting online! '
            f'{DESC_BULLET} beslist.nl')
    return {
        'cat_id': cat_id, 'key': key, 'cat_name': cat_name,
        'title': title, 'h1_title': h1, 'description': desc,
        'country_code': COUNTRY_CODE,
    }


# ---------------------------------------------------------------------------
# Redshift: top-visited faceted /c/ URLs
# ---------------------------------------------------------------------------
def _yyyymmdd(date_str, default):
    """'YYYY-MM-DD' -> int YYYYMMDD, tolerant of already-int or empty input."""
    if not date_str:
        return default
    s = str(date_str).replace('-', '').strip()
    try:
        return int(s)
    except ValueError:
        return default


def fetch_top_urls(top_n, date_from=None, date_to=None):
    """Top-N SEO-visited faceted /c/ URLs, ordered by visits desc.

    Returns list of dicts: {url, visits, revenue}. (Ordering differs from the
    archival notes/query.txt, which sorted by subcat name — we want top visited.)
    """
    dfrom = _yyyymmdd(date_from, 20250101)
    dto = _yyyymmdd(date_to, 20260608)
    sql = """
        SELECT SPLIT_PART(dv.url, '?', 1) AS url,
               count(*) AS visits,
               sum(fcv.cpc_revenue) + sum(fcv.ww_revenue) AS revenue
        FROM datamart.fct_visits fcv
        JOIN datamart.dim_visit dv ON fcv.dim_visit_key = dv.dim_visit_key
        JOIN chan_deriv.ref_channel_derivation_stats chan
             ON dv.aff_id = chan.aff_id AND dv.channel_id = chan.channel_id
        WHERE dv.is_real_visit = 1
          AND chan.marketing_channel = 'SEO'
          AND fcv.dim_date_key BETWEEN %s AND %s
          AND dv.url LIKE '%%beslist.nl%%'
          AND dv.url LIKE '%%/c/%%'
          AND dv.url NOT LIKE '%%/r/%%'
          AND dv.url NOT LIKE '%%+%%'
          AND dv.url NOT LIKE '%%/l/%%'
          AND dv.url NOT LIKE '%%/page_%%'
          AND dv.url NOT LIKE '%%#%%'
        GROUP BY 1
        HAVING count(*) > 0
        ORDER BY visits DESC
        LIMIT %s
    """
    conn = get_redshift_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, (dfrom, dto, int(top_n)))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_redshift_connection(conn)


# ---------------------------------------------------------------------------
# Dedup, step (a): combos this tool already holds locally
# ---------------------------------------------------------------------------
_existing_cache = {"combos": None, "loaded_at": 0.0}
_EXISTING_TTL = 600  # seconds


def load_local_combos(force=False):
    """Set of (cat_id, canon_key) pa.seo_titles_blueprints already holds, in any
    status. Cheap in-memory guard that keeps a re-run from rebuilding its own
    output; it says nothing about what the store holds — that is
    store_has_combos()."""
    now = time.time()
    if not force and _existing_cache["combos"] is not None \
            and now - _existing_cache["loaded_at"] < _EXISTING_TTL:
        return _existing_cache["combos"]
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        combos = set()
        cur.execute("SELECT cat_id, key FROM pa.seo_titles_blueprints")
        for row in cur.fetchall():
            combos.add((row['cat_id'], canon_key(row['key'])))
        _existing_cache["combos"] = combos
        _existing_cache["loaded_at"] = now
        return combos
    finally:
        cur.close()
        return_db_connection(conn)


# ---------------------------------------------------------------------------
# Dedup, step (b): ask the /page-titles store itself
# ---------------------------------------------------------------------------
def _record_exists(session, env, cat_id, key):
    """True / False / None(=could not tell) for one (cat_id, key) in the store."""
    base = PAGE_TITLES_API[env]
    for attempt in range(1, 4):
        try:
            resp = session.get(f"{base}/{cat_id}/record",
                               params={"key": key}, timeout=30)
            if resp.status_code == 200:
                return True
            if resp.status_code == 404:
                return False
            # 401/400/5xx: not an answer about this record.
            if resp.status_code < 500:
                return None
        except requests.RequestException:
            pass
        time.sleep(1.5 * attempt)
    return None


def store_has_combos(combos, env="production", workers=STORE_WORKERS,
                     force_recheck=False, progress=None):
    """Which of `combos` (an iterable of (cat_id, canon_key)) the /page-titles
    store already holds. Authoritative — this is the live store, not a snapshot.

    Answers are memoised in pa.page_titles_api_cache so a re-run only pays for
    combos it has not seen: a hit is kept forever (no delete verb exists, so a
    record cannot vanish), a miss is re-checked after STORE_MISS_TTL_DAYS.

    A combo whose GET cannot be answered (network, 401, 5xx) is reported as
    EXISTING. That is the conservative direction: treating a live record as new
    would rebuild it and overwrite it on the next push, while treating a missing
    record as existing only costs us one blueprint we could have added.
    """
    combos = list(dict.fromkeys(combos))
    if not combos:
        return set()
    if not PAGE_TITLES_KEY[env]():
        raise RuntimeError(f"missing API key for env={env}; cannot verify the store")

    exists, todo = set(), []
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT cat_id, key, found FROM pa.page_titles_api_cache
            WHERE found OR checked_at > now() - (%s || ' days')::interval
        """, (STORE_MISS_TTL_DAYS,))
        cached = {(r['cat_id'], r['key']): r['found'] for r in cur.fetchall()}
    finally:
        cur.close()
        return_db_connection(conn)

    for c in combos:
        hit = None if force_recheck else cached.get(c)
        if hit is True:
            exists.add(c)
        elif hit is False:
            pass
        else:
            todo.append(c)

    if not todo:
        return exists

    session = requests.Session()
    session.headers["X-Api-Key"] = PAGE_TITLES_KEY[env]()
    answers = {}

    def _one(c):
        return c, _record_exists(session, env, c[0], c[1])

    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for c, res in ex.map(_one, todo):
            done += 1
            if res is None:
                exists.add(c)          # conservative: never rebuild on doubt
                _inc("store_errors")
            else:
                answers[c] = res
                if res:
                    exists.add(c)
            if progress and done % 200 == 0:
                progress(done, len(todo))

    if answers:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.executemany("""
                INSERT INTO pa.page_titles_api_cache (cat_id, key, found, checked_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (cat_id, key) DO UPDATE
                    SET found = EXCLUDED.found, checked_at = EXCLUDED.checked_at
            """, [(c[0], c[1], v) for c, v in answers.items()])
            conn.commit()
        finally:
            cur.close()
            return_db_connection(conn)
    return exists


def get_store_record(cat_id, key, env="production"):
    """The record the store holds for (cat_id, key), or None when it holds none.
    The read path the tool lacked until 2026-08-24 — use it to verify a push."""
    if not PAGE_TITLES_KEY[env]():
        raise RuntimeError(f"missing API key for env={env}")
    session = requests.Session()
    session.headers["X-Api-Key"] = PAGE_TITLES_KEY[env]()
    resp = session.get(f"{PAGE_TITLES_API[env]}/{int(cat_id)}/record",
                       params={"key": canon_key(key)}, timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def init_seo_titles_table():
    """Create the tool's tables if missing (idempotent)."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pa.seo_titles_blueprints (
                cat_id       INTEGER NOT NULL,
                key          TEXT    NOT NULL,
                cat_name     TEXT,
                title        TEXT,
                h1_title     TEXT,
                description  TEXT,
                country_code TEXT DEFAULT 'NL',
                source_url   TEXT,
                visits       INTEGER,
                revenue      NUMERIC,
                status       TEXT DEFAULT 'built',
                last_error   TEXT,
                created_at   TIMESTAMP DEFAULT now(),
                pushed_at    TIMESTAMP,
                PRIMARY KEY (cat_id, key)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pa.page_titles_existing (
                cat_id       INTEGER NOT NULL,
                key          TEXT    NOT NULL,
                canon_key    TEXT    NOT NULL,
                title        TEXT,
                h1_title     TEXT,
                description  TEXT,
                country_code TEXT DEFAULT 'NL'
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS ix_pte_combo ON pa.page_titles_existing (cat_id, canon_key)")
        # Memoised answers from GET /page-titles/{cat_id}/record (see
        # store_has_combos). `key` is the canonical key, matching what we send.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pa.page_titles_api_cache (
                cat_id     INTEGER   NOT NULL,
                key        TEXT      NOT NULL,
                found      BOOLEAN   NOT NULL,
                checked_at TIMESTAMP NOT NULL DEFAULT now(),
                PRIMARY KEY (cat_id, key)
            )
        """)
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)


def _upsert_blueprint(cur, bp, source_url, visits, revenue):
    """Insert/refresh a built blueprint. Never downgrades a 'pushed' row."""
    cur.execute("""
        INSERT INTO pa.seo_titles_blueprints
            (cat_id, key, cat_name, title, h1_title, description, country_code,
             source_url, visits, revenue, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'built', now())
        ON CONFLICT (cat_id, key) DO UPDATE SET
            cat_name    = EXCLUDED.cat_name,
            title       = EXCLUDED.title,
            h1_title    = EXCLUDED.h1_title,
            description = EXCLUDED.description,
            source_url  = EXCLUDED.source_url,
            visits      = EXCLUDED.visits,
            revenue     = EXCLUDED.revenue
        WHERE pa.seo_titles_blueprints.status <> 'pushed'
    """, (bp['cat_id'], bp['key'], bp['cat_name'], bp['title'], bp['h1_title'],
          bp['description'], bp['country_code'], source_url, visits, revenue))


def _has_unique_title(cur, url):
    """True if pa.unique_titles_content already holds a non-empty title for url."""
    cur.execute("""
        SELECT c.title
        FROM pa.unique_titles_content c
        JOIN pa.urls u ON u.url_id = c.url_id
        WHERE u.url = pa.canonicalize_url(%s)
    """, (url,))
    row = cur.fetchone()
    return bool(row and row['title'])


# ---------------------------------------------------------------------------
# Threaded run orchestration (mirrors ai_titles_service pattern)
# ---------------------------------------------------------------------------
_state_lock = threading.Lock()
_seo_state = {"status": "idle"}


def _reset_state(top_n, date_from, date_to):
    with _state_lock:
        _seo_state.clear()
        _seo_state.update({
            "status": "running", "phase": "starting",
            "top_n": top_n, "date_from": date_from, "date_to": date_to,
            "urls_fetched": 0, "scanned": 0, "no_cat": 0, "no_facets": 0,
            "dup": 0, "skipped_existing": 0, "new_combos": 0,
            "skipped_local": 0, "skipped_store": 0, "store_errors": 0,
            "store_candidates": 0, "store_checked": 0, "store_total": 0,
            "titles_generated": 0, "titles_skipped": 0, "titles_failed": 0,
            "message": "", "should_stop": False,
            "started_at": time.time(), "finished_at": None,
        })


def _set(**kw):
    with _state_lock:
        _seo_state.update(kw)


def _inc(key, n=1):
    with _state_lock:
        _seo_state[key] = _seo_state.get(key, 0) + n


def get_run_status():
    with _state_lock:
        return dict(_seo_state)


def stop_run():
    with _state_lock:
        if _seo_state.get("status") == "running":
            _seo_state["should_stop"] = True
            return {"stopped": True}
    return {"stopped": False, "message": "no run in progress"}


def _stopping():
    with _state_lock:
        return _seo_state.get("should_stop", False)


def start_run(top_n=100, date_from=None, date_to=None):
    with _state_lock:
        if _seo_state.get("status") == "running":
            return {"started": False, "message": "a run is already in progress"}
    _reset_state(top_n, date_from, date_to)
    threading.Thread(target=_run, args=(top_n, date_from, date_to), daemon=True).start()
    return {"started": True, "top_n": top_n}


# ---------------------------------------------------------------------------
# Publish progress
# ---------------------------------------------------------------------------
# publish_built() stays synchronous — /api/seo-titles/publish already runs it in
# an executor, so the POST is off the event loop and the frontend can poll this
# state WHILE its own fetch is still in flight. That keeps the endpoint contract
# unchanged (the POST still returns the full result) and needs no extra thread.
_pub_lock = threading.Lock()
_pub_state = {"status": "idle"}


def _pub_reset(env, total):
    with _pub_lock:
        _pub_state.clear()
        _pub_state.update({
            "status": "running", "phase": "starting", "env": env,
            "total": total, "done": 0, "pushed": 0, "failed": 0,
            "batch": 0, "batches": 0, "message": "",
            "started_at": time.time(), "finished_at": None,
        })


def _pub_set(**kw):
    with _pub_lock:
        _pub_state.update(kw)


def get_publish_status():
    """Progress of the current/last publish. `pct` is derived here so every
    caller shows the same number."""
    with _pub_lock:
        s = dict(_pub_state)
    total, done = s.get("total") or 0, s.get("done") or 0
    s["pct"] = round(100.0 * done / total, 1) if total else (100.0 if s.get("status") == "done" else 0.0)
    return s


def mark_publish_error(msg):
    """Flag the publish state as failed. Called by the endpoint's except branch —
    without it a raising publish_built() would leave the bar spinning forever."""
    with _pub_lock:
        if _pub_state.get("status") == "running":
            _pub_state.update({"status": "error", "phase": "error",
                               "message": str(msg), "finished_at": time.time()})


def _run(top_n, date_from, date_to):
    try:
        # Try to keep the taxonomy slug->cat_id map warm.
        from backend.url_validator_service import _cache as taxonomy_cache

        _set(phase="fetching_urls")
        rows = fetch_top_urls(top_n, date_from, date_to)
        _set(urls_fetched=len(rows))

        rules = load_rules()
        local = load_local_combos(force=True)
        # Facet dependencies, so combos that cannot exist on the site are never
        # built. Empty map = cache not populated yet -> behave as before.
        deps = load_facet_deps()

        # ---- pass 1: parse + dedup. No HTTP here, so the whole URL list is
        # reduced to distinct candidate combos before a single GET is spent.
        _set(phase="scanning_urls")
        seen = set()          # every (cat_id, canon_key) examined this run
        candidates = []       # (ck, cat, types, url, visits, revenue)
        for r in rows:
            if _stopping():
                break
            _inc("scanned")
            url = (r.get('url') or '').lower()
            p = parse_url(url)
            if not p:
                continue
            leaf, types = p
            cat = _resolve_cat(taxonomy_cache, leaf)
            if not cat:
                _inc("no_cat")
                continue
            if not types:
                _inc("no_facets")
                continue
            # A dependent facet without its parent cannot be reached, so the
            # blueprint would be dead weight. Checked BEFORE the dedup/existing
            # checks so the counter reflects every such URL seen, not just the
            # first occurrence of each combo.
            bad = impossible_reason(types, deps)
            if bad:
                _inc("impossible")
                continue
            # dedup on the canonical (cat_id, key) — identical form used by
            # load_local_combos, so the same combo is never counted twice
            ck = (cat['cat_id'], canon_key('~'.join(sorted(types))))
            if ck in seen:
                _inc("dup")
                continue
            seen.add(ck)
            if ck in local:
                _inc("skipped_existing")
                _inc("skipped_local")
                continue
            candidates.append((ck, cat, types, url, r.get('visits'), r.get('revenue')))

        # ---- pass 2: ask the store about the survivors. One GET per candidate,
        # parallelized and memoised; see store_has_combos.
        in_store = set()
        if candidates and not _stopping():
            _set(phase="checking_store", store_candidates=len(candidates))
            in_store = store_has_combos(
                [c[0] for c in candidates],
                progress=lambda done, tot: _set(store_checked=done, store_total=tot))
            _set(store_checked=len(candidates), store_total=len(candidates))

        # ---- pass 3: build what neither we nor the store already have
        _set(phase="building_blueprints")
        created = set()       # unique (cat_id, canon_key) actually built this run
        new_sources = []      # (source_url) per new combo, for AI-title generation
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            for ck, cat, types, url, visits, revenue in candidates:
                if _stopping():
                    break
                if ck in in_store:
                    _inc("skipped_existing")
                    _inc("skipped_store")
                    continue
                bp = build_blueprint(cat['cat_id'], cat.get('cat_name', ''), types, rules)
                _upsert_blueprint(cur, bp, url, visits, revenue)
                conn.commit()
                if ck not in created:
                    created.add(ck)
                    new_sources.append(url)
                    _set(new_combos=len(created))  # unique created combos
        finally:
            cur.close()
            return_db_connection(conn)

        # AI unique titles for the source URLs of the new combos (best effort,
        # parallelized). Blueprint push does not depend on these succeeding.
        _set(phase="generating_titles")
        _generate_titles(new_sources)

        _set(phase="done", status="done", finished_at=time.time())
    except Exception as e:
        _set(phase="error", status="error", message=str(e), finished_at=time.time())


def _generate_titles(source_urls, workers=10):
    if not source_urls:
        return
    from backend.ai_titles_service import process_single_url

    # Skip URLs that already have a unique title.
    #
    # Eén query voor de hele batch, en FAIL CLOSED. Hiervoor liep hier een lus met
    # één cursor en `except Exception: todo.append(u)`. Zodra één lookup faalde stond
    # de verbinding in een afgebroken transactie (geen rollback), gooide élke volgende
    # _has_unique_title InFailedSqlTransaction, en belandde de hele rest van de batch
    # in `todo` — waarna process_single_url() onvoorwaardelijk naar
    # pa.unique_titles_content schrijft en handgeschreven titels overschrijft.
    # Kan de check niet worden uitgevoerd, dan genereren we juist NIET.
    todo = []
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Canonicaliseren blijft in SQL: pa.canonicalize_url() is de enige
        # bron van waarheid en een Python-kopie die één teken afwijkt zou juist
        # rijen missen — en dan overschrijven we alsnog handwerk.
        cur.execute("""
            WITH want AS (
                SELECT x AS raw, pa.canonicalize_url(x) AS canon
                FROM unnest(%s::text[]) AS x
            )
            SELECT w.raw AS raw
            FROM want w
            JOIN pa.urls u ON u.url = w.canon
            JOIN pa.unique_titles_content c ON c.url_id = u.url_id
            WHERE COALESCE(c.title, '') <> ''
        """, (list(source_urls),))
        covered = {r['raw'] for r in cur.fetchall()}
    except Exception as e:
        conn.rollback()
        # `message` is het veld dat de frontend toont; een eigen key zou onzichtbaar zijn.
        _set(message=f"unique-title-check mislukt, geen titels gegenereerd: {e}")
        _inc("titles_skipped", len(source_urls))
        return
    finally:
        cur.close()
        return_db_connection(conn)

    for u in source_urls:
        if u in covered:
            _inc("titles_skipped")
        else:
            todo.append(u)

    def _one(u):
        if _stopping():
            return
        try:
            res = process_single_url(u)
            if res.get("status") == "success":
                _inc("titles_generated")
            else:
                _inc("titles_failed")
        except Exception:
            _inc("titles_failed")

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_one, todo))


# ---------------------------------------------------------------------------
# Publish: POST built blueprints -> /page-titles, then push per-URL AI titles
# ---------------------------------------------------------------------------
def _post_page_titles(records, env):
    """POST a JSON array of records to /page-titles. Returns (ok, status, text)."""
    url = PAGE_TITLES_API[env]
    key = PAGE_TITLES_KEY[env]()
    if not key:
        return False, 0, f"missing API key for env={env}"
    headers = {"X-Api-Key": key, "Content-Type": "application/json"}
    last = None
    for attempt in range(1, 4):
        try:
            resp = requests.post(url, headers=headers, json=records, timeout=600)
            ok = resp.status_code in (200, 201)
            return ok, resp.status_code, (resp.text or "")[:500]
        except requests.RequestException as e:
            last = e
            time.sleep(2 * attempt)
    return False, 0, f"transport error after retries: {last}"


def update_blueprint(cat_id, key, title, h1_title, description):
    """Update the editable fields (title, h1_title, description) of a single
    blueprint identified by (cat_id, key). Returns {"updated": <rowcount>}."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE pa.seo_titles_blueprints
            SET title = %s, h1_title = %s, description = %s
            WHERE cat_id = %s AND key = %s
        """, (title, h1_title, description, int(cat_id), key))
        conn.commit()
        return {"updated": cur.rowcount}
    finally:
        cur.close()
        return_db_connection(conn)


def upsert_blueprint_built(cat_id, key, cat_name, title, h1_title, description):
    """Create (or refresh) a blueprint as status='built' from an edited row.
    Used when an "existing combo" row is edited so it moves into the Built set.
    Never downgrades a row that was already 'pushed'.

    The key is canonicalised on the way in. build_blueprint() emits a sorted key,
    but this path takes whatever /api/seo-titles/create-built was handed by the
    frontend, i.e. the facet order as it appeared in the URL. Storing that raw
    made the same combo insertable twice under two spellings — the PK is the raw
    (cat_id, key) while the dedup in load_local_combos() compares canon_key()
    — which is how 450 combos ended up duplicated (cleaned up 2026-08-10)."""
    key = canon_key(key)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO pa.seo_titles_blueprints
                (cat_id, key, cat_name, title, h1_title, description, country_code,
                 status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, 'NL', 'built', now())
            ON CONFLICT (cat_id, key) DO UPDATE SET
                cat_name    = EXCLUDED.cat_name,
                title       = EXCLUDED.title,
                h1_title    = EXCLUDED.h1_title,
                description = EXCLUDED.description,
                status      = 'built'
            WHERE pa.seo_titles_blueprints.status <> 'pushed'
        """, (int(cat_id), key, cat_name, title, h1_title, description))
        conn.commit()
        return {"upserted": cur.rowcount}
    finally:
        cur.close()
        return_db_connection(conn)


def remove_blueprints(combos):
    """Delete unpushed (built/failed) blueprints for the given combos. Never
    touches 'pushed' rows so the dedup push-log stays intact."""
    if not combos:
        return {"removed": 0}
    conn = get_db_connection()
    cur = conn.cursor()
    removed = 0
    try:
        for c in combos:
            cur.execute("""
                DELETE FROM pa.seo_titles_blueprints
                WHERE cat_id = %s AND key = %s AND status <> 'pushed'
            """, (int(c['cat_id']), c['key']))
            removed += cur.rowcount
        conn.commit()
        return {"removed": removed}
    finally:
        cur.close()
        return_db_connection(conn)


def publish_built(env="production", push_unique_titles=False, combos=None):
    """Push status='built' blueprints to /page-titles (batched upsert), flip
    successful ones to 'pushed', then push the per-URL AI titles. When `combos`
    is given, only those (cat_id, key) built rows are pushed; otherwise all."""
    if env not in PAGE_TITLES_API:
        return {"success": False, "message": f"unknown env {env!r}"}

    _pub_reset(env, 0)
    _pub_set(phase="fetching")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT cat_id, key, title, h1_title, description, country_code
            FROM pa.seo_titles_blueprints
            WHERE status = 'built'
            ORDER BY cat_id, key
        """)
        built = cur.fetchall()
    finally:
        cur.close()
        return_db_connection(conn)

    if combos:
        wanted = {(int(c['cat_id']), c['key']) for c in combos}
        built = [r for r in built if (int(r['cat_id']), r['key']) in wanted]

    if not built:
        _pub_set(status="done", phase="done", finished_at=time.time(),
                 message="no matching built blueprints to publish")
        return {"success": True, "pushed": 0, "message": "no matching built blueprints to publish"}

    # Pre-flight length check. /page-titles validates a POST atomically, so ONE
    # oversized record 400s the whole 5000-row batch and flips all 5000 to
    # 'failed'. Quarantine offenders up front so the rest of the batch still goes.
    too_long = [r for r in built
                if len(r['title'] or '') > MAX_TITLE_LEN or len(r['h1_title'] or '') > MAX_H1_LEN]
    if too_long:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.executemany("""
                UPDATE pa.seo_titles_blueprints
                SET status='failed', last_error=%s
                WHERE cat_id=%s AND key=%s
            """, [(f"skipped before push: title {len(r['title'] or '')} / h1_title "
                   f"{len(r['h1_title'] or '')} chars exceeds the {MAX_TITLE_LEN} cap",
                   int(r['cat_id']), r['key']) for r in too_long])
            conn.commit()
        finally:
            cur.close()
            return_db_connection(conn)
        skipped = {(int(r['cat_id']), r['key']) for r in too_long}
        built = [r for r in built if (int(r['cat_id']), r['key']) not in skipped]
        if not built:
            _pub_set(status="done", phase="done", finished_at=time.time(),
                     message=f"all {len(too_long)} candidates exceed the length cap")
            return {"success": False, "pushed": 0, "failed": len(too_long),
                    "message": f"all {len(too_long)} candidates exceed the length cap"}

    _pub_set(phase="pushing", total=len(built), skipped_too_long=len(too_long),
             batches=(len(built) + PUSH_BATCH - 1) // PUSH_BATCH)

    pushed = 0
    failed = 0
    batch_results = []
    for i in range(0, len(built), PUSH_BATCH):
        batch = built[i:i + PUSH_BATCH]
        _pub_set(batch=i // PUSH_BATCH + 1)
        records = [{
            "cat_id": int(r['cat_id']),
            "key": r['key'],
            "title": r['title'] or "",
            "h1_title": r['h1_title'] or "",
            "description": r['description'] or "",
            "country_code": r['country_code'] or "NL",
        } for r in batch]
        ok, code, text = _post_page_titles(records, env)
        batch_results.append({"batch": i // PUSH_BATCH + 1, "count": len(batch),
                              "ok": ok, "status_code": code, "response": text})
        combos = [(int(r['cat_id']), r['key']) for r in batch]
        c2 = get_db_connection()
        cur2 = c2.cursor()
        try:
            if ok:
                cur2.executemany("""
                    UPDATE pa.seo_titles_blueprints
                    SET status='pushed', pushed_at=now(), last_error=NULL
                    WHERE cat_id=%s AND key=%s
                """, combos)
                pushed += len(batch)
            else:
                cur2.executemany("""
                    UPDATE pa.seo_titles_blueprints
                    SET status='failed', last_error=%s
                    WHERE cat_id=%s AND key=%s
                """, [(text, cid, k) for (cid, k) in combos])
                failed += len(batch)
            c2.commit()
        finally:
            cur2.close()
            return_db_connection(c2)
        # count the batch as done only after its status flip is committed, so the
        # bar never runs ahead of what is actually persisted
        _pub_set(done=min(i + len(batch), len(built)), pushed=pushed, failed=failed)

    result = {
        "success": failed == 0 and not too_long,
        "env": env,
        "pushed": pushed,
        "failed": failed + len(too_long),
        "batches": batch_results,
    }
    if too_long:
        result["skipped_too_long"] = [
            {"cat_id": int(r['cat_id']), "key": r['key'],
             "title_len": len(r['title'] or ''), "h1_len": len(r['h1_title'] or '')}
            for r in too_long
        ]

    # OFF BY DEFAULT since 2026-07-31. This is not "the AI titles for the blueprints
    # just pushed" — upload_titles_to_api() regenerates a CSV of ALL ~1,02M unique
    # titles and re-uploads the whole file, which is byte-for-byte the same operation
    # as Publish All in the Unique Titles tool. It added ~20 minutes to a 10-minute
    # blueprint push and made Publish look hung. Unique titles have their own tool;
    # publishing them is that tool's job. The flag stays for a caller that explicitly
    # wants both in one go.
    if push_unique_titles and pushed:
        _pub_set(phase="unique_titles")
        try:
            from backend.unique_titles import upload_titles_to_api
            result["unique_titles_push"] = upload_titles_to_api()
        except Exception as e:
            result["unique_titles_push"] = {"success": False, "error": str(e)}

    _pub_set(phase="refreshing_dedup")
    load_local_combos(force=True)  # refresh dedup set with the new pushes
    _pub_set(status="done", phase="done", finished_at=time.time(),
             message=f"pushed {pushed}, failed {failed}")
    return result


# ---------------------------------------------------------------------------
# Read helpers for the frontend
# ---------------------------------------------------------------------------
def get_preview(limit=100, status="built"):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # "existing" comes from the tblPageTitles export table, not the blueprints
        # tool table. Shape the rows like blueprint rows so the frontend renders them
        # in the same grid (cat_name / source_url / visits are not tracked there).
        if status == "existing":
            # pa.page_titles_existing (the tblPageTitles export) carries TWO row
            # layouts merged from different export generations:
            #   * "normal"  — key=facet combo, title=page title, h1_title=H1, description=meta
            #   * "shifted" — key=category name, title=facet combo, h1_title=page title, description=H1
            # Detect which by whether the `title` column holds a real page title
            # (contains 'beslist.nl'); then normalise the text columns into the
            # same (facet key / title / h1_title / description) shape. Deepest cat
            # comes from the cat_name column (backfilled from the Taxonomy tree by
            # cat_id via scripts/backfill_page_titles_existing_catname.py), so it's
            # populated for both layouts, not just the shifted rows.
            cur.execute("""
                SELECT cat_id,
                       CASE WHEN title ILIKE '%%beslist.nl%%' THEN key         ELSE title       END AS key,
                       cat_name,
                       CASE WHEN title ILIKE '%%beslist.nl%%' THEN title       ELSE h1_title    END AS title,
                       CASE WHEN title ILIKE '%%beslist.nl%%' THEN h1_title    ELSE description END AS h1_title,
                       -- meta description: the row's own (normal layout) if present, else the
                       -- category browse_description fetched from /html-title-descriptions
                       COALESCE(
                           CASE WHEN title ILIKE '%%beslist.nl%%' THEN NULLIF(description, '') END,
                           NULLIF(browse_description, '')
                       ) AS description,
                       NULL::text AS source_url, NULL::int AS visits, NULL::numeric AS revenue,
                       'existing' AS status,
                       NULL::timestamp AS created_at, NULL::timestamp AS pushed_at,
                       NULL::text AS example_title
                FROM pa.page_titles_existing
                ORDER BY cat_id, key
                LIMIT %s
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]
        cur.execute("""
            SELECT b.cat_id, b.key, b.cat_name, b.title, b.h1_title, b.description,
                   b.source_url, b.visits, b.revenue, b.status, b.created_at, b.pushed_at,
                   c.title AS example_title
            FROM pa.seo_titles_blueprints b
            LEFT JOIN pa.urls u ON u.url = pa.canonicalize_url(b.source_url)
            LEFT JOIN pa.unique_titles_content c ON c.url_id = u.url_id
            WHERE (%s = 'all' OR b.status = %s)
            ORDER BY b.visits DESC NULLS LAST, b.cat_id, b.key
            LIMIT %s
        """, (status, status, limit))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def get_recent(limit=20):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT cat_id, key, cat_name, title, status, visits, created_at, pushed_at
            FROM pa.seo_titles_blueprints
            ORDER BY COALESCE(pushed_at, created_at) DESC
            LIMIT %s
        """, (limit,))
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def get_stats():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # NOTE: a row count of the July snapshot, not of the live store. The store
        # has no list endpoint, so its total cannot be counted — only individual
        # combos can be looked up (store_has_combos / get_store_record).
        cur.execute("SELECT count(*) AS n FROM pa.page_titles_existing")
        existing = cur.fetchone()['n']
        cur.execute("""
            SELECT status, count(*) AS n
            FROM pa.seo_titles_blueprints GROUP BY status
        """)
        by_status = {row['status']: row['n'] for row in cur.fetchall()}
        cur.execute("""
            SELECT count(*) AS n, count(*) FILTER (WHERE found) AS found
            FROM pa.page_titles_api_cache
        """)
        cache = cur.fetchone()
        return {
            "existing_blueprints": existing,
            "built": by_status.get("built", 0),
            "pushed": by_status.get("pushed", 0),
            "failed": by_status.get("failed", 0),
            "store_checked": cache['n'],
            "store_found": cache['found'],
        }
    finally:
        cur.close()
        return_db_connection(conn)
