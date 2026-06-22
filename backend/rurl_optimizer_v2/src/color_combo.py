"""V43: colour-combination facet enrichment.

Beslist `kleurcombinaties_*` facets (colour pairs such as "Blauw/wit") only
surface in the Search API when a `kleur` facet value is already applied as a
filter, so they are absent from the cached facet universe (built from bare
category enumeration). When a redirect already pins a single base `kleur~<id>`
facet AND the query names a SECOND colour, this module live-probes the Search
API with that kleur value applied, finds a `kleurcombinaties_*` value whose
name covers both colours, and appends it:

    /c/kleur~400983              ("servies blauw-wit", Blauw matched)
 -> /c/kleur~400983~~kleurcombinaties_woonacc~23450550   (value "Blauw/wit")

The probe both finds the value AND proves it exists with products (count>=1),
so the appended facet bypasses the cache-backed facet_url_exists check that
would otherwise reject an un-enumerated facet. Results are memoised per worker;
for large batches a prefetch (cf. facet_probe.py) would avoid the live calls.
"""
import json
import re
import sqlite3
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

from src.db_loader import SEARCH_BASE_URL, SEARCH_LOCALE, DataLoader, HTTP_TIMEOUT
from src.search_derived import _is_fresh  # 7-day TTL freshness (CACHE_TTL_DAYS)

# Persistent probe cache. A SEPARATE sqlite file (not the shared
# search_derived.sqlite) so enabling WAL here can't change the journal mode the
# search_derived / facet_probe caches run under. Bump COMBO_SCHEMA_VERSION to
# invalidate all cached picks after a selection-logic change.
_COMBO_DB_PATH = Path(__file__).parent.parent / "data" / "cache" / "color_combo.sqlite"
COMBO_SCHEMA_VERSION = 1
_MISS = object()  # sentinel: key absent/stale (distinct from a cached "no combo")

# Base colour vocabulary = the common `kleur` facet value names (lowercased).
# Only used to detect colour TOKENS in the keyword; the value-name match below
# is vocabulary-independent (it compares raw token sets), so an unusual colour
# in a combo value still matches as long as the keyword's colours are a subset.
COLORS = frozenset({
    'zwart', 'wit', 'blauw', 'rood', 'groen', 'grijs', 'zilver', 'geel',
    'bruin', 'roze', 'oranje', 'goud', 'paars', 'beige', 'multicolor',
    'taupe', 'brons', 'ecru', 'koper', 'camel', 'cognac', 'huid', 'platina',
    'transparant', 'creme', 'crème', 'antraciet', 'turquoise', 'bordeaux',
    'mint', 'zalm', 'lila', 'naturel', 'donkerblauw', 'lichtblauw',
    'donkergroen', 'lichtgrijs', 'rosegoud', 'zilvergrijs',
})

_FACET_META: Optional[dict] = None


def _facet_meta() -> dict:
    """facet_id -> urlSlug, loaded once per worker."""
    global _FACET_META
    if _FACET_META is None:
        try:
            _FACET_META = DataLoader(use_cache=True)._fetch_facet_meta()
        except Exception:
            _FACET_META = {}
    return _FACET_META


def keyword_colors(keyword: str) -> list:
    """Distinct colour tokens in the keyword, in order of appearance."""
    out = []
    for t in re.findall(r'[a-zà-ž]+', (keyword or '').lower()):
        if t in COLORS and t not in out:
            out.append(t)
    return out


def _subcat_slug(url: str) -> str:
    if not url:
        return ""
    path = url.split('/c/', 1)[0].rstrip('/')
    if '/products/' in path:
        path = path.split('/products/', 1)[-1]
    parts = path.split('/')
    return parts[1] if len(parts) >= 2 else ""


def _connect(readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        return sqlite3.connect(f"file:{_COMBO_DB_PATH}?mode=ro", uri=True, timeout=5)
    conn = sqlite3.connect(_COMBO_DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")  # safe concurrent reads + worker writes
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS color_combo_cache (
            subcat_slug TEXT NOT NULL,
            kleur_id    TEXT NOT NULL,
            colors_key  TEXT NOT NULL,
            payload     TEXT NOT NULL,
            fetched_at  TEXT NOT NULL,
            combo_schema INTEGER NOT NULL,
            PRIMARY KEY (subcat_slug, kleur_id, colors_key)
        )
        """
    )
    return conn


def _combo_get(subcat_slug: str, kleur_id: str, colors_key: str):
    """Return the cached payload dict, or _MISS when absent/stale/old-schema."""
    if not _COMBO_DB_PATH.exists():
        return _MISS
    try:
        c = _connect(readonly=True)
    except sqlite3.OperationalError:
        return _MISS
    try:
        row = c.execute(
            "SELECT payload, fetched_at, combo_schema FROM color_combo_cache "
            "WHERE subcat_slug=? AND kleur_id=? AND colors_key=?",
            (subcat_slug, kleur_id, colors_key),
        ).fetchone()
        if not row or row[2] != COMBO_SCHEMA_VERSION or not _is_fresh(row[1]):
            return _MISS
        return json.loads(row[0])
    except (sqlite3.OperationalError, ValueError):
        return _MISS
    finally:
        c.close()


def _combo_put(subcat_slug: str, kleur_id: str, colors_key: str, payload: dict) -> None:
    try:
        c = _connect(readonly=False)
    except sqlite3.OperationalError:
        return
    try:
        c.execute(
            "INSERT OR REPLACE INTO color_combo_cache "
            "(subcat_slug, kleur_id, colors_key, payload, fetched_at, combo_schema) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (subcat_slug, kleur_id, colors_key, json.dumps(payload),
             datetime.now(timezone.utc).isoformat(), COMBO_SCHEMA_VERSION),
        )
        c.commit()
    except sqlite3.OperationalError:
        pass  # another worker is writing; the miss just re-probes next time
    finally:
        c.close()


def _live_probe(subcat_slug: str, kleur_id: str, want: set):
    """One Search API call with kleur=<kleur_id> applied; return
    (facet_name, value_id, value_name) of a kleurcombinaties_* value whose name
    covers every colour in `want`, or None."""
    params = {
        'category': subcat_slug,
        'countryLanguage': SEARCH_LOCALE,
        'isBot': 'true',
        'limit': '1',
        'trackTotalHits': 'true',
        'filters[kleur][0]': str(kleur_id),
    }
    url = f"{SEARCH_BASE_URL}/search/products?{urllib.parse.urlencode(params)}"
    try:
        data = requests.get(url, timeout=HTTP_TIMEOUT).json()
    except Exception:
        return None
    meta = _facet_meta()
    for f in (data.get('facets') or []):
        name = (meta.get(f.get('id')) or '').lower()
        if not name.startswith('kleurcombinat'):
            continue
        for v in (f.get('values') or []):
            vn = (v.get('facetValue') or '').lower()
            vcolors = set(re.findall(r'[a-zà-ž]+', vn))
            if want and want <= vcolors and (v.get('count') or 0) >= 1:
                return (meta.get(f.get('id')), v.get('id'), v.get('facetValue'))
    return None


def _probe_combo(subcat_slug: str, kleur_id: str, colors):
    """Cache-first lookup of the matching kleurcombinaties_* value. Reads the
    persistent sqlite cache (incl. a negative 'no combo' result); on a miss it
    live-probes once and writes the outcome back. Returns
    (facet_name, value_id, value_name) or None."""
    if not subcat_slug or not kleur_id:
        return None
    cols = sorted(set(colors))
    colors_key = "+".join(cols)
    if not colors_key:
        return None
    cached = _combo_get(subcat_slug, str(kleur_id), colors_key)
    if cached is not _MISS:
        combo = cached.get("combo")
        return tuple(combo) if combo else None
    result = _live_probe(subcat_slug, str(kleur_id), set(cols))
    _combo_put(subcat_slug, str(kleur_id), colors_key,
               {"combo": list(result)} if result else {"no_combo": True})
    return result


def enrich(redirect_url: str, keyword: str):
    """If the redirect pins a single base kleur facet and the keyword names a
    second colour, append the matching kleurcombinaties_* facet.

    Returns (new_url, (facet_name, value_id, value_name)) on a hit, else
    (redirect_url, None)."""
    if not redirect_url or '/c/' not in redirect_url:
        return redirect_url, None
    if 'kleurcombinat' in redirect_url:           # already has one
        return redirect_url, None
    frag = redirect_url.split('/c/', 1)[1]
    kleur_id = None
    for axis in frag.split('~~'):
        if '~' not in axis:
            continue
        fname, fval = axis.split('~', 1)
        if fname == 'kleur':                      # the base colour facet only
            kleur_id = fval
            break
    if not kleur_id:
        return redirect_url, None
    cols = keyword_colors(keyword)
    if len(cols) < 2:
        return redirect_url, None
    combo = _probe_combo(_subcat_slug(redirect_url), str(kleur_id), frozenset(cols))
    if not combo:
        return redirect_url, None
    fname, vid, vname = combo
    new_url = redirect_url.rstrip('/') + f"~~{fname}~{vid}"
    return new_url, combo
