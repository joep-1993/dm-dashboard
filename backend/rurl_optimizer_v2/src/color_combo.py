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
import re
import urllib.parse
from functools import lru_cache
from typing import Optional

import requests

from src.db_loader import SEARCH_BASE_URL, SEARCH_LOCALE, DataLoader, HTTP_TIMEOUT

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


@lru_cache(maxsize=8192)
def _probe_combo(subcat_slug: str, kleur_id: str, colors_key: frozenset):
    """Probe the Search API with kleur=<kleur_id> applied; return
    (facet_name, value_id, value_name) of a kleurcombinaties_* value whose name
    covers every colour in colors_key, or None."""
    if not subcat_slug or not kleur_id:
        return None
    params = {
        'category': subcat_slug,
        'countryLanguage': SEARCH_LOCALE,
        'isBot': 'true',
        'limit': '1',
        'trackTotalHits': 'true',
        f'filters[kleur][0]': str(kleur_id),
    }
    url = f"{SEARCH_BASE_URL}/search/products?{urllib.parse.urlencode(params)}"
    try:
        data = requests.get(url, timeout=HTTP_TIMEOUT).json()
    except Exception:
        return None
    want = set(colors_key)
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
