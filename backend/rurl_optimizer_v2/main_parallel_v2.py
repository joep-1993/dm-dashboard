"""
Beslist.nl R-URL Redirect Optimizer - Parallel Processing V2

Optimized version using shared memory and batch processing.
Key improvements:
- Pre-loads all data before spawning workers
- Uses imap_unordered for better throughput
- Larger chunk sizes for less overhead
- Saves incrementally for recovery

Usage:
    python3 main_parallel_v2.py data/input/r_urls_full.csv -o output/results.csv -w 12
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import argparse
import logging
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import re
import sys
import time
import pickle

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def preload_data(use_cache=True):
    """Pre-load all data needed for processing."""
    from src.db_loader import DataLoader
    from src.facet_filter import FacetFilter

    print("Pre-loading data...")
    start = time.time()

    loader = DataLoader(use_cache=use_cache)
    facets_df = loader.load_facets()
    categories_df = loader.load_categories()

    # Pre-compute category lookup
    category_lookup = {}
    for _, row in categories_df.iterrows():
        url_name = row['url_name']
        if url_name:
            parts = str(url_name).split('_')
            for part in reversed(parts):
                if part.isdigit():
                    category_lookup[part] = row['display_name']
                    break

    # Pre-compute type facets
    facet_filter = FacetFilter(facets_df)
    all_type_facets = facet_filter.get_all_type_facets()

    loader.close()

    elapsed = time.time() - start
    print(f"Data loaded in {elapsed:.1f}s")
    print(f"  - {len(facets_df)} facet records")
    print(f"  - {len(category_lookup)} category mappings")
    print(f"  - {len(all_type_facets)} type facets")

    return {
        'facets_df': facets_df,
        'categories_df': categories_df,
        'category_lookup': category_lookup,
        'all_type_facets': all_type_facets
    }


def save_data_cache(data, cache_file):
    """Save preloaded data to pickle cache."""
    with open(cache_file, 'wb') as f:
        pickle.dump(data, f)


def load_data_cache(cache_file):
    """Load preloaded data from pickle cache."""
    with open(cache_file, 'rb') as f:
        return pickle.load(f)


# Global for worker processes
_worker_data = None


# V34: when True, the multi-facet rescue appends an explicit query size
# (XL, 122-128) onto the assembled /c/ URL. ON by default (2026-06-06) — the
# size match is always COLLECTED in the probe cache and is now emitted unless
# explicitly disabled (CLI: --no-rescue-include-size). NOTE: per-size pages
# churn in/out of stock faster than the type/fanshop/colour axes, so a
# size-narrowed redirect can go thin if that size sells out; disable per-run
# with --no-rescue-include-size if that's a concern.
# Worker processes pick it up via init_worker_v2 initargs.
RESCUE_INCLUDE_SIZE = True


def init_worker_v2(cache_file, fuzzy_threshold, use_token_coverage=True,
                   rescue_include_size=True):
    """Initialize worker with pre-cached data."""
    global _worker_data, RESCUE_INCLUDE_SIZE
    RESCUE_INCLUDE_SIZE = rescue_include_size

    from src.parser import RUrlParser
    from src.facet_filter import FacetFilter
    from src.matcher import KeywordMatcher
    from src.url_builder import UrlBuilder

    # Suppress logging in workers
    logging.getLogger().setLevel(logging.WARNING)

    # Load pre-cached data
    data = load_data_cache(cache_file)

    facet_filter = FacetFilter(data['facets_df'])
    builder = UrlBuilder()
    # V32: let the builder verify brand/shop facets across subcat depths.
    builder.facet_url_exists = facet_filter.facet_url_set().__contains__

    _worker_data = {
        'parser': RUrlParser(),
        'facet_filter': facet_filter,
        'matcher': KeywordMatcher(fuzzy_threshold=fuzzy_threshold,
                                  use_token_coverage=use_token_coverage),
        'builder': builder,
        'category_lookup': data['category_lookup'],
        'all_type_facets': data['all_type_facets'],
        'categories_df': data['categories_df']  # V14: For subcategory name matching
    }


def extract_subcategory_id_from_url(url):
    """Extract subcategory ID from redirect URL."""
    if not url:
        return ""
    try:
        if '/c/' in url:
            path = url.split('/c/')[0]
        else:
            path = url
        parts = path.rstrip('/').split('/')
        if parts:
            subcat_name = parts[-1]
            id_parts = subcat_name.split('_')
            if len(id_parts) >= 2:
                for part in reversed(id_parts):
                    if part.isdigit():
                        return part
    except Exception:
        pass
    return ""


def _maybe_promote_to_specific_subcat(
    subcat_match, matched_word, parsed, categories_df, facet_filter, matcher
):
    """V14.1 specificity rescue. The per-word loop in step 3 picks the highest-
    scoring single-word subcat match and breaks — e.g. "gereedschap trolley"
    → "Gereedschap" (klussen_486173, exact 100), missing the deeper sibling
    "Gereedschapskoffers" (klussen_486172_1348201) whose own facets carry the
    matching "Trolley" type.

    If the chosen subcat can't absorb any leftover keyword token via its
    facets, scan deeper same-maincat siblings whose first display word shares
    a 4+ char prefix with the matched word. If one of them DOES carry a
    facet that absorbs a leftover token, swap to it. The downstream
    _append_facet_to_subcat_redirect then attaches that facet to the URL.
    """
    from src.validation_rules import STOPWORDS, SHOP_NAMES

    matched_lower = (matched_word or '').lower()
    keyword_tokens = [
        w for w in (parsed.keyword or '').lower().split()
        if w not in STOPWORDS and w not in SHOP_NAMES and len(w) >= 3
    ]
    # A token is "leftover" if it isn't the matched word and isn't a substring
    # of the matched word in either direction (covers "gereedschap" being
    # absorbed by a candidate's "gereedschapskoffers").
    leftover = [
        w for w in keyword_tokens
        if w != matched_lower
        and w not in matched_lower
        and matched_lower not in w
    ]
    if not leftover:
        return subcat_match

    main_cat = parsed.main_category or ''
    cur_url = subcat_match.get('url_name', '') or ''

    def _facet_hits(url_name):
        target_id = ''
        for part in reversed(url_name.split('_')):
            if part.isdigit():
                target_id = part
                break
        if not target_id:
            return 0
        fdf = facet_filter.filter_by_subcategory(target_id)
        if fdf.empty:
            return 0
        fvs = facet_filter.get_facet_values(fdf)
        hits = 0
        for tok in leftover:
            r = matcher.match_with_partial(tok, fvs, exclude_winkel=True)
            if r.is_match:
                hits += 1
                continue
            # Compound-suffix fallback. Dutch retail facets often glue the
            # subcat noun onto the modifier ("trolley" → "Gereedschapstrolley"),
            # and the matcher's MIN_LENGTH_RATIO guard blocks the short token
            # against the long facet value. In this rescue we've already
            # established the subcat-stem context, so a suffix-end match on
            # a non-strict facet is a reliable extra signal.
            if len(tok) < 4:
                continue
            for fv in fvs:
                if fv.facet_name.lower() in ('winkel', 'merk'):
                    continue
                vname = fv.facet_value_name.lower()
                if vname.endswith(tok) and len(vname) - len(tok) >= 3:
                    hits += 1
                    break
        return hits

    if _facet_hits(cur_url) > 0:
        # Winner already covers a leftover token via its own facets — keep.
        return subcat_match

    cur_depth = cur_url.count('_')
    best = None  # (facet_hits, depth, new_match_dict)
    for _, row in categories_df.iterrows():
        url_name = row.get('url_name', '') or ''
        display = row.get('display_name', '') or ''
        if not url_name.startswith(main_cat + '_'):
            continue
        if url_name == cur_url:
            continue
        # Must be strictly deeper than the current winner.
        if url_name.count('_') <= cur_depth:
            continue
        first_word = display.split()[0].lower() if display else ''
        if not first_word:
            continue
        # First display word shares a 4+ char prefix with the matched word.
        common = 0
        for a, b in zip(first_word, matched_lower):
            if a == b:
                common += 1
            else:
                break
        if common < min(4, len(matched_lower), len(first_word)):
            continue

        hits = _facet_hits(url_name)
        if hits == 0:
            continue

        key = (hits, url_name.count('_'))
        if best is None or key > (best[0], best[1]):
            new_match = {
                'matched_category': display,
                'url_name': url_name,
                'category_path': f'/products/{main_cat}/{url_name}',
                'score': subcat_match.get('score', 100),
                'match_type': 'subcategory_name_specific',
            }
            best = (hits, url_name.count('_'), new_match)

    if best is None:
        return subcat_match
    return best[2]


def _leftover_token_matches_facet_token(kw_tok, fv_tok, matcher):
    """A facet token counts as covered by a leftover token when they're
    morphologically equivalent (Dutch plural/diminutive suffixes) OR the
    facet token is a Dutch compound ending in the leftover token, e.g.
    'gereedschapstrolley' ↔ 'trolley'. The compound-suffix arm requires a
    ≥3-char prefix on the facet side so it doesn't false-positive on tiny
    coincidental endings."""
    if matcher._tokens_equal_modulo_morphology(kw_tok, fv_tok):
        return True
    if (len(kw_tok) >= 3
            and fv_tok.endswith(kw_tok)
            and len(fv_tok) - len(kw_tok) >= 3):
        return True
    return False


def _collect_longest_per_axis_from_leftover(leftover_tokens, facet_values, matcher):
    """For each non-strict facet axis (excluding winkel + merk), return the
    facet value whose tokens are all covered by the leftover tokens,
    preferring the LONGEST facet value name. Catches cases the joined-
    leftover matcher misses due to MIN_LENGTH_RATIO ("Dames" vs
    "pescara dames" trips length-ratio 5/13<0.4) and lets a multi-attribute
    leftover ("rood dames") attach one facet per axis (kleur~Rood +
    doelgroep_mode~Dames). Returns {axis_name: MatchResult}."""
    from src.matcher import MatchResult
    by_axis = {}
    for fv in facet_values:
        axis = fv.facet_name.lower()
        if axis in ('winkel', 'merk'):
            continue
        fv_tokens = matcher._coverage_tokens(fv.facet_value_name)
        if not fv_tokens:
            continue
        if not all(
            any(_leftover_token_matches_facet_token(kt, ft, matcher)
                for kt in leftover_tokens)
            for ft in fv_tokens
        ):
            continue
        existing = by_axis.get(axis)
        if (existing is None
                or len(fv.facet_value_name)
                > len(existing.facet_value.facet_value_name)):
            by_axis[axis] = MatchResult(
                keyword=' '.join(leftover_tokens),
                facet_value=fv,
                match_type='leftover_longest_per_axis',
                score=90,
                matched_text=fv.facet_value_name,
            )
    return by_axis


def _append_facet_to_subcat_redirect(result, parsed, subcategory_match, facet_filter, matcher):
    # When a subcat-name match wins (e.g. "tuinkast kunststof" → "Tuinkasten")
    # the leftover token ("kunststof") was previously thrown away. Match it
    # against the target subcat's own facet pool and tack the winning facet
    # onto the redirect URL.
    if not result or not getattr(result, 'success', False) or not result.redirect_url:
        return result

    url_name = (subcategory_match or {}).get('url_name', '')
    target_subcat_id = ''
    for part in reversed(url_name.split('_')):
        if part.isdigit():
            target_subcat_id = part
            break
    if not target_subcat_id:
        return result

    matched_name = (subcategory_match.get('matched_category', '') or '').lower()
    # Drop 1–2 char fragments: a subcat name like "T-shirts" tokenizes to
    # {'t', 'shirts'}, and a 1-char 't' would substring-match (and wrongly
    # absorb) any leftover token containing a 't' — e.g. "elftal", "thuis" —
    # before facet matching ever sees them. Real category nouns we want to
    # absorb ("shirts", "tuinkasten") are all >= 3 chars.
    matched_words = {w for w in re.findall(r'\w+', matched_name) if len(w) >= 3}

    def _absorbed_by_subcat(tok: str) -> bool:
        # Treat the token as "already matched" if it's a substring of any
        # matched-name word or vice versa — so "scharnieren" is absorbed by
        # "deurscharnieren" and "tuinkast" by "tuinkasten", leaving only the
        # truly leftover modifiers (e.g. "kunststof") for facet matching.
        for mw in matched_words:
            if tok == mw or tok in mw or mw in tok:
                return True
        return False

    leftover_tokens = [
        w for w in (parsed.keyword or '').lower().split()
        if not _absorbed_by_subcat(w) and len(w) >= 3
    ]
    if not leftover_tokens:
        return result
    leftover = ' '.join(leftover_tokens)

    facets_df = facet_filter.filter_by_subcategory(target_subcat_id)
    if facets_df.empty:
        return result
    facet_values = facet_filter.get_facet_values(facets_df)
    if not facet_values:
        return result

    # Multi-axis longest-per-axis collector. For every non-strict facet axis
    # (kleur, materiaal, doelgroep_mode, type_*, …) pick the facet value
    # whose tokens are all covered by the leftover, preferring the LONGEST
    # facet value name on ties ("Nike Air" over "Nike", "Lichtblauw met
    # stippen" over "Blauw"). Replaces the legacy joined → compound-suffix
    # → per-token-first-hit chain and gives multi-attribute leftovers like
    # "rood dames" both kleur~Rood and doelgroep_mode~Dames.
    matches_by_axis = _collect_longest_per_axis_from_leftover(
        leftover_tokens, facet_values, matcher,
    )

    # Joined-leftover safety net. The token-equality scan above can miss
    # typos or partial-substring matches that the matcher's fuzzy paths
    # would catch (e.g. "scharniren" → "Scharnieren"). Run match_with_partial
    # too and merge into the same axis dict, keeping the longer value on
    # collision.
    fmatch = matcher.match_with_partial(leftover, facet_values, exclude_winkel=True)
    if (fmatch.is_match
            and fmatch.facet_value is not None
            and fmatch.facet_value.facet_name.lower() not in ('winkel', 'merk')):
        axis = fmatch.facet_value.facet_name.lower()
        existing = matches_by_axis.get(axis)
        if (existing is None
                or len(fmatch.facet_value.facet_value_name)
                > len(existing.facet_value.facet_value_name)):
            matches_by_axis[axis] = fmatch

    # Per-token EXACT merk pass. The subcat-name match has already established
    # product/category context, so a score=100 brand hit on a single leftover
    # token is safe to attach (e.g. "bic" + subcat "Aanstekers" → merk~BIC).
    # Mirrors STRICT_FACET_EXACT_THRESHOLD in matcher.match_multi_word.
    from src.validation_rules import (
        STRICT_FACET_EXACT_THRESHOLD,
        STOPWORDS,
        SHOP_NAMES,
    )
    merk_facets = [fv for fv in facet_values if fv.facet_name.lower() == 'merk']
    merk_match = None
    if merk_facets:
        for tok in leftover_tokens:
            if (len(tok) >= 3
                    and tok not in STOPWORDS
                    and tok not in SHOP_NAMES):
                cand = matcher.match_with_partial(tok, merk_facets, exclude_winkel=False)
                if cand.is_match and cand.score >= STRICT_FACET_EXACT_THRESHOLD:
                    merk_match = cand
                    break

    # Order the non-merk axes by facet value length descending (most specific
    # first, stable for stable output), then append merk last.
    appends = sorted(
        matches_by_axis.values(),
        key=lambda m: -len(m.facet_value.facet_value_name),
    )
    if merk_match:
        appends.append(merk_match)

    # V34: deterministic size append (flag-gated). The fuzzy leftover collector
    # above can't match clothing/shoe sizes ("122-128", "XL") — they're <3 chars
    # or numeric, so size_tokens does it deterministically against this subcat's
    # own maat_* values. Appended last (least navigational intent) and only when
    # RESCUE_INCLUDE_SIZE is on, mirroring the V28 search-derived rescue. Skipped
    # if a size axis was somehow already collected, so we never double-append.
    if RESCUE_INCLUDE_SIZE:
        from src.matcher import MatchResult
        from src.facet_probe import _is_size_facet
        from src.size_tokens import extract_sizes, match_size_value
        already_size = any(_is_size_facet(m.facet_value.facet_name) for m in appends)
        sizes = extract_sizes(parsed.keyword) if not already_size else []
        if sizes:
            size_fvs = [fv for fv in facet_values if _is_size_facet(fv.facet_name)]
            hit = match_size_value(sizes, [(fv.facet_value_id, fv.facet_value_name)
                                           for fv in size_fvs])
            if hit:
                hit_id = hit[0]
                size_fv = next((fv for fv in size_fvs
                                if fv.facet_value_id == hit_id), None)
                if size_fv is not None:
                    appends.append(MatchResult(
                        keyword=parsed.keyword,
                        facet_value=size_fv,
                        match_type='size_token',
                        score=90,
                        matched_text=size_fv.facet_value_name,
                    ))

    if not appends:
        return result

    for m in appends:
        fragment = m.facet_value.url_fragment
        if '/c/' in result.redirect_url:
            result.redirect_url = result.redirect_url.rstrip('/') + '~~' + fragment
            result.facet_fragment = (
                result.facet_fragment + '~~' + fragment
                if result.facet_fragment else fragment
            )
            result.facet_count = (result.facet_count or 0) + 1
        else:
            result.redirect_url = result.redirect_url.rstrip('/') + '/c/' + fragment
            result.facet_fragment = fragment
            result.facet_count = 1

    result.facet_names = ', '.join(m.facet_value.facet_name for m in appends)
    result.facet_value_names = ', '.join(
        (m.matched_text or m.facet_value.facet_value_name) for m in appends
    )
    result.reason = (
        (result.reason or '')
        + f"; appended {result.facet_fragment} ({result.facet_value_names!r}) from leftover '{leftover}'"
    )
    return result


def _rescue_long_unmatched_token(keyword, target_text, threshold=8, prefix_link=False):
    """Hard-reject guard for the V28 search-derived rescue path.

    Returns the first non-stopword, non-generic-adjective query token of
    length >= threshold whose stem is NOT present as a token of target_text
    (the rescued dom_cat name + appended facet value name), else None.

    prefix_link (V33): when True, a long token also counts as represented if
    a target token is its >=4-char prefix (or vice versa) — so the assembled
    facet value 'Thuis' covers the query token 'thuisshirt'. Only the
    multi-facet rescue passes this; the single-facet path keeps the strict
    stem-equality test so it can't be loosened into the false-positives it
    was built to block (waterfilter / inductiekookplaat).

    Used only on the rescue path — where the matcher FAILED and search
    guessed a category — so a long product-type token the guess dropped
    (e.g. 'inductiekookplaat', 'bewegingssensor', 'waterfilter') yields no
    redirect instead of a confident-but-wrong one. Stem-equality (not
    substring) is deliberate: 'filter' is a token of 'waterfilter' but a
    'Filter' attribute is a different product than a water filter.
    Legitimate subcategory_name matches (e.g. 'hoesloze dekbedden') never
    reach this path, so their semantic-coverage facets are unaffected.
    """
    import re as _re
    from src.validation_rules import STOPWORDS, SHOP_NAMES, GENERIC_ADJECTIVES

    def _stem(t):
        t = t.lower()
        if len(t) > 3 and t.endswith('s'):
            t = t[:-1]
        if len(t) > 3 and t.endswith('e'):
            t = t[:-1]
        return t

    target_toks = {_stem(t) for t in _re.findall(r'[a-z0-9]+', (target_text or '').lower())}
    for w in (keyword or '').lower().split():
        if len(w) < threshold:
            continue
        if w in STOPWORDS or w in SHOP_NAMES or w in GENERIC_ADJECTIVES:
            continue
        sw = _stem(w)
        if sw in target_toks:
            continue
        if prefix_link and any(
            (len(tt) >= 4 and sw.startswith(tt)) or (len(sw) >= 4 and tt.startswith(sw))
            for tt in target_toks
        ):
            continue
        return w
    return None


def _assemble_multi_facet(multi, existing_facet, size_facet=None):
    """V33: build a multi-facet /c/ fragment from the cached multi_facets list
    (one keyword-consistent value per axis). Preserves any facet the original
    R-URL already carried and never repeats a facet name. Returns
    (fragment, [value_name, ...]) or ('', []) when nothing assemblable.

    V34: when ``size_facet`` is provided (caller opted into honouring an
    explicit query size), it's appended last — after the intent axes — so the
    landing page is size-narrowed. Off by default; see RESCUE_INCLUDE_SIZE."""
    existing_names = {p.split('~', 1)[0]
                      for p in (existing_facet or '').split('~~') if '~' in p}
    seen = set(existing_names)
    frags, names = [], []
    for m in list(multi or []) + ([size_facet] if size_facet else []):
        fn = m.get('facet_name')
        vid = m.get('value_id')
        if not fn or vid is None or fn in seen:
            continue
        seen.add(fn)
        frags.append(f"{fn}~{vid}")
        names.append(m.get('value_name') or '')
    if not frags:
        return '', []
    fragment = '~~'.join(([existing_facet] if existing_facet else []) + frags)
    return fragment, names


def _is_bare_category_noun(tok: str, cat_name: str) -> bool:
    """Whole-token test: True only when the token IS the category noun (or its
    Dutch singular/plural stem), NOT merely contains it. So for category
    "Shirts": 'shirt'/'shirts' -> True, but the compound 'trainingsshirt'
    (which discovers the Sportshirts child) -> False. This is the token-level
    version of the 1-char-fragment guard from the _absorbed_by_subcat fix:
    substring containment over-absorbs, equality does not."""
    if not tok or not cat_name:
        return False
    c = cat_name.lower()
    cstem = c.rstrip('s').rstrip('en')
    tstem = tok.rstrip('s').rstrip('en')
    return tok == c or tok == cstem or tstem == c or tstem == cstem


def _split_strip_keyword(keyword: str, cat_name: str) -> list:
    """Tokenize on whitespace AND hyphens (global-pass style), then drop any
    token that is the bare category noun. 'nike-nederlands-elftal-trainingsshirt'
    with cat "Shirts" -> ['nike','nederlands','elftal','trainingsshirt'] (nothing
    dropped — no token equals 'shirt'); 'nike-shirt' -> ['nike']."""
    import re as _re
    toks = [t for t in _re.split(r'[\s-]+', (keyword or '').lower()) if t]
    return [t for t in toks if not _is_bare_category_noun(t, cat_name)]


def _facet_url_parts(facet_url: str):
    """Extract {main_category, subcategory_name, subcategory_id} from a facet
    value URL like '/products/mode/mode_432360_469350/c/type_sportshirts~...'.
    Mirrors process_global_rurls._extract_category_from_facet_url."""
    if not facet_url:
        return None
    path = facet_url.split('/c/')[0] if '/c/' in facet_url else facet_url
    path = path.rstrip('/')
    if '/products/' in path:
        path = path.split('/products/')[-1]
    parts = path.split('/')
    if len(parts) < 2:
        return None
    subcat_name = parts[1]
    subcat_id = ''
    for p in reversed(subcat_name.split('_')):
        if p.isdigit():
            subcat_id = p
            break
    return {'main_category': parts[0], 'subcategory_name': subcat_name,
            'subcategory_id': subcat_id}


def _facet_fragment_superset(child_fragment, parent_fragment):
    """True iff every facet axis (name~id) in parent_fragment is also present
    in child_fragment AND child carries at least one extra. Used by the
    maincat-mode rescue to adopt only strict enrichments — e.g. the cascade's
    'fanshop~1335065' being superseded by 'fanshop~1335065~~type_sportshirts~9253235'
    — while refusing a rescue that drops or swaps the cascade's facet."""
    def _axes(frag):
        return {p for p in (frag or '').split('~~') if p}
    child = _axes(child_fragment)
    parent = _axes(parent_fragment)
    return bool(parent) and parent < child


def _resolve_category_noun_anchor(keyword, categories_df, main_category, matcher,
                                  min_score=95):
    """Return {'subcat_id','url_name','display_name'} for the best (sub)category
    whose display name a keyword token NAMES (score >= min_score), within
    main_category; else None.

    A query token that names a category is a category signal, not a facet:
    'shirt' -> "Shirts" (mode_432360). Resolving that anchor lets the caller
    keep the head-noun out of facet matching, so a generic noun never drives a
    mono-category facet (type_sportshirts lives only in Sportshirts) and
    over-narrows the redirect. Specific compounds ('trainingsshirt',
    'voetbalshirt') do NOT name a category, so they fall through to the normal
    type-facet discovery and can still descend to a child."""
    import re as _re
    if categories_df is None or not keyword:
        return None
    candidates = [keyword] + [t for t in _re.split(r'[\s_-]+', keyword.lower())
                              if len(t) >= 4]
    best, seen = None, set()
    for cand in candidates:
        if cand in seen:
            continue
        seen.add(cand)
        m = matcher.match_subcategory_name(cand, categories_df,
                                           main_category=main_category)
        if m and m.get('score', 0) >= min_score:
            if best is None or m['score'] > best['score']:
                best = m
    if not best:
        return None
    url_name = best.get('url_name', '')
    subcat_id = ''
    for p in reversed(url_name.split('_')):
        if p.isdigit():
            subcat_id = p
            break
    if not subcat_id:
        return None
    return {'subcat_id': subcat_id, 'url_name': url_name,
            'display_name': best.get('matched_category', '')}


def _derive_facets_in_subtree(parsed, anchor_subcat_id, anchor_cat_name,
                              facet_filter, matcher, all_type_facets, builder,
                              categories_df=None):
    """Convergence helper: derive facets the global-pass way, but bounded by the
    category the URL already pins.

    (C) When the URL pins only a main category and categories_df is supplied, a
        keyword token that NAMES a (sub)category ('shirt' -> "Shirts") first
        anchors that category — keeping the generic head-noun out of facet
        matching so it can't drive a mono-category facet (type_sportshirts).
    (1) Tokenize the keyword on hyphens + drop the bare category noun.
    (2) Discover the best type facet WITHIN the anchor's subtree (the anchor
        subcat + any deeper child) to find the right child subcat — e.g.
        'trainingsshirt' -> type_sportshirts living in mode_432360_469350.
    (3) Descend into that subcat and run a full multi-facet match there
        (fanshop/merk/ut_voetbalshirt/…). Fall back to a multi-facet match in
        the anchor subcat itself.

    Returns a builder result (with >=1 facet) or None. Bounding the discovery
    to the subtree makes this strictly safer than the unanchored global pass:
    it cannot jump to an unrelated category (the meubel->Kapstokmeubels class
    of error), because every candidate lives under the URL's own category."""
    from src.parser import ParsedRUrl

    # (B) maincat_mode: the URL pinned only a main category (a top-level
    # /products/<maincat>/r/<kw>/), so there is no subcat to anchor to.
    # Discovery is then bounded to the whole main category instead of a single
    # subcat subtree. This is what lets a bare /products/mode/r/ URL still
    # discover 'trainingsshirt' -> type_sportshirts (mode_432360_469350) and
    # recombine it with the fanshop facet section 4 found on its own.
    maincat_mode = not anchor_subcat_id
    anchor_slug = parsed.subcategory_name or ''

    # (C) Category-noun anchoring (maincat mode). A query token that NAMES a
    # (sub)category is a category signal, not a facet: 'shirt' -> "Shirts"
    # (mode_432360). Anchor there and strip the noun, so a generic head-noun
    # never drives a mono-category facet like type_sportshirts (only present in
    # Sportshirts) and over-narrows the redirect. Remaining specific tokens can
    # still descend to a child via the type-facet discovery below.
    if maincat_mode and categories_df is not None and parsed.main_category:
        cat_anchor = _resolve_category_noun_anchor(
            parsed.keyword, categories_df, parsed.main_category, matcher)
        if cat_anchor:
            anchor_subcat_id = cat_anchor['subcat_id']
            anchor_cat_name = cat_anchor['display_name']
            anchor_slug = cat_anchor['url_name']
            maincat_mode = False

    search_kw = ' '.join(_split_strip_keyword(parsed.keyword, anchor_cat_name))
    if not search_kw.strip():
        return None
    if maincat_mode:
        if not parsed.main_category:
            return None
    elif not anchor_slug:
        return None

    def _accept(res):
        return res if (res and getattr(res, 'facet_count', 0) >= 1) else None

    # (1)+(2) type-facet discovery constrained to the anchor scope: the anchor
    # subtree (subcat mode) or the whole main category (maincat mode).
    subtree_types = []
    for fv in (all_type_facets or []):
        cp = _facet_url_parts(fv.url)
        if not cp:
            continue
        if maincat_mode:
            if cp.get('main_category') == parsed.main_category:
                subtree_types.append(fv)
        else:
            slug = cp['subcategory_name']
            if slug == anchor_slug or slug.startswith(anchor_slug + '_'):
                subtree_types.append(fv)

    result = None
    if subtree_types:
        type_matches = [m for m in (matcher.match_multi_word(
            search_kw, subtree_types, all_type_facets=None,
            require_type_for_merk=True,
            current_main_category=parsed.main_category) or []) if m.facet_value]
        if type_matches:
            best = max(type_matches, key=lambda m: m.score)
            disc = _facet_url_parts(best.facet_value.url)
            if disc and disc.get('subcategory_id'):
                dfacets = facet_filter.get_facet_values(
                    facet_filter.filter_by_subcategory(disc['subcategory_id']))
                if dfacets:
                    multi = matcher.match_multi_word(
                        search_kw, dfacets, all_type_facets=None,
                        require_type_for_merk=True,
                        current_main_category=disc['main_category'])
                    if multi:
                        pseudo = ParsedRUrl(
                            original_url=parsed.original_url,
                            category_path=f"{disc['main_category']}/{disc['subcategory_name']}",
                            full_category_path=f"/products/{disc['main_category']}/{disc['subcategory_name']}",
                            main_category=disc['main_category'],
                            subcategory_id=disc['subcategory_id'],
                            subcategory_name=disc['subcategory_name'],
                            keyword=parsed.keyword,
                            existing_facet=getattr(parsed, 'existing_facet', '') or '',
                        )
                        res = builder.build_multi_facet(pseudo, multi)
                        res.reason = f"[subtree_type_descend] {res.reason}"
                        result = _accept(res)

    # (3) fallback: multi-facet match directly in the anchor subcat.
    # Skipped in maincat mode — there is no anchor subcat, and a maincat-wide
    # multi-match would just reproduce section 4's facet-collapse.
    if not result and not maincat_mode:
        afacets = facet_filter.get_facet_values(
            facet_filter.filter_by_subcategory(anchor_subcat_id))
        if afacets:
            multi = matcher.match_multi_word(
                search_kw, afacets, all_type_facets=None,
                require_type_for_merk=True,
                current_main_category=parsed.main_category)
            if multi:
                # When the anchor came from a category-noun match (not the URL),
                # parsed has no subcat — build against a pseudo pinned to the
                # anchor so the redirect lands on e.g. mode_432360 "Shirts".
                if parsed.subcategory_id == anchor_subcat_id:
                    build_parsed = parsed
                else:
                    build_parsed = ParsedRUrl(
                        original_url=parsed.original_url,
                        category_path=f"{parsed.main_category}/{anchor_slug}",
                        full_category_path=f"/products/{parsed.main_category}/{anchor_slug}",
                        main_category=parsed.main_category,
                        subcategory_id=anchor_subcat_id,
                        subcategory_name=anchor_slug,
                        keyword=parsed.keyword,
                        existing_facet=getattr(parsed, 'existing_facet', '') or '',
                    )
                res = builder.build_multi_facet(build_parsed, multi)
                res.reason = f"[subtree_anchor] {res.reason}"
                result = _accept(res)

    return result


def _has_strong_subcat_name_match(parsed, categories_df, matcher, threshold=95):
    """True if the keyword (full or a meaningful word) names a subcategory in
    the URL's own subtree or main category at >= threshold. Mirrors the matching
    in steps 2b/3 — used to decide whether a purely cross-category step-1 match
    should be deferred so the subcategory-name steps can win instead."""
    if categories_df is None or not parsed.keyword:
        return False
    from src.validation_rules import STOPWORDS as _SW, SHOP_NAMES as _SN
    contexts = [c for c in (parsed.subcategory_name, parsed.main_category) if c]
    candidates = [parsed.keyword] + [
        w for w in parsed.keyword.lower().split()
        if len(w) >= 4 and w not in _SW and w not in _SN
    ]
    for ctx in contexts:
        for cand in candidates:
            m = matcher.match_subcategory_name(cand, categories_df, main_category=ctx)
            if m and m.get('score', 0) >= threshold:
                return True
    return False


def process_url_v2(args):
    """Process single URL in worker."""
    global _worker_data

    url, multi_facet = args

    import re  # Nodig voor DIMENSION_PATTERN + V30 coverage check (moet vóór gebruik staan)
    from src.reliability_scorer import calculate_reliability_score, get_reliability_tier, compute_h1_similarity, _v27_reject_reason
    from src.search_derived import derive_redirect as derive_search_redirect
    from src.facet_probe import derive_facet as derive_search_facet
    from src.validation_rules import STOPWORDS, SHOP_NAMES

    # Hard exclusion: external API URLs that should never be processed.
    # Skip parsing/matching entirely and emit a visible row with a reason.
    EXCLUDED_HOSTS = ("api.scrape.do",)
    url_lower = (url or "").lower()
    if any(h in url_lower for h in EXCLUDED_HOSTS):
        return {
            'original_url': url,
            'main_category': '',
            'original_category': '',
            'keyword': '',
            'redirect_url': None,
            'redirect_category': '',
            'is_cross_category': False,
            'facet_fragment': '',
            'facet_names': '',
            'facet_value_names': '',
            'facet_count': 0,
            'match_score': 0,
            'match_type': 'excluded',
            'reliability_score': 0,
            'reliability_tier': 'D',
            'matched_keywords': '',
            'unmatched_keywords': '',
            'match_coverage': 0.0,
            'has_stopwords': False,
            'stopwords_found': '',
            'shop_in_keyword': '',
            'keyword_type': 'excluded',
            'has_dimensions': False,
            'merk_of_shop_missing': '',
            'success': False,
            'reason': 'Excluded URL: external API host (api.scrape.do)',
        }

    d = _worker_data
    parser = d['parser']
    facet_filter = d['facet_filter']
    matcher = d['matcher']
    builder = d['builder']
    category_lookup = d['category_lookup']
    all_type_facets = d['all_type_facets']

    # Parse URL
    parsed = parser.parse(url)
    if not parsed.is_valid:
        return {
            'original_url': url,
            'main_category': '',
            'original_category': '',
            'keyword': '',
            'redirect_url': None,
            'redirect_category': '',
            'is_cross_category': False,
            'facet_fragment': '',
            'facet_names': '',
            'facet_value_names': '',
            'facet_count': 0,
            'match_score': 0,
            'match_type': 'none',
            'reliability_score': 0,
            'reliability_tier': 'D',
            'matched_keywords': '',
            'unmatched_keywords': '',
            'match_coverage': 0.0,
            'has_stopwords': False,
            'stopwords_found': '',
            'shop_in_keyword': '',  # V23
            'keyword_type': 'no_matchable',  # V23.1
            'has_dimensions': False,  # V23.2
            'success': False,
            'reason': parsed.error_message
        }

    # V27: Stopwords-only short-circuit. If every token in the keyword is a
    # stopword (e.g. "de goedkoopste", "beste koop consumentenbond"), there
    # is nothing to match against a facet — and the engine's previous
    # fallback (no redirect, score 0) discarded perfectly usable category
    # traffic. Redirect to the clean category page that the R-URL already
    # carried instead. Skipped for parsed URLs without a category — those
    # don't apply (mainpage search URLs fail parsing earlier anyway).
    from src.validation_rules import STOPWORDS, SHOP_NAMES, detect_shops_in_keyword as _detect_shops
    _kw_tokens = [w for w in (parsed.keyword or '').lower().split() if len(w) >= 2]
    _shops_in_kw = _detect_shops(parsed.keyword)
    _non_stop_non_shop = [w for w in _kw_tokens if w not in STOPWORDS and w not in SHOP_NAMES]
    if (parsed.full_category_path
            and _kw_tokens
            and not _non_stop_non_shop
            and not _shops_in_kw):
        # Preserve any /c/<facet> the original URL already carried — a
        # stopwords-only keyword ("beste getest", "de goedkoopste") means there
        # is nothing to MATCH, but it must not silently drop a facet selection
        # the URL pinned (e.g. /r/beste_getest/c/afmeting_bedbodem_bed_matras~…).
        # Mirrors the V32 category-noun-only short-circuit below.
        _ef27 = getattr(parsed, 'existing_facet', '') or ''
        _base27 = f"https://www.beslist.nl{parsed.full_category_path}"
        clean_url = f"{_base27}/c/{_ef27}" if _ef27 else f"{_base27}/"
        return {
            'original_url': url,
            'main_category': parsed.main_category or '',
            'original_category': category_lookup.get(parsed.subcategory_id, '') if parsed.subcategory_id else '',
            'keyword': parsed.keyword,
            'redirect_url': clean_url,
            'redirect_category': category_lookup.get(parsed.subcategory_id, '') if parsed.subcategory_id else (parsed.main_category or ''),
            'is_cross_category': False,
            'facet_fragment': _ef27,
            'facet_names': _ef27.split('~', 1)[0] if _ef27 else '',
            'facet_value_names': '',
            'facet_count': 1 if _ef27 else 0,
            'match_score': 0,
            'match_type': 'stopwords_only_clean_category',
            'reliability_score': 80,  # category page is a safe landing — no facet so no bad-facet risk
            'reliability_tier': 'B',
            'h1_similarity': 0,
            'reject_reason': '',
            'matched_keywords': '',
            'unmatched_keywords': ', '.join(_kw_tokens),
            'match_coverage': 0.0,
            'has_stopwords': True,
            'stopwords_found': ', '.join(t for t in _kw_tokens if t in STOPWORDS),
            'shop_in_keyword': '',
            'keyword_type': 'stopwords_only',
            'has_dimensions': False,
            'merk_of_shop_missing': '',
            'success': True,
            'reason': 'V27: keyword is stopwords-only — redirected to clean category URL'
                      + (f" (preserved existing facet '{_ef27}')" if _ef27 else ''),
        }

    # V32: "redundant keyword" short-circuit. When every meaningful keyword
    # token is just the head noun of the subcategory the URL is ALREADY in
    # (e.g. /products/mode/mode_432360/r/shirt/ — "shirt" == the "Shirts"
    # subcat), there's nothing left to match on a facet. Matching it anyway
    # lets the category noun fuzzy-hit an unrelated sub-type value
    # ("shirt" → type_sportshirts "Fitness-shirts") and, worse, DROP a facet
    # the URL already carried. Keep the category page and preserve any
    # existing /c/ facet instead.
    _sub_name = category_lookup.get(parsed.subcategory_id, '') if parsed.subcategory_id else ''
    if parsed.subcategory_id and _sub_name and _non_stop_non_shop and not _shops_in_kw:
        # Residual = keyword tokens (hyphen-split) minus the bare category noun,
        # using WHOLE-TOKEN equality. The old check used substring containment
        # (`_cat_stem in w`), which judged 'nike-nederlands-elftal-trainingsshirt'
        # to be "just the Shirts category noun" because 'shirt' is a substring of
        # the glued token — collapsing a rich query to the bare category page.
        # Now V32 only fires when nothing meaningful remains after dropping bare
        # category nouns (e.g. /mode_432360/r/shirt/ or /r/shirts/).
        _residual = [w for w in _split_strip_keyword(parsed.keyword, _sub_name)
                     if w not in STOPWORDS and w not in SHOP_NAMES and len(w) >= 2]
        if not _residual:
            _base = f"https://www.beslist.nl{parsed.full_category_path}"
            _ef = getattr(parsed, 'existing_facet', '') or ''
            _clean_url = f"{_base}/c/{_ef}" if _ef else f"{_base}/"
            return {
                'original_url': url,
                'main_category': parsed.main_category or '',
                'original_category': _sub_name,
                'keyword': parsed.keyword,
                'redirect_url': _clean_url,
                'redirect_category': _sub_name,
                'is_cross_category': False,
                'facet_fragment': _ef,
                'facet_names': _ef.split('~', 1)[0] if _ef else '',
                'facet_value_names': '',
                'facet_count': 1 if _ef else 0,
                'match_score': 0,
                'match_type': 'category_noun_only_clean_category',
                'reliability_score': 80,
                'reliability_tier': 'B',
                'h1_similarity': 0,
                'reject_reason': '',
                'matched_keywords': '',
                'unmatched_keywords': ', '.join(_non_stop_non_shop),
                'match_coverage': 0.0,
                'has_stopwords': False,
                'stopwords_found': '',
                'shop_in_keyword': '',
                'keyword_type': 'category_noun_only',
                'has_dimensions': False,
                'merk_of_shop_missing': '',
                'success': True,
                'reason': (f"V32: keyword '{parsed.keyword}' is just the '{_sub_name}' category "
                           "noun — kept category"
                           + (f" + existing facet '{_ef}'" if _ef else " page")),
            }

    # V30: Shop-name short-circuit — if the keyword contains any SHOP_NAME
    # word, skip matching entirely. Row stays in the output for visibility
    # but without a redirect URL.
    _shops = _shops_in_kw
    if _shops:
        return {
            'original_url': url,
            'main_category': parsed.main_category or '',
            'original_category': category_lookup.get(parsed.subcategory_id, '') if parsed.subcategory_id else '',
            'keyword': parsed.keyword,
            'redirect_url': None,
            'redirect_category': '',
            'is_cross_category': False,
            'facet_fragment': '',
            'facet_names': '',
            'facet_value_names': '',
            'facet_count': 0,
            'match_score': 0,
            'match_type': 'shop_name',
            'reliability_score': 0,
            'reliability_tier': 'D',
            'matched_keywords': '',
            'unmatched_keywords': '',
            'match_coverage': 0.0,
            'has_stopwords': False,
            'stopwords_found': '',
            'shop_in_keyword': ', '.join(_shops),
            'keyword_type': 'shop_only',
            'has_dimensions': False,
            'merk_of_shop_missing': '',
            'success': False,
            'reason': 'shop_name detected',
        }

    result = None
    # Holds a step-1 match that is PURELY cross-category (every facet hit lives
    # in another main category). We defer it so the own-subtree subcategory-NAME
    # steps (2b/3) get first chance — a stray token must not pre-empt naming the
    # child subcat the keyword actually points at. Restored as a last-resort
    # fallback below if nothing better is found.
    _deferred_cross_result = None

    # ==========================================================================
    # MATCHING VOLGORDE (V14.1):
    # 1. Subcategory facets (als subcategory_id aanwezig)
    # 2. Parent subcategory facets
    # 3. V14.1: Subcategorie naam matching met HOGE score (≥95) binnen maincat
    #    -> Generieke termen zoals "scharnieren" gaan naar subcategorie "Deurscharnieren"
    # 4. Main category facets (alle facets binnen maincat)
    #    -> Specifieke termen zoals "onzichtbare scharnieren" gaan naar facet
    # 5. V14: Subcategorie naam matching (lagere scores) binnen maincat
    # 6. V14: Cross-category subcategorie naam matching
    # 7. Category-only fallback
    # ==========================================================================

    from src.validation_rules import SUBCATEGORY_MATCH_THRESHOLD

    # Filter facets (only if we have a subcategory_id)
    facet_values = []
    if parsed.subcategory_id:
        filtered_facets = facet_filter.filter_by_subcategory(parsed.subcategory_id)
        facet_values = facet_filter.get_facet_values(filtered_facets)

    # 1. SUBCATEGORY FACETS - Multi-facet matching
    if not result and facet_values:
        if multi_facet or ' ' in parsed.keyword:
            # Q10: pass the subcategory's display name so its head noun (e.g.
            # "partytent" when already in the Partytenten subcat) is skipped
            # during facet matching — otherwise the category noun matches a
            # sub-type value like t_partytent "Zijwanden partytent", a false
            # positive that over-narrows the redirect.
            _sub_cat_name = (category_lookup.get(str(parsed.subcategory_id), '')
                             if parsed.subcategory_id else '')
            match_results = matcher.match_multi_word(
                parsed.keyword, facet_values,
                all_type_facets=all_type_facets,
                require_type_for_merk=True,
                current_main_category=parsed.main_category,
                category_name=_sub_cat_name,
            )
            if match_results:
                # Defer a purely cross-category result (every hit has a
                # cross_category_path) ONLY when the keyword also names a
                # subcategory in the URL's own subtree/maincat at high score —
                # then steps 2b/3 should win. e.g. /…_557622/r/rolgordijn_zonder_
                # boren/ (Raamdecoratie): stray token "boren" hits cross-maincat
                # "Appelboren" (Keukenhulpjes), but "rolgordijn" names child
                # subcat Rolgordijnen (99) → defer. Without a strong subcat-name
                # alternative we keep the cross-category match (often the best
                # option, e.g. "toilet fontein" → t_wastafel), so good cross-cat
                # matches with no better home are not stripped.
                _all_cross = all(getattr(mr, 'cross_category_path', None)
                                 for mr in match_results)
                if _all_cross and _has_strong_subcat_name_match(
                        parsed, d.get('categories_df'), matcher):
                    _deferred_cross_result = builder.build_multi_facet(
                        parsed, match_results)
                else:
                    result = builder.build_multi_facet(parsed, match_results)

    # 1. SUBCATEGORY FACETS - Single facet
    if not result and facet_values:
        match_result = matcher.match_with_partial(parsed.keyword, facet_values)
        if match_result.is_match:
            result = builder.build(parsed, match_result)

    # 1c. OWN-SUBCAT COMPOUND RETRY — must run BEFORE the parent/sibling
    # fallback (step 2). A glued Dutch compound ("antislipmat" = "antislip" +
    # "mat") doesn't match its base facet value ("Antislip" in o_matten) until
    # it's decomposed. Without trying the decomposed form against the URL's OWN
    # subcat here, step 2's parent_subcat fallback steals a weaker secondary
    # token onto a sibling facet — e.g. /…_6674987/r/antislipmat_bad-douche/
    # (Douchematten) matched "douche" → sibling Zeepdispensers t_zeepd "Douche"
    # instead of "antislip" → own o_matten "Antislip". Scoped to the own subcat
    # and same-maincat hits only, so it can't introduce a cross-category jump.
    if not result and facet_values and parsed.keyword:
        from src.synonyms import expand_compounds as _expand_compounds
        _sub_cat_name_1c = (category_lookup.get(str(parsed.subcategory_id), '')
                            if parsed.subcategory_id else '')
        for _variant in _expand_compounds(parsed.keyword)[1:]:  # skip original
            if ' ' in _variant:
                _vm = matcher.match_multi_word(
                    _variant, facet_values,
                    all_type_facets=all_type_facets,
                    require_type_for_merk=True,
                    current_main_category=parsed.main_category,
                    category_name=_sub_cat_name_1c,
                )
                _vm = [mr for mr in (_vm or [])
                       if not getattr(mr, 'cross_category_path', None)]
                if _vm:
                    result = builder.build_multi_facet(parsed, _vm)
                    result.reason = f"[own-subcat compound:{_variant!r}] " + result.reason
                    break
            else:
                _vmp = matcher.match_with_partial(_variant, facet_values)
                if _vmp.is_match:
                    result = builder.build(parsed, _vmp)
                    result.reason = f"[own-subcat compound:{_variant!r}] " + result.reason
                    break

    # 2. PARENT SUBCATEGORY FACETS (only if we have a subcategory)
    if not result and parsed.subcategory_id:
        parent_facets_df = facet_filter.filter_by_parent_subcategory(parsed.subcategory_name)
        if not parent_facets_df.empty:
            parent_facets = facet_filter.get_facet_values(parent_facets_df)
            if parent_facets:
                if multi_facet or ' ' in parsed.keyword:
                    match_results = matcher.match_multi_word(
                        parsed.keyword, parent_facets,
                        all_type_facets=None, require_type_for_merk=True,
                        current_main_category=parsed.main_category
                    )
                    if match_results:
                        result = builder.build_multi_facet(parsed, match_results)
                        result.reason = f"[parent_subcat] " + result.reason
                else:
                    parent_match = matcher.match_with_partial(parsed.keyword, parent_facets)
                    if parent_match.is_match:
                        result = builder.build(parsed, parent_match)
                        result.reason = f"[parent_subcat] " + result.reason

    # 2c. V28: Compound-decomposition retry — runs BEFORE every subcategory-
    # name fallback. Dutch retail keywords often glue a specifier onto a
    # base noun ("huistelefoon" = "huis"+"telefoon"), but indexed facet
    # values carry only the base ("Senioren telefoon"). If we don't try
    # the decomposed variant before V14 / sub-subcat name matching, the
    # partial match "huistelefoon" → subcat "Huistelefoons" wins at score
    # ~99 and we miss the much better facet match.
    if not result and parsed.keyword:
        from src.synonyms import expand_compounds
        variants = expand_compounds(parsed.keyword)
        for variant in variants[1:]:   # skip the original (already tried)
            v_match = None
            v_facets = facet_values
            # V31: try multi-word against subcat facets FIRST when the
            # variant has multiple tokens — match_with_partial treats the
            # variant as one phrase and misses cases like
            # 'combi wasmachine droger' → 'Wasmachine en droger kasten'
            # (token coverage 2/2 after stopword filter = score ~90).
            if v_facets and ' ' in variant:
                multi_in_sub = matcher.match_multi_word(
                    variant, v_facets,
                    all_type_facets=all_type_facets,
                    require_type_for_merk=True,
                    current_main_category=parsed.main_category,
                )
                # Strip cross-maincat hits — V28 retry targets within-subcat decomposition.
                multi_in_sub = [mr for mr in (multi_in_sub or [])
                                if not getattr(mr, 'cross_category_path', None)]
                if multi_in_sub:
                    result = builder.build_multi_facet(parsed, multi_in_sub)
                    result.reason = f"[V28 compound:{variant!r}][subcat-multi] " + result.reason
                    break
            if v_facets:
                v_match = matcher.match_with_partial(variant, v_facets)
                if v_match.is_match:
                    result = builder.build(parsed, v_match)
                    result.reason = f"[V28 compound:{variant!r}] " + result.reason
                    break
            # Try maincat-level facets as well
            mc_facets_df = facet_filter.filter_by_main_category(parsed.main_category)
            if not mc_facets_df.empty:
                mc_facets = facet_filter.get_facet_values(mc_facets_df)
                if multi_facet or ' ' in variant:
                    mc_results = matcher.match_multi_word(
                        variant, mc_facets, all_type_facets=all_type_facets,
                        require_type_for_merk=True,
                        current_main_category=parsed.main_category,
                    )
                    # V31: drop cross-maincat hits — keep going so a same-maincat
                    # variant later in the loop still has a chance.
                    mc_results = [mr for mr in (mc_results or [])
                                  if not getattr(mr, 'cross_category_path', None)]
                    if mc_results:
                        result = builder.build_multi_facet(parsed, mc_results)
                        result.reason = f"[V28 compound:{variant!r}][maincat] " + result.reason
                        break
                else:
                    mc_match = matcher.match_with_partial(variant, mc_facets)
                    if (mc_match.is_match
                            and not getattr(mc_match, 'cross_category_path', None)):
                        result = builder.build(parsed, mc_match)
                        result.reason = f"[V28 compound:{variant!r}][maincat] " + result.reason
                        break

    # 2b. V29: SUB-SUBCATEGORIE NAAM MATCHING (≥95) — when the URL pins a
    #     subcategory, first look for a child subcategory whose display name
    #     matches the keyword. Fixes cases like
    #       /main_sanitair_559434/r/wandpaneel/  ->  .../559434_560019 (Douchepanelen)
    HIGH_SUBCAT_THRESHOLD = 95
    if not result and parsed.subcategory_name:
        categories_df = d.get('categories_df')
        if categories_df is not None:
            child_match = matcher.match_subcategory_name(
                parsed.keyword, categories_df, main_category=parsed.subcategory_name
            )
            matched_word = parsed.keyword  # default: full-keyword match path
            if not child_match or child_match.get('score', 0) < HIGH_SUBCAT_THRESHOLD:
                for kw in parsed.keyword.lower().split():
                    if len(kw) < 4:
                        continue
                    wm = matcher.match_subcategory_name(
                        kw, categories_df, main_category=parsed.subcategory_name
                    )
                    if wm and wm.get('score', 0) >= HIGH_SUBCAT_THRESHOLD:
                        child_match = wm
                        matched_word = kw
                        break
            # Specificity rescue: prefer a deeper same-maincat sibling whose
            # facets absorb a leftover keyword token.
            if child_match and child_match.get('score', 0) >= HIGH_SUBCAT_THRESHOLD:
                child_match = _maybe_promote_to_specific_subcat(
                    child_match, matched_word, parsed, categories_df,
                    facet_filter, matcher,
                )
            if child_match and child_match.get('score', 0) >= HIGH_SUBCAT_THRESHOLD:
                result = builder.build_subcategory_redirect(
                    original_url=url,
                    keyword=parsed.keyword,
                    subcategory_match=child_match,
                    main_category=parsed.main_category,
                    existing_facet=parsed.existing_facet,
                )
                result.reason = f"[child_subcat] " + result.reason
                result = _append_facet_to_subcat_redirect(
                    result, parsed, child_match, facet_filter, matcher
                )

    # 3. V14.1: SUBCATEGORIE NAAM MATCHING met HOGE SCORE (≥95) binnen maincat
    # Voor generieke termen: "scharnieren" -> subcategorie "Deurscharnieren"
    # Dit voorkomt dat een specifieke facet ("Onzichtbare scharnieren") wordt gekozen
    # V28: Per-woord matching - probeer eerst full keyword, dan individuele woorden
    HIGH_SUBCAT_THRESHOLD = 95
    if not result:
        categories_df = d.get('categories_df')
        if categories_df is not None:
            # Probeer eerst het volledige keyword
            subcat_match = matcher.match_subcategory_name(
                parsed.keyword,
                categories_df,
                main_category=parsed.main_category
            )
            matched_word = parsed.keyword  # default: full-keyword match path
            # V28: Als full keyword niet matcht, probeer individuele woorden
            if not subcat_match or subcat_match.get('score', 0) < HIGH_SUBCAT_THRESHOLD:
                keywords_to_try = parsed.keyword.lower().split()
                for kw in keywords_to_try:
                    if len(kw) < 4:
                        continue
                    word_match = matcher.match_subcategory_name(
                        kw, categories_df, main_category=parsed.main_category
                    )
                    if word_match and word_match.get('score', 0) >= HIGH_SUBCAT_THRESHOLD:
                        subcat_match = word_match
                        matched_word = kw
                        break
            # Specificity rescue: if a deeper same-maincat sibling can absorb
            # a leftover keyword token via its facets, prefer it.
            if subcat_match and subcat_match.get('score', 0) >= HIGH_SUBCAT_THRESHOLD:
                subcat_match = _maybe_promote_to_specific_subcat(
                    subcat_match, matched_word, parsed, categories_df,
                    facet_filter, matcher,
                )
            # Alleen accepteren als score ≥ 95 (bijna exacte match)
            if subcat_match and subcat_match.get('score', 0) >= HIGH_SUBCAT_THRESHOLD:
                result = builder.build_subcategory_redirect(
                    original_url=url,
                    keyword=parsed.keyword,
                    subcategory_match=subcat_match,
                    main_category=parsed.main_category,
                    existing_facet=parsed.existing_facet  # V19: preserve existing facet
                )
                result.reason = f"[subcat_name_high] " + result.reason
                result = _append_facet_to_subcat_redirect(
                    result, parsed, subcat_match, facet_filter, matcher
                )

    # 4. MAIN CATEGORY FACETS - Zoek in alle facets binnen maincat
    # Voor specifiekere termen: "onzichtbare scharnieren" -> facet "Onzichtbare scharnieren"
    if not result:
        from src.validation_rules import GENERIC_ADJECTIVES, GENERIC_NOUNS

        def _maincat_match_is_generic_only(match_results_):
            """V31: True iff every kept facet matches a keyword TOKEN that is
            generic. Without this guard, /r/tv-meubel_set/ matches 'set'
            (a generic noun) to facet 'Set' in an unrelated subcat
            (servies/tableware) and we accept the cross-subcat jump. The
            in-subcat case is still fine because pass 1 handles those."""
            if not match_results_:
                return False
            for mr in match_results_:
                tok = (getattr(mr, 'keyword', '') or '').lower().strip()
                if not tok:
                    continue
                if (tok in GENERIC_ADJECTIVES or tok in GENERIC_NOUNS
                        or tok in STOPWORDS or tok in SHOP_NAMES):
                    continue
                return False
            return True

        maincat_facets_df = facet_filter.filter_by_main_category(parsed.main_category)
        if not maincat_facets_df.empty:
            maincat_facets = facet_filter.get_facet_values(maincat_facets_df)
            if maincat_facets:
                if multi_facet or ' ' in parsed.keyword:
                    match_results = matcher.match_multi_word(
                        parsed.keyword, maincat_facets,
                        all_type_facets=None, require_type_for_merk=True,
                        current_main_category=parsed.main_category
                    )
                    if match_results and not _maincat_match_is_generic_only(match_results):
                        result = builder.build_multi_facet(parsed, match_results)
                        result.reason = f"[maincat] " + result.reason
                else:
                    maincat_match = matcher.match_with_partial(parsed.keyword, maincat_facets)
                    if maincat_match.is_match:
                        kw_tok = (maincat_match.keyword or '').lower().strip()
                        if not (kw_tok in GENERIC_ADJECTIVES or kw_tok in GENERIC_NOUNS
                                or kw_tok in STOPWORDS or kw_tok in SHOP_NAMES):
                            result = builder.build(parsed, maincat_match)
                            result.reason = f"[maincat] " + result.reason

    # 5. V14: SUBCATEGORIE NAAM MATCHING (lagere scores) binnen same main_category
    # Fallback voor wanneer geen facet match maar wel subcategorie naam match
    # V28: Per-woord matching - probeer eerst full keyword, dan individuele woorden
    if not result:
        categories_df = d.get('categories_df')
        if categories_df is not None:
            subcat_match = matcher.match_subcategory_name(
                parsed.keyword,
                categories_df,
                main_category=parsed.main_category
            )
            # V28: Als full keyword niet matcht, probeer individuele woorden
            if not subcat_match:
                keywords_to_try = parsed.keyword.lower().split()
                for kw in keywords_to_try:
                    if len(kw) < 4:
                        continue
                    word_match = matcher.match_subcategory_name(
                        kw, categories_df, main_category=parsed.main_category
                    )
                    if word_match and word_match.get('score', 0) >= SUBCATEGORY_MATCH_THRESHOLD:
                        subcat_match = word_match
                        break
            if subcat_match:
                result = builder.build_subcategory_redirect(
                    original_url=url,
                    keyword=parsed.keyword,
                    subcategory_match=subcat_match,
                    main_category=parsed.main_category,
                    existing_facet=parsed.existing_facet  # V19: preserve existing facet
                )
                result = _append_facet_to_subcat_redirect(
                    result, parsed, subcat_match, facet_filter, matcher
                )

    # 6. V14: CROSS-CATEGORY SUBCATEGORIE NAAM MATCHING
    # Alleen als geen match binnen maincat - zoek in alle categorieën
    # V30: full-keyword path vereist score=100 (exacte match) om foute jumps te voorkomen.
    # V31: per-word fallback re-enabled, maar STRIKT begrensd: alleen
    # tokens die niet generic zijn (GENERIC_ADJECTIVES/NOUNS/STOPWORDS/SHOPS)
    # en >= 6 chars lang, met score >= 95. Pakt /r/tv-meubel_set/ → TV-meubels
    # (token 'tv-meubel' scoort 99 cross-maincat) zonder de V30 false-positives
    # terug te brengen — 'meubel' alleen wordt nu gefilterd door GENERIC_NOUNS.
    if not result:
        categories_df = d.get('categories_df')
        if categories_df is not None:
            from src.validation_rules import GENERIC_ADJECTIVES, GENERIC_NOUNS
            subcat_match = matcher.match_subcategory_name(
                parsed.keyword,
                categories_df,
                main_category=None  # Search across ALL categories
            )
            # V30: full keyword vereist score=100
            if subcat_match and subcat_match.get('score', 0) == 100:
                pass  # accept as-is
            else:
                # V31: try non-generic individual tokens
                CROSS_CAT_TOKEN_MIN_LEN = 6
                CROSS_CAT_TOKEN_MIN_SCORE = 95
                best_per_word = None
                # Sort tokens longest-first — longer tokens are more
                # discriminating, so we accept the first valid hit.
                tokens = sorted(
                    (t for t in parsed.keyword.lower().split()
                     if len(t) >= CROSS_CAT_TOKEN_MIN_LEN
                     and t not in STOPWORDS
                     and t not in SHOP_NAMES
                     and t not in GENERIC_ADJECTIVES
                     and t not in GENERIC_NOUNS),
                    key=len, reverse=True,
                )
                for tok in tokens:
                    cand = matcher.match_subcategory_name(
                        tok, categories_df, main_category=None,
                    )
                    if cand and cand.get('score', 0) >= CROSS_CAT_TOKEN_MIN_SCORE:
                        best_per_word = cand
                        best_per_word['_matched_token'] = tok
                        break
                if best_per_word:
                    subcat_match = best_per_word

            if subcat_match and (
                subcat_match.get('score', 0) == 100
                or subcat_match.get('_matched_token')
            ):
                result = builder.build_subcategory_redirect(
                    original_url=url,
                    keyword=parsed.keyword,
                    subcategory_match=subcat_match,
                    main_category=parsed.main_category,
                    existing_facet=parsed.existing_facet  # V19: preserve existing facet
                )

    # CONVERGENCE RESCUE: when the anchored cascade above produced NO facets
    # (None, a bare-category redirect, or a facet-less subcat/collapse), try
    # the global-pass-style subtree delegation — hyphen-split the keyword, drop
    # the bare category noun, discover the best type facet within THIS
    # category's subtree to find the right child subcat, then run a full
    # multi-facet match there. This is what lets
    # /products/mode/mode_432360/r/nike-nederlands-elftal-trainingsshirt/
    # resolve to mode_432360_469350 (Sportshirts) with
    # fanshop~Nederlands Elftal ~~ merk~Nike ~~ type_sportshirts.
    #
    # Gated to FACET-LESS outcomes only (rescue, not pre-empt): a confident
    # anchored multi-facet result from the cascade is never overridden — that
    # pre-empting was what regressed cases like 'alcatel_senioren_mobiel'
    # (Mobiele telefoons → wrongly Huistelefoons) and dropped facets like
    # 'illy_koffiebonen_1kg' (lost '1 kg'). Bounded to the anchor subtree so it
    # can't jump to an unrelated maincat.
    # Trigger width (<=2) only controls HOW OFTEN the rescue runs, never
    # correctness: the adoption rule below is monotonic-safe (it only ever adds
    # facets within the SAME destination subcat, or fills a 0-facet baseline).
    # <=2 lets a thin 2-facet cascade (e.g. samsung TV -> merk + 4K Ultra HD)
    # be enriched with the dimension facet (55 inch) it missed, without paying
    # the rescue cost on already-rich (3+ facet) results.
    # (B) The rescue also runs for top-level /products/<maincat>/r/<kw>/ URLs
    # that pin only a main category (no subcategory_id). Section 4's maincat
    # match collapses a multi-axis query (fanshop + type) to the single
    # best-covered facet value and emits that value's standalone subcat, so the
    # type axis is lost. Anchored to the main category, discovery can re-find
    # the type facet, pin the right subcat, and recombine both facets.
    _cascade_fc = getattr(result, 'facet_count', 0) if result else 0
    if (parsed.subcategory_id or parsed.main_category) and (not result or _cascade_fc <= 2):
        _sub_nm = category_lookup.get(parsed.subcategory_id, '') if parsed.subcategory_id else ''
        _resc = _derive_facets_in_subtree(
            parsed, parsed.subcategory_id or None, _sub_nm,
            facet_filter, matcher, all_type_facets, builder,
            categories_df=d.get('categories_df'),
        )
        if _resc and getattr(_resc, 'facet_count', 0) >= 1:
            # Adoption rules, ordered safest-first:
            #  (a) baseline had NO facets (bare category / collapse / block) →
            #      adopt any faceted rescue.
            #  (b) baseline was THIN (1 facet) → adopt ONLY as pure enrichment:
            #      same destination subcategory, strictly more facets. This
            #      turns the target's cascade result (Sportshirts +
            #      type_sportshirts only) into the full Sportshirts + fanshop +
            #      merk + type, WITHOUT ever flipping a different-category thin
            #      match (e.g. alcatel_senioren_mobiel → Mobiele telefoons stays
            #      put, since the rescue's Huistelefoons is a different subcat).
            _resc_fc = getattr(_resc, 'facet_count', 0)
            _adopt = False
            if not result or _cascade_fc == 0:
                _adopt = True
            elif parsed.subcategory_id:
                # Anchored baseline: enrich ONLY within the same destination
                # subcat, so a different-subcat thin match stays put
                # (alcatel_senioren_mobiel → Mobiele telefoons isn't flipped).
                if (extract_subcategory_id_from_url(_resc.redirect_url)
                        == extract_subcategory_id_from_url(result.redirect_url)
                        and _resc_fc > _cascade_fc):
                    _adopt = True
            else:
                # (B) maincat-only baseline: the URL pinned no subcat, so there
                # is no anchor to respect. Adopt when the rescue strictly
                # enriches the cascade — every cascade facet preserved plus a
                # type facet that pins a more specific subcat (fanshop ->
                # fanshop + type_sportshirts). The superset test refuses a
                # rescue that drops or swaps the cascade's facet.
                if (_resc_fc > _cascade_fc
                        and _facet_fragment_superset(
                            getattr(_resc, 'facet_fragment', ''),
                            getattr(result, 'facet_fragment', ''))):
                    _adopt = True
            if _adopt:
                _resc.reason = "[subtree-rescue] " + (_resc.reason or '')
                result = _resc

    # Restore the deferred purely-cross-category step-1 match as a last resort:
    # nothing in the own subtree / same maincat matched, so the cross-maincat
    # type hit is the only candidate left (better than a bare category page).
    if not result and _deferred_cross_result is not None:
        result = _deferred_cross_result

    if not result:
        result = builder.build_category_only(parsed)

    # Build output
    r = result
    # V14.1 fix: Use parsed.subcategory_id for original category (from input URL)
    # not r.subcategory_id (which may be from the redirect result)
    original_cat_name = category_lookup.get(str(parsed.subcategory_id), parsed.subcategory_id) if parsed.subcategory_id else ''
    redirect_subcat_id = extract_subcategory_id_from_url(r.redirect_url)
    redirect_cat_name = category_lookup.get(str(redirect_subcat_id), redirect_subcat_id) if redirect_subcat_id else ''

    is_cross_category = (
        original_cat_name and redirect_cat_name and
        original_cat_name != redirect_cat_name
    )

    # V12: Calculate keyword coverage FIRST (V21: needed for reliability score)
    # V30: Include numeric tokens (len=1 digits like "6", "8") so "vijverfolie 6 x 8" doesn't get 100% coverage
    keyword_words = [w.lower() for w in r.keyword.split()
                     if len(w) >= 2 or re.match(r'^\d+$', w)] if r.keyword else []

    # Find stopwords in original keyword
    stopwords_in_keyword = [w for w in keyword_words if w in STOPWORDS]
    has_stopwords = len(stopwords_in_keyword) > 0
    stopwords_found = ', '.join(stopwords_in_keyword) if stopwords_in_keyword else ''

    # V23: Find shop names in keyword (these are tracked separately, not matched to facets)
    shops_in_keyword = [w for w in keyword_words if w in SHOP_NAMES]
    shop_in_keyword = ', '.join(shops_in_keyword) if shops_in_keyword else ''

    # V23.2: Check if keyword contains dimensions
    DIMENSION_PATTERN = re.compile(
        r'\d+\s*x\s*\d+|\d+\s*cm\b|\d+\s*mm\b|\d+\s*meter\b|\d+\s*m\b|\d+\s*persoons\b|\d+\s*liter\b',
        re.IGNORECASE
    )
    has_dims = bool(DIMENSION_PATTERN.search(r.keyword)) if r.keyword else False

    # Determine matched keywords from facet_value_names or redirect_category (for subcategory_name matches)
    matched_keywords = []
    unmatched_keywords = []

    # V14.1: Voor subcategorie naam matches, gebruik redirect_category voor coverage berekening
    match_target = r.facet_value_names if r.facet_value_names else redirect_cat_name

    if match_target and keyword_words:
        # Get matched values (lowercased for comparison)
        facet_values_lower = [fv.lower() for fv in match_target.split(', ')] if match_target else []

        # V29: trust the matcher when it used a semantic path. For synonym
        # phrase rewrites and the token-coverage scorer, the matcher has
        # already validated the match — V27's literal-substring check on
        # unmatched_keywords is the wrong question to ask. Without this,
        # e.g. "senioren telefoon" → "Senioren mobiel" (via synonym) gets
        # 'telefoon' marked unmatched, V27 long-unmatched fires, score
        # drops to 0, V28 rescue overrides the good match with bare subcat.
        # V32: subcategory_name removed from the trusted set. We now always
        # look for unmatched query parts in subcategory-name matches and treat
        # them the same as facet matches (coverage penalty + V27 long-unmatched
        # hard-reject in the scorer). 'synonym'/'token_coverage' stay trusted
        # because their matchers already validate intent semantically.
        TRUSTED_MATCH_TYPES = {
            'synonym', 'token_coverage',
        }
        if r.match_type in TRUSTED_MATCH_TYPES:
            for word in keyword_words:
                if word in STOPWORDS or word in SHOP_NAMES:
                    continue
                matched_keywords.append(word)
        else:
            # Default path: literal-substring check against facet_value_names.
            # A keyword token also counts as matched if its compound base
            # form (per COMPOUND_DECOMPOSITIONS) is represented — without this,
            # "huistelefoon" → "Senioren telefoon" via the V28 compound retry
            # would have its original token "huistelefoon" flagged unmatched
            # even though its base form "telefoon" is in the facet.
            from src.synonyms import COMPOUND_DECOMPOSITIONS as _CDEC
            for word in keyword_words:
                word_matched = False
                if word in STOPWORDS or word in SHOP_NAMES:
                    continue

                forms = [word]
                base = _CDEC.get(word)
                if base:
                    forms.append(base)

                for form in forms:
                    for fv in facet_values_lower:
                        if (form in fv or
                            fv in form or
                            form.rstrip('e').rstrip('s') in fv or
                            fv.rstrip('e').rstrip('s') in form):
                            word_matched = True
                            break
                    if word_matched:
                        break

                if word_matched:
                    matched_keywords.append(word)
                else:
                    unmatched_keywords.append(word)

    # Calculate coverage (excluding stopwords AND shop names from denominator)
    non_stopword_keywords = [w for w in keyword_words if w not in STOPWORDS and w not in SHOP_NAMES]
    match_coverage = 0.0
    if non_stopword_keywords:
        match_coverage = round(100 * len(matched_keywords) / len(non_stopword_keywords), 1)

    matched_keywords_str = ', '.join(matched_keywords) if matched_keywords else ''
    unmatched_keywords_str = ', '.join(unmatched_keywords) if unmatched_keywords else ''

    # V23.1: Determine keyword type
    has_shops = len(shops_in_keyword) > 0
    has_stops = len(stopwords_in_keyword) > 0
    has_matchable = len(non_stopword_keywords) > 0  # non_stopword_keywords excludes shops AND stopwords

    if has_matchable:
        keyword_type = 'product'
    elif has_shops and has_stops:
        keyword_type = 'shop_and_stopwords'
    elif has_shops:
        keyword_type = 'shop_only'
    elif has_stops:
        keyword_type = 'stopwords_only'
    else:
        keyword_type = 'no_matchable'

    # V26: Synthetic H1 similarity — built from URL components, no crawling.
    h1_similarity = compute_h1_similarity(
        keyword=r.keyword,
        original_cat_name=original_cat_name,
        redirect_cat_name=redirect_cat_name,
        facet_value_names=r.facet_value_names,
    ) if r.success else 0

    # V21: Calculate reliability score WITH match_coverage
    reliability_score = 0
    reliability_tier = 'D'
    if r.success:
        reliability_score = calculate_reliability_score(
            match_score=r.match_score,
            facet_count=r.facet_count,
            match_type=r.match_type,
            is_cross_category=is_cross_category,
            facet_value_names=r.facet_value_names,
            keyword=r.keyword,
            reason=r.reason,
            match_coverage=match_coverage,  # V21: pass coverage to reliability scorer
            h1_similarity=h1_similarity,    # V26: H1 similarity as trust signal
            matched_keywords=matched_keywords,    # V27: generic-adjective + long-unmatched floors
            unmatched_keywords=unmatched_keywords,
        )
        reliability_tier = get_reliability_tier(reliability_score)
    # V27: Only surface the rejection reason when the scorer actually
    # acted on it (score dropped to 0). V32: the subcategory_name branch now
    # also runs V27, so this correctly surfaces long-unmatched rejections for
    # subcategory matches too; the gate still suppresses the reason on rows
    # whose score was never reduced.
    reject_reason = (
        _v27_reject_reason(matched_keywords, unmatched_keywords, match_type=r.match_type) or ''
        if reliability_score == 0 else ''
    )

    # V28: cache-only search-derived rescue + disagree-warning.
    # The prefetch step ran sequentially before this pool spawned and
    # populated the SQLite cache; here we only read. Skipped when the row
    # has no matchable token (already redirected by other branches).
    final_redirect_url = r.redirect_url
    final_redirect_cat_name = redirect_cat_name
    final_match_type = r.match_type
    final_reason = r.reason
    final_score = reliability_score
    final_tier = reliability_tier
    flag_for_review = ''
    search_derived_total = None
    search_derived_dom_cat = ''
    search_derived_dom_share = None

    if has_matchable and parsed.main_category and parsed.keyword:
        derived = derive_search_redirect(parsed.main_category, parsed.keyword)
        search_derived_total = derived.get('total')
        search_derived_dom_cat = derived.get('dom_cat_name', '') or ''
        search_derived_dom_share = derived.get('dom_cat_share')

        # V31 guard: when the matcher already produced a clean facet match in
        # the URL's own subcategory, do NOT let search-derived override it
        # with a different subcategory's guess. V27 routinely zeros the
        # reliability score whenever any long unmatched token is present
        # (e.g. "verrijdbare" in "zweefparasol met verrijdbare voet"), which
        # then triggers the rescue path below — but the matcher's anchored
        # multi-facet result is more trustworthy than search-derived's
        # different-subcat guess. Restore a tier-C score so the row isn't
        # rescued out from under the user.
        _skip_rescue_override = False
        if (
            r.success
            and getattr(r, 'facet_count', 0) >= 1
            and r.subcategory_id
            and r.subcategory_id == parsed.subcategory_id
        ):
            _derived_subcat_id = ''
            for _part in reversed((derived.get('dom_cat_url_slug') or '').split('_')):
                if _part.isdigit():
                    _derived_subcat_id = _part
                    break
            if _derived_subcat_id and _derived_subcat_id != parsed.subcategory_id:
                _skip_rescue_override = True
                if reliability_score < 50:
                    final_score = 60
                    final_tier = get_reliability_tier(final_score)
                    final_reason = (r.reason or '') + (
                        ' [V31: kept matcher result; search-derived suggested '
                        f"different subcat '{derived.get('dom_cat_name','')}']"
                    )

        if reliability_score < 50 and derived.get('redirect_url') and not _skip_rescue_override:
            # Preserve any /c/<facet> the original URL carried — search-derived
            # rescue should never silently strip an existing facet selection.
            base_redirect = derived['redirect_url'].rstrip('/')
            if parsed.existing_facet:
                final_redirect_url = f"{base_redirect}/c/{parsed.existing_facet}"
            else:
                final_redirect_url = derived['redirect_url']

            # V29: try the matcher against the rescue subcat's facets first
            # — if the keyword (or its synonym/compound expansion) matches a
            # facet inside dom_cat, prefer that over a bare deepest_cat
            # redirect. This catches the case where the original URL's
            # subcat had no relevant facet but the dom_cat does, e.g.
            # /elektronica/_19943088/r/senioren_telefoon/ → dom_cat is
            # _19934132 (Mobiele telefoons), which has the Senioren mobiel
            # facet via the senioren_telefoon ↔ senioren_mobiel synonym.
            dom_slug = derived.get('dom_cat_url_slug')
            local_match = None
            appended_value_name = ''  # facet value name appended below, if any
            _used_multi = False       # V33: multi-facet rescue overrode the reject
            if dom_slug:
                # Extract subcat_id from slug (last numeric segment).
                dom_subcat_id = ''
                for part in reversed(dom_slug.split('_')):
                    if part.isdigit():
                        dom_subcat_id = part
                        break
                if dom_subcat_id:
                    dom_facets_df = facet_filter.filter_by_subcategory(dom_subcat_id)
                    if not dom_facets_df.empty:
                        dom_facets = facet_filter.get_facet_values(dom_facets_df)
                        dom_results = matcher.match_multi_word(
                            parsed.keyword, dom_facets,
                            current_main_category=parsed.main_category,
                        )
                        if dom_results:
                            local_match = dom_results[0]
            if local_match and local_match.is_match:
                pf_name = local_match.facet_value.facet_name
                pf_vid = local_match.facet_value.facet_value_id
                pf_value_name = local_match.matched_text or ''
                fragment = f"{pf_name}~{pf_vid}"
                existing_names = {p.split('~', 1)[0]
                                  for p in (parsed.existing_facet or '').split('~~')
                                  if '~' in p}
                if pf_name not in existing_names:
                    if parsed.existing_facet:
                        final_redirect_url = (
                            f"{base_redirect}/c/{parsed.existing_facet}~~{fragment}"
                        )
                    else:
                        final_redirect_url = f"{base_redirect}/c/{fragment}"
                    final_match_type = 'search_derived_subcat_with_facet'
                    appended_value_name = pf_value_name
                    final_reason_extra = (
                        f"; appended {pf_name}~{pf_vid} ({pf_value_name!r}, "
                        f"matcher score {local_match.score})"
                    )
                else:
                    final_match_type = 'search_derived_subcat'
                    final_reason_extra = ''
                local_match_used = True
            else:
                local_match_used = False
                probe = derive_search_facet(parsed.main_category, parsed.keyword)

            # V29: facet-probe extension — append a dominant facet on top of
            # the deepest_cat redirect when one is cached. Stage 1 (free,
            # from base response) and Stage 2 (filter probes) both feed
            # this same cache. Combines naturally with existing_facet via ~~.
            # Skipped when the local matcher already produced a match above.
            if local_match_used:
                pass  # match_type / reason already set
            elif probe and probe.get('mode') in ('match', 'match_from_response'):
                pf_name = probe.get('facet_name')
                pf_vid = probe.get('value_id')
                pf_value_name = probe.get('value_name', '')
                pf_cov = probe.get('coverage', 0)
                fragment = f"{pf_name}~{pf_vid}"
                # Avoid duplicating if existing_facet already used same facet name.
                existing_names = {p.split('~', 1)[0]
                                  for p in (parsed.existing_facet or '').split('~~')
                                  if '~' in p}
                if pf_name not in existing_names:
                    if parsed.existing_facet:
                        final_redirect_url = (
                            f"{base_redirect}/c/{parsed.existing_facet}~~{fragment}"
                        )
                    else:
                        final_redirect_url = f"{base_redirect}/c/{fragment}"
                    final_match_type = 'search_derived_subcat_with_facet'
                    appended_value_name = pf_value_name
                    final_reason_extra = (
                        f"; appended {pf_name}~{pf_vid} ({pf_value_name!r}, "
                        f"coverage {int(100*pf_cov)}%)"
                    )
                else:
                    final_match_type = 'search_derived_subcat'
                    final_reason_extra = ''
            else:
                final_match_type = 'search_derived_subcat'
                final_reason_extra = ''

            # Hard-reject (user decision 2026-05-27): the search-derived guess
            # is only trustworthy if it didn't silently drop a long product-
            # type token from the query. If a >=8-char non-stopword token is
            # absent from both the rescued dom_cat name and the appended facet
            # value, the redirect points at the wrong product (Q4
            # 'bewegingssensor', Q7 'waterfilter', Q9 'inductiekookplaat') —
            # emit no redirect instead.
            # A curated synonym match semantically bridges its source tokens to
            # the facet value even when there's zero lexical overlap (e.g.
            # "afdekplaat inductiekookplaat" → "Inductie beschermer", "hoesloze"
            # → "Zonder overtrek"). Feed the synonym's source phrase into the
            # long-unmatched check so it doesn't reject a correct synonym match
            # just because the descriptive token isn't literally in the facet
            # name. Mirrors the TRUSTED_MATCH_TYPES handling in the coverage calc.
            _synonym_src = ''
            if local_match_used and getattr(local_match, 'match_type', '') == 'synonym':
                _synonym_src = getattr(local_match, 'keyword', '') or ''
            _reject_tok = _rescue_long_unmatched_token(
                parsed.keyword,
                ' '.join(filter(None, [derived.get('dom_cat_name', ''),
                                       appended_value_name, _synonym_src])),
            )
            if _reject_tok:
                # V33: before giving up, try a multi-facet assembly. A single
                # appended facet can't represent a query spanning several axes
                # (fanshop + merk + product-type + colour); assembling one
                # keyword-consistent value per axis often DOES cover the long
                # token the single-facet path dropped. e.g.
                # /mode/r/Nike_nederlands_elftal_thuisshirt_oranje_maat_-_122-128/
                # → Sportshirts /c/ fanshop~Nederlands Elftal ~~ merk~Nike
                #   ~~ ut_voetbalshirt~Thuis ~~ kleur~Oranje. 'nederlands' is
                # then covered by fanshop and 'thuisshirt' by 'Thuis' (prefix).
                _probe_payload = (derive_search_facet(parsed.main_category,
                                                      parsed.keyword) or {})
                _multi = _probe_payload.get('multi_facets') or []
                # V34: size is opt-in — per-size pages churn in/out of stock,
                # so honour an explicit query size only when RESCUE_INCLUDE_SIZE.
                _size_facet = (_probe_payload.get('size_facet')
                               if RESCUE_INCLUDE_SIZE else None)
                _multi_frag, _multi_names = _assemble_multi_facet(
                    _multi, parsed.existing_facet, size_facet=_size_facet)
                _still_unmatched = None
                if _multi_frag and len(_multi) >= 2:
                    _still_unmatched = _rescue_long_unmatched_token(
                        parsed.keyword,
                        ' '.join(filter(None, [derived.get('dom_cat_name', '')] + _multi_names)),
                        prefix_link=True,
                    )
                if _multi_frag and len(_multi) >= 2 and _still_unmatched is None:
                    _used_multi = True
                    final_redirect_url = f"{base_redirect}/c/{_multi_frag}"
                    final_match_type = 'search_derived_subcat_multi_facet'
                    final_reason_extra = (
                        "; multi-facet: "
                        + ", ".join(f"{m['facet_name']}~{m['value_id']}"
                                    f"({m['value_name']!r})" for m in _multi)
                    )
                    # Falls through to the shared finals block below, which is
                    # gated on `not _used_multi` only for the cat-name/score
                    # lines; those are set here instead.
                    final_redirect_cat_name = derived['dom_cat_name']
                    final_score = 70
                    final_tier = get_reliability_tier(final_score)
                    final_reason = (
                        f"[V33] Search-derived multi-facet rescue: "
                        f"'{derived['dom_cat_name']}'" + final_reason_extra
                        + (f" (rescued long token '{_reject_tok}')")
                    )
                    reject_reason = ''
            if _reject_tok and not _used_multi:
                final_redirect_url = None
                final_redirect_cat_name = ''
                final_match_type = 'rejected_long_unmatched'
                final_score = 0
                final_tier = 'D'
                reject_reason = (
                    f"V28-rescue rejected: long unmatched product token "
                    f"'{_reject_tok}' not represented in dom_cat "
                    f"'{derived.get('dom_cat_name','')}'"
                    + (f" or facet '{appended_value_name}'" if appended_value_name else "")
                )
                final_reason = reject_reason
                flag_for_review = ''
                # Fall through to the return; the V31 leftover block (score>=50)
                # and the maincat validator (needs a redirect URL) both no-op.
                return {
                    'original_url': r.original_url,
                    'main_category': r.main_category,
                    'original_category': original_cat_name,
                    'keyword': r.keyword,
                    'redirect_url': None,
                    'redirect_category': '',
                    'is_cross_category': is_cross_category,
                    'facet_fragment': '',
                    'facet_names': '',
                    'facet_value_names': '',
                    'facet_count': 0,
                    'match_score': r.match_score,
                    'match_type': final_match_type,
                    'reliability_score': 0,
                    'reliability_tier': 'D',
                    'h1_similarity': 0,
                    'reject_reason': reject_reason,
                    'flag_for_review': '',
                    'search_derived_total': search_derived_total,
                    'search_derived_dom_cat': search_derived_dom_cat,
                    'search_derived_dom_share': search_derived_dom_share,
                    'matched_keywords': matched_keywords_str,
                    'unmatched_keywords': unmatched_keywords_str,
                    'match_coverage': match_coverage,
                    'has_stopwords': has_stopwords,
                    'stopwords_found': stopwords_found,
                    'shop_in_keyword': shop_in_keyword,
                    'keyword_type': keyword_type,
                    'has_dimensions': has_dims,
                    'merk_of_shop_missing': getattr(r, 'merk_of_shop_missing', ''),
                    'success': False,
                    'reason': final_reason,
                }

            if not _used_multi:
                final_redirect_cat_name = derived['dom_cat_name']
                final_score = 75
                final_tier = get_reliability_tier(final_score)
                final_reason = (
                    f"[V28] Search-derived: {derived.get('total')} products dominantly "
                    f"in '{derived['dom_cat_name']}' ({int(100*derived['dom_cat_share'])}%)"
                    + (f"; preserved original facet '{parsed.existing_facet}'"
                       if parsed.existing_facet else "")
                    + final_reason_extra
                )
                reject_reason = ''
        elif reliability_score >= 70 and derived.get('mode') in ('and', 'fallback') and not derived.get('redirect_url'):
            flag_for_review = (
                f"[V28] Legacy score {reliability_score}, but search "
                f"({derived.get('total')} products) shows no dominant deepest_cat"
            )

        # GUARD: don't let a single weak token drag a cross-category type match
        # out of the category the URL already pins, when the SEARCH SIGNAL
        # agrees the URL's own subcategory is where the query belongs.
        # Concrete case: /huis_tuin_505062_505149/r/dekbed_zonder_hoes/ — only
        # "hoes" matched, onto type_opberger "Opberghoes" in the unrelated
        # Opbergzakken subcat (10 products, tier D), while the Search API's
        # dominant category for "dekbed zonder hoes" IS the URL's own Dekbedden
        # subcat. We deliberately gate on search-derived dom_cat == origin
        # subcat (not a hardcoded generic-word list) because the offending
        # token here ("hoes") is not in GENERIC_NOUNS — the search agreement is
        # the trustworthy signal. Single-token + <50% coverage keeps this
        # narrow so genuine multi-token cross-category jumps are untouched.
        # Falls back to the origin category page (preserving any existing /c/
        # facet); the synonym fix above resolves THIS url to the right facet
        # before it ever reaches here, so this is the general safety net.
        if (
            final_redirect_url
            and r.match_type == 'cross_category_type'
            and is_cross_category
            and parsed.subcategory_id
            and len(matched_keywords) == 1
            and match_coverage < 50
        ):
            _dom_subcat_id = ''
            for _part in reversed((derived.get('dom_cat_url_slug') or '').split('_')):
                if _part.isdigit():
                    _dom_subcat_id = _part
                    break
            if _dom_subcat_id and _dom_subcat_id == str(parsed.subcategory_id):
                _origin_base = f"https://www.beslist.nl{parsed.full_category_path}"
                _ef = getattr(parsed, 'existing_facet', '') or ''
                final_redirect_url = (
                    f"{_origin_base}/c/{_ef}" if _ef else f"{_origin_base}/"
                )
                final_redirect_cat_name = original_cat_name
                final_match_type = 'cross_type_rejected_kept_origin'
                final_score = 70
                final_tier = get_reliability_tier(final_score)
                final_reason = (
                    f"[guard] rejected cross-category type jump to "
                    f"'{redirect_cat_name}' on single token "
                    f"'{matched_keywords[0]}'; Search API's dominant category is "
                    f"the URL's own '{original_cat_name}' — kept origin category"
                    + (f" + existing facet '{_ef}'" if _ef else " page")
                )
                reject_reason = ''
                flag_for_review = ''

        # V31: leftover-token facet append on high-score rows.
        # The rescue path above only runs when the matcher failed (score<50).
        # When the matcher succeeded but left some keyword tokens lexically
        # unrepresented in the chosen target (e.g. "hoesloze" in
        # "hoesloze_dekbedden" — no overlap with subcat "Dekbedden" or any
        # local facet, no synonym bridge to facet value "Zonder overtrek"),
        # the facet-probe coverage signal can still narrow the page.
        # We compute leftover tokens locally — `unmatched_keywords` above
        # is unreliable here because the matched_keywords logic marks every
        # token as matched whenever match_type is in TRUSTED_MATCH_TYPES
        # (including subcategory_name), even when the keyword token has
        # zero lexical or semantic representation in the target.
        local_leftover_tokens = []
        if has_matchable and reliability_score >= 50 and final_redirect_url:
            from src.validation_rules import GENERIC_ADJECTIVES
            target_text = ' '.join(filter(None, [
                redirect_cat_name or '',
                r.facet_value_names or '',
                final_redirect_url or '',
            ])).lower()
            for w in keyword_words:
                # V31: also skip GENERIC_ADJECTIVES — tokens like 'mini', 'klein',
                # 'rood' are size/color descriptors, not brand evidence. Without
                # this skip the leftover-merk path appends a brand whenever such
                # a token correlates with one brand by chance (e.g.
                # /r/mini_airco_voor_caravan/ → Evolar at 80% because Evolar
                # uses 'mini' in its Caravan-airco product titles).
                if w in STOPWORDS or w in SHOP_NAMES or w in GENERIC_ADJECTIVES:
                    continue
                # match if literal substring OR stem-stripped match
                stem = w.rstrip('e').rstrip('s')
                if w in target_text or (stem and stem in target_text):
                    continue
                local_leftover_tokens.append(w)

        if (
            reliability_score >= 50
            and final_redirect_url
            and local_leftover_tokens
            and derived.get('dom_cat_url_slug')
            and final_match_type not in (
                'search_derived_subcat',
                'search_derived_subcat_with_facet',
            )
        ):
            base_path = final_redirect_url.split('/c/', 1)[0].rstrip('/')
            matcher_subcat = base_path.rsplit('/', 1)[-1]
            if matcher_subcat == derived['dom_cat_url_slug']:
                probe = derive_search_facet(parsed.main_category, parsed.keyword)
                if probe and probe.get('mode') in ('match', 'match_from_response'):
                    pf_name = probe.get('facet_name')
                    pf_vid = probe.get('value_id')
                    pf_value_name = probe.get('value_name', '')
                    pf_cov = probe.get('coverage', 0)
                    fragment = f"{pf_name}~{pf_vid}"
                    existing_facet_part = ''
                    if '/c/' in final_redirect_url:
                        existing_facet_part = (
                            final_redirect_url.split('/c/', 1)[1].rstrip('/')
                        )
                    existing_names = {
                        p.split('~', 1)[0]
                        for p in (existing_facet_part or '').split('~~')
                        if '~' in p
                    }
                    if pf_name and pf_name not in existing_names:
                        if existing_facet_part:
                            final_redirect_url = (
                                f"{base_path}/c/{existing_facet_part}~~{fragment}"
                            )
                        else:
                            final_redirect_url = f"{base_path}/c/{fragment}"
                        final_match_type = f"{final_match_type}_with_probe_facet"
                        final_reason = (
                            (final_reason or '')
                            + f"; [V31] appended {pf_name}~{pf_vid} "
                            + f"({pf_value_name!r}, coverage {int(100*pf_cov)}%) "
                            + f"for leftover token(s): "
                            + ", ".join(local_leftover_tokens)
                        )

    # Maincat-path sanity check. A correct redirect path looks like
    # /products/{maincat}/{subcat}[/c/...] where {subcat} starts with
    # {maincat}_. Older code paths can lose the {maincat} segment for
    # hyphenated maincats (sport_outdoor_vrije-tijd, films-series, ...) and
    # produce /products/{subcat}/ which 404s on the live site. Try to repair
    # in-place by inserting the inferred maincat; if we can't, suppress the
    # redirect and flag the row for review.
    if final_redirect_url:
        try:
            from urllib.parse import urlparse, urlunparse
            _p = urlparse(final_redirect_url)
            if _p.path.startswith('/products/'):
                _segs = [s for s in _p.path.split('/') if s]
                # _segs = ['products', <maincat>, <subcat?>, 'c'?, <facet>?, ...]
                if len(_segs) >= 2:
                    _second = _segs[1]
                    _second_parts = _second.split('_')
                    # Malformed iff the segment right after 'products' looks
                    # like a subcat slug (has a numeric id token).
                    if any(p.isdigit() for p in _second_parts):
                        _maincat_parts = []
                        for _p2 in _second_parts:
                            if _p2.isdigit():
                                break
                            _maincat_parts.append(_p2)
                        if _maincat_parts and len(_maincat_parts) < len(_second_parts):
                            _inferred = '_'.join(_maincat_parts)
                            _repaired_path = '/products/' + _inferred + '/' + '/'.join(_segs[1:])
                            if not _p.path.endswith('/'):
                                pass
                            else:
                                _repaired_path += '/'
                            final_redirect_url = urlunparse(
                                _p._replace(path=_repaired_path)
                            )
                            final_reason = (
                                (final_reason or '')
                                + f"; repaired missing maincat segment '{_inferred}/'"
                            )
                        else:
                            flag_for_review = (
                                (flag_for_review + '; ' if flag_for_review else '')
                                + f"malformed redirect: no maincat could be inferred from '{_second}'"
                            )
                            final_redirect_url = None
                            final_match_type = 'malformed_redirect'
        except Exception as _e:
            flag_for_review = (
                (flag_for_review + '; ' if flag_for_review else '')
                + f"maincat validator error: {_e}"
            )

    return {
        'original_url': r.original_url,
        'main_category': r.main_category,
        'original_category': original_cat_name,
        'keyword': r.keyword,
        'redirect_url': final_redirect_url,
        'redirect_category': final_redirect_cat_name,
        'is_cross_category': is_cross_category,
        'facet_fragment': r.facet_fragment,
        'facet_names': r.facet_names,
        'facet_value_names': r.facet_value_names,
        'facet_count': r.facet_count,
        'match_score': r.match_score,
        'match_type': final_match_type,
        'reliability_score': final_score,
        'reliability_tier': final_tier,
        'h1_similarity': h1_similarity,  # V26: synthetic H1 overlap (0-100)
        'reject_reason': reject_reason,  # V27: why the row was hard-rejected
        'flag_for_review': flag_for_review,  # V28: legacy-confident but search disagrees
        'search_derived_total': search_derived_total,
        'search_derived_dom_cat': search_derived_dom_cat,
        'search_derived_dom_share': search_derived_dom_share,
        'matched_keywords': matched_keywords_str,
        'unmatched_keywords': unmatched_keywords_str,
        'match_coverage': match_coverage,
        'has_stopwords': has_stopwords,
        'stopwords_found': stopwords_found,
        'shop_in_keyword': shop_in_keyword,  # V23: Winkelnamen apart geregistreerd
        'keyword_type': keyword_type,  # V23.1: Type keyword (product, shop_only, stopwords_only, etc.)
        'has_dimensions': has_dims,  # V23.2: Bevat keyword afmetingen (200cm, 120x80, etc.)
        'merk_of_shop_missing': getattr(r, 'merk_of_shop_missing', ''),
        'success': bool(final_redirect_url),
        'reason': final_reason,
    }


def print_summary(results_df):
    """Print summary statistics."""
    total = len(results_df)
    success_count = results_df['success'].sum()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total URLs processed: {total:,}")
    print(f"Successful redirects: {success_count:,} ({100*success_count/total:.1f}%)")
    print(f"Failed:               {total - success_count:,} ({100*(total-success_count)/total:.1f}%)")

    print("\nBy reliability tier:")
    for tier in ['A', 'B', 'C', 'D']:
        count = len(results_df[results_df['reliability_tier'] == tier])
        print(f"  Tier {tier}: {count:,} ({100*count/total:.1f}%)")

    prod_ready = len(results_df[results_df['reliability_tier'].isin(['A', 'B'])])
    print(f"\nProduction ready (A+B): {prod_ready:,} ({100*prod_ready/total:.1f}%)")


def main():
    parser = argparse.ArgumentParser(
        description="Beslist.nl R-URL Optimizer - Parallel V2"
    )
    parser.add_argument('input', help='Input CSV file')
    parser.add_argument('-o', '--output', help='Output CSV file')
    parser.add_argument('-c', '--column', default='r_url', help='URL column name')
    parser.add_argument('-w', '--workers', type=int, default=None,
                        help='Worker count (default: CPU - 2)')
    parser.add_argument('--multi-facet', action='store_true',
                        help='Enable multi-facet matching')
    parser.add_argument('--threshold', type=int, default=80,
                        help='Fuzzy threshold (default: 80)')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Batch size for progress updates')
    parser.add_argument('--chunksize', type=int, default=100,
                        help='Chunk size for multiprocessing (default: 100)')
    parser.add_argument('--no-v28', action='store_true',
                        help='Skip the V28 search-derived prefetch + rescue layer')
    parser.add_argument('--no-token-coverage', action='store_true',
                        help='V29: skip the facet-value-centric token-coverage scorer '
                             'and use the legacy per-token cascade instead. The '
                             'token-coverage scorer is on by default — it picks '
                             '"Senioren telefoon" over "Draadloze telefoon" for '
                             '"vaste senioren telefoons", where the legacy count-'
                             'tiebreak gets it wrong.')
    parser.add_argument('--enable-facet-probe', action='store_true',
                        help='V29 EXPERIMENTAL: after V28 picks a dominant '
                             'deepest_cat, also probe candidate facet values to '
                             'find one covering ≥60%% of the result set, and '
                             'append it to the redirect URL. Stage 1 reads from '
                             "the V28 base response's facets[] (no extra calls); "
                             'stage 2 falls back to per-value filter probes. '
                             'Same SEARCH_QPS budget as V28 prefetch.')
    parser.add_argument('--rescue-include-size',
                        dest='rescue_include_size',
                        action=argparse.BooleanOptionalAction, default=True,
                        help='V34: when the multi-facet rescue fires and the '
                             'query names a size (XL, 122-128), append the '
                             'matching maat_* facet so the landing page is '
                             'size-narrowed. ON by default (2026-06-06); pass '
                             '--no-rescue-include-size to fall back to the '
                             'broader category page (per-size pages churn '
                             'in/out of stock).')

    args = parser.parse_args()

    num_workers = args.workers or max(1, mp.cpu_count() - 2)

    # Load input
    print(f"\nLoading {args.input}...")
    df = pd.read_csv(args.input)

    if args.column not in df.columns:
        print(f"ERROR: Column '{args.column}' not found")
        return

    urls = df[args.column].tolist()
    total = len(urls)
    print(f"Loaded {total:,} URLs")

    # Hard exclusion: external API / scraper-proxy URLs (e.g. api.scrape.do,
    # which embeds a beslist URL as a query param and otherwise leaks into the
    # global pass). Drop them at the input so they never appear in the output
    # at all — the in-worker guard in process_url_v2 stays as a backstop.
    EXCLUDED_HOSTS = ("api.scrape.do",)
    _kept = [u for u in urls if not any(h in str(u).lower() for h in EXCLUDED_HOSTS)]
    _dropped = total - len(_kept)
    if _dropped:
        print(f"Excluded {_dropped:,} external-host URL(s) "
              f"({', '.join(EXCLUDED_HOSTS)}) from processing")
        urls = _kept
        total = len(urls)

    # Pre-load data and cache
    data = preload_data(use_cache=True)
    cache_file = '/tmp/r_url_optimizer_cache.pkl'
    save_data_cache(data, cache_file)
    print(f"Data cached to {cache_file}")

    # Default output
    if not args.output:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"output/results_{timestamp}.csv"

    # Check for existing progress (resume capability)
    progress_file = args.output.replace('.csv', '_progress.csv')
    processed_urls = set()
    if os.path.exists(progress_file):
        print(f"\nFound progress file: {progress_file}")
        existing_df = pd.read_csv(progress_file)
        processed_urls = set(existing_df['original_url'].tolist())
        print(f"Resuming from {len(processed_urls):,} already processed URLs")

    # Filter out already processed URLs
    urls_to_process = [u for u in urls if u not in processed_urls]
    total_remaining = len(urls_to_process)

    if total_remaining == 0:
        print("All URLs already processed!")
        # Just copy progress to final output
        if os.path.exists(progress_file):
            import shutil
            shutil.copy(progress_file, args.output)
            print(f"Results saved to: {args.output}")
        return

    print(f"\nURLs to process: {total_remaining:,} (skipped {len(processed_urls):,} already done)")

    # V28: Prefetch search-derived signals once, sequentially, throttled.
    # The parallel matcher reads from the SQLite cache only — never hits
    # the API — so worker count is decoupled from API rate.
    if not getattr(args, 'no_v28', False):
        from src.parser import RUrlParser as _Parser
        from src import search_derived as _sd
        _parser = _Parser()
        _pairs = []
        for u in urls_to_process:
            p = _parser.parse(u)
            if p.is_valid and p.main_category and p.keyword:
                _pairs.append((p.main_category, p.keyword))
        if _pairs:
            print(f"\n[V28] Prefetching search signals for {len(set(_pairs)):,} unique "
                  f"(maincat, keyword) pairs at {_sd.SEARCH_QPS} QPS...")
            stats = _sd.prefetch_pairs(_pairs)
            print(f"[V28] Prefetch done: {stats}")

            # V29: optional facet-probe pass — only fires when V28 cache says
            # mode=and. Reads the cached surfaced_facets first (free), then
            # falls back to per-value filter probes.
            if getattr(args, 'enable_facet_probe', False):
                from src import facet_probe as _fp
                fp_stats = _fp.prefetch_facet_probes(_pairs)
                print(f"[V29 facet-probe] Prefetch done: {fp_stats}")

    # Process with pool
    print(f"\nProcessing {total_remaining:,} URLs with {num_workers} workers...")
    start_time = time.time()

    results = []
    url_args = [(url, args.multi_facet) for url in urls_to_process]

    # Optimal chunksize: balance between overhead and load distribution
    # For large datasets: higher chunksize = less overhead
    chunksize = args.chunksize
    if total_remaining > 10000:
        chunksize = max(chunksize, 200)
    elif total_remaining > 100000:
        chunksize = max(chunksize, 500)

    # Batch save interval
    SAVE_INTERVAL = 5000  # Save every 5000 URLs
    last_save_count = 0

    with mp.Pool(
        processes=num_workers,
        initializer=init_worker_v2,
        initargs=(cache_file, args.threshold, not args.no_token_coverage,
                  args.rescue_include_size)
    ) as pool:
        # Use imap_unordered for better throughput
        with tqdm(total=total_remaining, desc="Processing") as pbar:
            for result in pool.imap_unordered(process_url_v2, url_args, chunksize=chunksize):
                results.append(result)
                pbar.update(1)

                # Incremental save every SAVE_INTERVAL URLs
                if len(results) - last_save_count >= SAVE_INTERVAL:
                    # Save progress
                    batch_df = pd.DataFrame(results)
                    if os.path.exists(progress_file):
                        # Append to existing
                        existing_df = pd.read_csv(progress_file)
                        combined_df = pd.concat([existing_df, batch_df], ignore_index=True)
                        combined_df.to_csv(progress_file, index=False)
                    else:
                        batch_df.to_csv(progress_file, index=False)
                    last_save_count = len(results)
                    tqdm.write(f"  [Checkpoint] Saved {len(results):,} URLs to {progress_file}")

    elapsed = time.time() - start_time
    print(f"\nProcessed in {elapsed:.1f}s ({total_remaining/elapsed:.1f} URLs/sec)")

    # Save final results (combine with any previously processed)
    print(f"\nSaving to {args.output}...")
    results_df = pd.DataFrame(results)

    # Combine with previously processed if resuming
    if os.path.exists(progress_file) and len(processed_urls) > 0:
        existing_df = pd.read_csv(progress_file)
        # Only keep existing that were not in this batch
        existing_df = existing_df[~existing_df['original_url'].isin(results_df['original_url'])]
        results_df = pd.concat([existing_df, results_df], ignore_index=True)

    if 'visits' in df.columns:
        # Match by original_url since order may differ with imap_unordered
        url_to_visits = dict(zip(df[args.column], df['visits']))
        results_df['visits'] = results_df['original_url'].map(url_to_visits)
    if 'visit_rev' in df.columns:
        url_to_rev = dict(zip(df[args.column], df['visit_rev']))
        results_df['visit_rev'] = results_df['original_url'].map(url_to_rev)

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(args.output, index=False)

    # Also save final progress file
    results_df.to_csv(progress_file, index=False)

    print_summary(results_df)
    print(f"\nResults saved to: {args.output}")

    # Cleanup cache (but keep progress file for safety)
    os.remove(cache_file)
    print(f"Progress file kept at: {progress_file}")


if __name__ == '__main__':
    mp.set_start_method('spawn', force=True)
    main()
