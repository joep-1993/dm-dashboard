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


def init_worker_v2(cache_file, fuzzy_threshold):
    """Initialize worker with pre-cached data."""
    global _worker_data

    from src.parser import RUrlParser
    from src.facet_filter import FacetFilter
    from src.matcher import KeywordMatcher
    from src.url_builder import UrlBuilder

    # Suppress logging in workers
    logging.getLogger().setLevel(logging.WARNING)

    # Load pre-cached data
    data = load_data_cache(cache_file)

    _worker_data = {
        'parser': RUrlParser(),
        'facet_filter': FacetFilter(data['facets_df']),
        'matcher': KeywordMatcher(fuzzy_threshold=fuzzy_threshold),
        'builder': UrlBuilder(),
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


def process_url_v2(args):
    """Process single URL in worker."""
    global _worker_data

    url, multi_facet = args

    import re  # Nodig voor DIMENSION_PATTERN + V30 coverage check (moet vóór gebruik staan)
    from src.reliability_scorer import calculate_reliability_score, get_reliability_tier
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

    # V30: Shop-name short-circuit — if the keyword contains any SHOP_NAME
    # word, skip matching entirely. Row stays in the output for visibility
    # but without a redirect URL.
    from src.validation_rules import detect_shops_in_keyword as _detect_shops
    _shops = _detect_shops(parsed.keyword)
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
            match_results = matcher.match_multi_word(
                parsed.keyword, facet_values,
                all_type_facets=all_type_facets,
                require_type_for_merk=True,
                current_main_category=parsed.main_category
            )
            if match_results:
                result = builder.build_multi_facet(parsed, match_results)

    # 1. SUBCATEGORY FACETS - Single facet
    if not result and facet_values:
        match_result = matcher.match_with_partial(parsed.keyword, facet_values)
        if match_result.is_match:
            result = builder.build(parsed, match_result)

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
            if not child_match or child_match.get('score', 0) < HIGH_SUBCAT_THRESHOLD:
                for kw in parsed.keyword.lower().split():
                    if len(kw) < 4:
                        continue
                    wm = matcher.match_subcategory_name(
                        kw, categories_df, main_category=parsed.subcategory_name
                    )
                    if wm and wm.get('score', 0) >= HIGH_SUBCAT_THRESHOLD:
                        child_match = wm
                        break
            if child_match and child_match.get('score', 0) >= HIGH_SUBCAT_THRESHOLD:
                result = builder.build_subcategory_redirect(
                    original_url=url,
                    keyword=parsed.keyword,
                    subcategory_match=child_match,
                    main_category=parsed.main_category,
                    existing_facet=parsed.existing_facet,
                )
                result.reason = f"[child_subcat] " + result.reason

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
                        break
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

    # 4. MAIN CATEGORY FACETS - Zoek in alle facets binnen maincat
    # Voor specifiekere termen: "onzichtbare scharnieren" -> facet "Onzichtbare scharnieren"
    if not result:
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
                    if match_results:
                        result = builder.build_multi_facet(parsed, match_results)
                        result.reason = f"[maincat] " + result.reason
                else:
                    maincat_match = matcher.match_with_partial(parsed.keyword, maincat_facets)
                    if maincat_match.is_match:
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

    # 6. V14: CROSS-CATEGORY SUBCATEGORIE NAAM MATCHING
    # Alleen als geen match binnen maincat - zoek in alle categorieën
    # V30: Geen per-woord fallback (te veel valse positieven bij cross-category)
    # V30: Vereist score=100 (exacte match op categorienaam) om verkeerde maincat redirects te voorkomen
    if not result:
        categories_df = d.get('categories_df')
        if categories_df is not None:
            subcat_match = matcher.match_subcategory_name(
                parsed.keyword,
                categories_df,
                main_category=None  # Search across ALL categories
            )
            # V30: Alleen accepteren bij score=100 (exacte match), geen fuzzy cross-category
            if subcat_match and subcat_match.get('score', 0) == 100:
                result = builder.build_subcategory_redirect(
                    original_url=url,
                    keyword=parsed.keyword,
                    subcategory_match=subcat_match,
                    main_category=parsed.main_category,
                    existing_facet=parsed.existing_facet  # V19: preserve existing facet
                )

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

        for word in keyword_words:
            word_matched = False
            # Skip stopwords and shop names - they are intentionally not matched
            if word in STOPWORDS or word in SHOP_NAMES:
                continue

            # Check if this keyword word is represented in any facet value
            for fv in facet_values_lower:
                # Check various match patterns
                if (word in fv or  # Word is contained in facet
                    fv in word or  # Facet is contained in word
                    word.rstrip('e').rstrip('s') in fv or  # Handle Dutch plurals/suffixes
                    fv.rstrip('e').rstrip('s') in word):
                    word_matched = True
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
            match_coverage=match_coverage  # V21: pass coverage to reliability scorer
        )
        reliability_tier = get_reliability_tier(reliability_score)

    return {
        'original_url': r.original_url,
        'main_category': r.main_category,
        'original_category': original_cat_name,
        'keyword': r.keyword,
        'redirect_url': r.redirect_url,
        'redirect_category': redirect_cat_name,
        'is_cross_category': is_cross_category,
        'facet_fragment': r.facet_fragment,
        'facet_names': r.facet_names,
        'facet_value_names': r.facet_value_names,
        'facet_count': r.facet_count,
        'match_score': r.match_score,
        'match_type': r.match_type,
        'reliability_score': reliability_score,
        'reliability_tier': reliability_tier,
        'matched_keywords': matched_keywords_str,
        'unmatched_keywords': unmatched_keywords_str,
        'match_coverage': match_coverage,
        'has_stopwords': has_stopwords,
        'stopwords_found': stopwords_found,
        'shop_in_keyword': shop_in_keyword,  # V23: Winkelnamen apart geregistreerd
        'keyword_type': keyword_type,  # V23.1: Type keyword (product, shop_only, stopwords_only, etc.)
        'has_dimensions': has_dims,  # V23.2: Bevat keyword afmetingen (200cm, 120x80, etc.)
        'merk_of_shop_missing': getattr(r, 'merk_of_shop_missing', ''),
        'success': r.success,
        'reason': r.reason
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
        initargs=(cache_file, args.threshold)
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
