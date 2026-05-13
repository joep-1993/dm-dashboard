"""
V29 Global R-URL Processing Script

Verwerkt de ~10.589 URLs met formaat /products/r/<keyword>/ (zonder categorie).
Deze worden niet herkend door de huidige parser omdat er geen main_category/subcategory
in het URL-pad zit.

Strategie:
1. Keyword extractie direct uit de URL
2. Cross-category subcategorie naam matching (geen categorie context)
3. Cross-category type facet matching
4. Facet matching binnen gevonden categorie (als subcategorie match gevonden)

Input:  data/output/redirects_v28_250k.csv
Output: data/output/redirects_v29_250k.csv
"""

import pandas as pd
import numpy as np
import re
import time
import sys
import os
from urllib.parse import unquote
from tqdm import tqdm
from multiprocessing import Pool

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main_parallel_v2 import (
    preload_data, save_data_cache, load_data_cache,
    extract_subcategory_id_from_url
)

CACHE_FILE = '/tmp/r_url_optimizer_cache.pkl'
INPUT_FILE = 'data/output/redirects_v28_250k.csv'
OUTPUT_FILE = 'data/output/redirects_v29_250k.csv'

# Regex to extract keyword from global R-URLs
# /products/r/vacuumzakken_action/ → "vacuumzakken_action"
# /products/r/hobby_horse_goedkoop/page_2 → "hobby_horse_goedkoop"
GLOBAL_RURL_PATTERN = re.compile(
    r'(?:https?://)?(?:www\.)?beslist\.nl/products/r/(.+?)(?:/page_\d+)?/?$'
)


def extract_keyword_from_global_url(url: str) -> str:
    """Extract and normalize keyword from a global R-URL."""
    decoded = unquote(url.strip())
    match = GLOBAL_RURL_PATTERN.match(decoded)
    if not match:
        return ''

    raw_keyword = match.group(1)

    # Strip trailing /c/... (existing facet)
    if '/c/' in raw_keyword:
        raw_keyword = raw_keyword.split('/c/')[0]

    # Normalize: replace separators with spaces, lowercase, strip
    keyword = raw_keyword.lower()
    keyword = re.sub(r'[-_+/]', ' ', keyword)
    keyword = ' '.join(keyword.split())
    return keyword


# Worker globals
_worker_data = None


def init_worker(cache_file, fuzzy_threshold):
    """Initialize worker with pre-cached data."""
    global _worker_data

    from src.matcher import KeywordMatcher
    from src.url_builder import UrlBuilder
    from src.facet_filter import FacetFilter

    import logging
    logging.getLogger().setLevel(logging.WARNING)

    data = load_data_cache(cache_file)

    _worker_data = {
        'matcher': KeywordMatcher(fuzzy_threshold=fuzzy_threshold),
        'builder': UrlBuilder(),
        'facet_filter': FacetFilter(data['facets_df']),
        'category_lookup': data['category_lookup'],
        'all_type_facets': data['all_type_facets'],
        'categories_df': data['categories_df'],
    }


def process_global_url(args):
    """Process a single global R-URL without category context."""
    global _worker_data

    url, keyword = args

    from src.reliability_scorer import calculate_reliability_score, get_reliability_tier, compute_h1_similarity, _v27_reject_reason
    from src.validation_rules import (
        STOPWORDS, SHOP_NAMES, SUBCATEGORY_MATCH_THRESHOLD
    )

    d = _worker_data
    matcher = d['matcher']
    builder = d['builder']
    facet_filter = d['facet_filter']
    category_lookup = d['category_lookup']
    all_type_facets = d['all_type_facets']
    categories_df = d['categories_df']

    # Empty result template
    empty = {
        'original_url': url,
        'main_category': '',
        'original_category': '',
        'keyword': keyword,
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
        'shop_in_keyword': '',
        'keyword_type': 'no_matchable',
        'has_dimensions': False,
        'merk_of_shop_missing': '',
        'success': False,
        'reason': 'No keyword extracted',
    }

    if not keyword:
        return empty

    # V30: Shop-name short-circuit — keep the row but skip matching.
    from src.validation_rules import detect_shops_in_keyword as _detect_shops
    _shops = _detect_shops(keyword)
    if _shops:
        shop_row = dict(empty)
        shop_row.update({
            'match_type': 'shop_name',
            'shop_in_keyword': ', '.join(_shops),
            'keyword_type': 'shop_only',
            'reason': 'shop_name detected',
        })
        return shop_row

    result = None
    HIGH_SUBCAT_THRESHOLD = 95

    # ======================================================================
    # MATCHING (geen categorie context - alles is cross-category)
    # ======================================================================

    # --- 1. Subcategorie naam matching (hoog, ≥95) - alle categorieën ---
    subcat_match = matcher.match_subcategory_name(
        keyword, categories_df, main_category=None
    )
    # V28: Per-woord fallback
    if not subcat_match or subcat_match.get('score', 0) < HIGH_SUBCAT_THRESHOLD:
        for kw in keyword.split():
            if len(kw) < 4:
                continue
            if kw in STOPWORDS or kw in SHOP_NAMES:
                continue
            word_match = matcher.match_subcategory_name(
                kw, categories_df, main_category=None
            )
            if word_match and word_match.get('score', 0) >= HIGH_SUBCAT_THRESHOLD:
                subcat_match = word_match
                break

    if subcat_match and subcat_match.get('score', 0) >= HIGH_SUBCAT_THRESHOLD:
        # We hebben een categorie gevonden — probeer facet matching daarin
        matched_main_cat = _extract_main_cat_from_url_name(
            subcat_match.get('url_name', '')
        )
        matched_subcat_name = subcat_match.get('url_name', '')

        # Probeer facets binnen de gevonden subcategorie
        if matched_subcat_name:
            matched_subcat_id = _extract_last_id(matched_subcat_name)
            if matched_subcat_id:
                filtered_facets = facet_filter.filter_by_subcategory(matched_subcat_id)
                facet_values = facet_filter.get_facet_values(filtered_facets)
                if facet_values:
                    match_results = matcher.match_multi_word(
                        keyword, facet_values,
                        all_type_facets=None,
                        require_type_for_merk=True,
                        current_main_category=matched_main_cat
                    )
                    if match_results:
                        # Bouw een ParsedRUrl-achtig object voor de builder
                        from src.parser import ParsedRUrl
                        pseudo_parsed = ParsedRUrl(
                            original_url=url,
                            category_path=f"{matched_main_cat}/{matched_subcat_name}",
                            full_category_path=f"/products/{matched_main_cat}/{matched_subcat_name}",
                            main_category=matched_main_cat,
                            subcategory_id=matched_subcat_id,
                            subcategory_name=matched_subcat_name,
                            keyword=keyword,
                            existing_facet='',
                        )
                        result = builder.build_multi_facet(pseudo_parsed, match_results)
                        result.reason = f"[global_facet_in_subcat] {result.reason}"

        # Als geen facet match, gebruik de subcategorie redirect
        if not result:
            result = builder.build_subcategory_redirect(
                original_url=url,
                keyword=keyword,
                subcategory_match=subcat_match,
                main_category=matched_main_cat,
                existing_facet=''
            )
            result.reason = f"[global_subcat_high] {result.reason}"

    # --- 2. Cross-category type facet matching ---
    if not result and all_type_facets:
        match_results = matcher.match_multi_word(
            keyword, all_type_facets,
            all_type_facets=None,
            require_type_for_merk=True,
            current_main_category=None
        )
        if match_results:
            # MatchResult objecten hebben .facet_value.url en .cross_category_path
            # De builder's build_multi_facet verwerkt cross_category_path automatisch
            # We moeten een geldig ParsedRUrl meegeven als basis
            first = match_results[0]
            fv_url = first.facet_value.url if first.facet_value else ''
            cat_parts = _extract_category_from_facet_url(fv_url)

            if cat_parts:
                from src.parser import ParsedRUrl
                pseudo_parsed = ParsedRUrl(
                    original_url=url,
                    category_path=cat_parts['category_path'],
                    full_category_path=f"/products/{cat_parts['category_path']}",
                    main_category=cat_parts['main_category'],
                    subcategory_id=cat_parts['subcategory_id'],
                    subcategory_name=cat_parts['subcategory_name'],
                    keyword=keyword,
                    existing_facet='',
                )
                result = builder.build_multi_facet(pseudo_parsed, match_results)
                result.reason = f"[global_cross_type] {result.reason}"

    # --- 3. Subcategorie naam matching (laag, ≥80) ---
    if not result:
        subcat_match_low = matcher.match_subcategory_name(
            keyword, categories_df, main_category=None
        )
        if not subcat_match_low:
            for kw in keyword.split():
                if len(kw) < 4:
                    continue
                if kw in STOPWORDS or kw in SHOP_NAMES:
                    continue
                word_match = matcher.match_subcategory_name(
                    kw, categories_df, main_category=None
                )
                if word_match and word_match.get('score', 0) >= SUBCATEGORY_MATCH_THRESHOLD:
                    subcat_match_low = word_match
                    break

        if subcat_match_low and subcat_match_low.get('score', 0) >= SUBCATEGORY_MATCH_THRESHOLD:
            matched_main_cat = _extract_main_cat_from_url_name(
                subcat_match_low.get('url_name', '')
            )
            result = builder.build_subcategory_redirect(
                original_url=url,
                keyword=keyword,
                subcategory_match=subcat_match_low,
                main_category=matched_main_cat,
                existing_facet=''
            )
            result.reason = f"[global_subcat_low] {result.reason}"

    # --- 4. Geen match ---
    if not result:
        empty['reason'] = f"[global] No match found for '{keyword}'"
        return empty

    # ======================================================================
    # OUTPUT BOUWEN (zelfde logica als process_url_v2)
    # ======================================================================
    r = result
    redirect_subcat_id = extract_subcategory_id_from_url(r.redirect_url)
    redirect_cat_name = category_lookup.get(
        str(redirect_subcat_id), redirect_subcat_id
    ) if redirect_subcat_id else ''

    # Keyword analyse
    keyword_words = [w.lower() for w in keyword.split() if len(w) >= 2]
    stopwords_in_keyword = [w for w in keyword_words if w in STOPWORDS]
    shops_in_keyword = [w for w in keyword_words if w in SHOP_NAMES]

    # Dimension check
    DIMENSION_PATTERN = re.compile(
        r'\d+\s*x\s*\d+|\d+\s*cm\b|\d+\s*mm\b|\d+\s*meter\b|\d+\s*m\b|\d+\s*persoons\b|\d+\s*liter\b',
        re.IGNORECASE
    )
    has_dims = bool(DIMENSION_PATTERN.search(keyword))

    # Coverage berekening
    match_target = r.facet_value_names if r.facet_value_names else redirect_cat_name
    matched_keywords = []
    unmatched_keywords = []

    if match_target and keyword_words:
        facet_values_lower = [fv.lower() for fv in match_target.split(', ')]
        for word in keyword_words:
            if word in STOPWORDS or word in SHOP_NAMES:
                continue
            word_matched = False
            for fv in facet_values_lower:
                if (word in fv or fv in word or
                    word.rstrip('e').rstrip('s') in fv or
                    fv.rstrip('e').rstrip('s') in word):
                    word_matched = True
                    break
            if word_matched:
                matched_keywords.append(word)
            else:
                unmatched_keywords.append(word)

    non_stopword_keywords = [
        w for w in keyword_words if w not in STOPWORDS and w not in SHOP_NAMES
    ]
    match_coverage = 0.0
    if non_stopword_keywords:
        match_coverage = round(
            100 * len(matched_keywords) / len(non_stopword_keywords), 1
        )

    # Keyword type
    has_matchable = len(non_stopword_keywords) > 0
    if has_matchable:
        keyword_type = 'product'
    elif shops_in_keyword and stopwords_in_keyword:
        keyword_type = 'shop_and_stopwords'
    elif shops_in_keyword:
        keyword_type = 'shop_only'
    elif stopwords_in_keyword:
        keyword_type = 'stopwords_only'
    else:
        keyword_type = 'no_matchable'

    # V26: Synthetic H1 similarity (no crawling — built from URL components).
    # Global R-URLs have no original deepest_cat (they're maincat-level), so
    # only the keyword feeds the R-URL side.
    h1_similarity = compute_h1_similarity(
        keyword=keyword,
        original_cat_name=None,
        redirect_cat_name=redirect_cat_name,
        facet_value_names=r.facet_value_names,
    ) if r.success else 0

    # Reliability score — global URLs zijn altijd cross-category
    reliability_score = 0
    reliability_tier = 'D'
    if r.success:
        reliability_score = calculate_reliability_score(
            match_score=r.match_score,
            facet_count=r.facet_count,
            match_type=r.match_type,
            is_cross_category=True,  # Global = altijd cross-category
            facet_value_names=r.facet_value_names,
            keyword=keyword,
            reason=r.reason,
            match_coverage=match_coverage,
            h1_similarity=h1_similarity,
            matched_keywords=matched_keywords,    # V27: generic-adjective + long-unmatched floors
            unmatched_keywords=unmatched_keywords,
        )
        reliability_tier = get_reliability_tier(reliability_score)
    # V27: only surface the reason when the scorer actually rejected
    # (score=0). Subcategory-name matches return early in the scorer and
    # would otherwise carry a misleading flag.
    reject_reason = (
        _v27_reject_reason(matched_keywords, unmatched_keywords) or ''
        if reliability_score == 0 else ''
    )

    return {
        'original_url': r.original_url,
        'main_category': r.main_category,
        'original_category': '',  # Geen originele categorie
        'keyword': keyword,
        'redirect_url': r.redirect_url,
        'redirect_category': redirect_cat_name,
        'is_cross_category': True,
        'facet_fragment': r.facet_fragment,
        'facet_names': r.facet_names,
        'facet_value_names': r.facet_value_names,
        'facet_count': r.facet_count,
        'match_score': r.match_score,
        'match_type': r.match_type,
        'reliability_score': reliability_score,
        'reliability_tier': reliability_tier,
        'h1_similarity': h1_similarity,  # V26: synthetic H1 overlap (0-100)
        'reject_reason': reject_reason,  # V27: why the row was hard-rejected
        'matched_keywords': ', '.join(matched_keywords),
        'unmatched_keywords': ', '.join(unmatched_keywords),
        'match_coverage': match_coverage,
        'has_stopwords': len(stopwords_in_keyword) > 0,
        'stopwords_found': ', '.join(stopwords_in_keyword),
        'shop_in_keyword': ', '.join(shops_in_keyword),
        'keyword_type': keyword_type,
        'has_dimensions': has_dims,
        'merk_of_shop_missing': getattr(r, 'merk_of_shop_missing', ''),
        'success': r.success,
        'reason': r.reason,
    }


def _extract_main_cat_from_url_name(url_name: str) -> str:
    """Extract main_category from url_name like 'klussen_486170_6356938'.

    Split on underscores up to the first numeric segment instead of the older
    `[a-z_]+?` regex — hyphenated maincats like sport_outdoor_vrije-tijd or
    films-series otherwise return as-is and produce broken redirect paths.
    """
    if not url_name:
        return ''
    parts = url_name.split('_')
    main_cat_parts = []
    for part in parts:
        if part.isdigit():
            break
        main_cat_parts.append(part)
    if main_cat_parts and len(main_cat_parts) < len(parts):
        return '_'.join(main_cat_parts)
    return url_name


def _extract_last_id(url_name: str) -> str:
    """Extract last numeric ID from url_name."""
    if not url_name:
        return ''
    parts = url_name.split('_')
    for part in reversed(parts):
        if part.isdigit():
            return part
    return ''


def _extract_category_from_facet_url(facet_url: str) -> dict:
    """Extract category info from a facet URL like '/products/klussen/klussen_486170/c/...'."""
    if not facet_url:
        return None
    # Remove /c/ part
    path = facet_url.split('/c/')[0] if '/c/' in facet_url else facet_url
    path = path.rstrip('/')
    # Extract parts after /products/
    if '/products/' in path:
        path = path.split('/products/')[-1]
    parts = path.split('/')
    if len(parts) >= 2:
        main_cat = parts[0]
        subcat_name = parts[1]
        subcat_id = _extract_last_id(subcat_name)
        return {
            'main_category': main_cat,
            'subcategory_name': subcat_name,
            'subcategory_id': subcat_id,
            'category_path': f"{main_cat}/{subcat_name}",
        }
    return None


def main():
    start_time = time.time()

    print(f"Loading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df):,} rows")

    # Identificeer global R-URLs (main_category is NaN)
    global_mask = df['main_category'].isna()
    global_df = df[global_mask].copy()
    print(f"\nGlobal R-URLs (geen main_category): {len(global_df):,}")

    if len(global_df) == 0:
        print("Geen global R-URLs gevonden!")
        return

    # Extract keywords
    global_df['extracted_keyword'] = global_df['original_url'].apply(
        extract_keyword_from_global_url
    )
    has_keyword = global_df['extracted_keyword'].str.len() > 0
    print(f"Keyword geextraheerd: {has_keyword.sum():,}")
    print(f"Geen keyword:         {(~has_keyword).sum():,}")

    if has_keyword.sum() == 0:
        print("Geen keywords geextraheerd!")
        return

    # Prepare args
    args_list = list(zip(
        global_df.loc[has_keyword, 'original_url'],
        global_df.loc[has_keyword, 'extracted_keyword']
    ))

    # Ensure data cache
    if not os.path.exists(CACHE_FILE):
        print("\nPre-loading data...")
        data = preload_data()
        save_data_cache(data, CACHE_FILE)
    else:
        print(f"\nUsing cached data from {CACHE_FILE}")

    # Process
    num_workers = min(14, os.cpu_count() or 4)
    print(f"\nProcessing {len(args_list):,} URLs with {num_workers} workers...")

    results = []
    with Pool(
        processes=num_workers,
        initializer=init_worker,
        initargs=(CACHE_FILE, 80)
    ) as pool:
        for result in tqdm(
            pool.imap_unordered(process_global_url, args_list, chunksize=100),
            total=len(args_list),
            desc="Processing global R-URLs"
        ):
            results.append(result)

    patched_df = pd.DataFrame(results)
    print(f"\nProcessed {len(patched_df):,} URLs")

    # Also include the rows where keyword extraction failed
    failed_urls = global_df.loc[~has_keyword, 'original_url'].tolist()
    if failed_urls:
        failed_results = []
        for url in failed_urls:
            failed_results.append({
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
                'shop_in_keyword': '',
                'keyword_type': 'no_matchable',
                'has_dimensions': False,
                'merk_of_shop_missing': '',
                'success': False,
                'reason': f'Could not extract keyword from URL: {url}',
            })
        patched_df = pd.concat(
            [patched_df, pd.DataFrame(failed_results)], ignore_index=True
        )

    # Merge back into full DataFrame
    df_indexed = df.set_index('original_url')
    patched_indexed = patched_df.set_index('original_url')

    for col in patched_indexed.columns:
        if col in df_indexed.columns:
            # Cast to object to avoid FutureWarning when writing strings into
            # an originally-numeric column.
            if df_indexed[col].dtype != patched_indexed[col].dtype:
                df_indexed[col] = df_indexed[col].astype(object)
            df_indexed.loc[patched_indexed.index, col] = patched_indexed[col]

    df_result = df_indexed.reset_index()

    # Save
    print(f"\nSaving to {OUTPUT_FILE}...")
    df_result.to_csv(OUTPUT_FILE, index=False)

    xlsx_file = OUTPUT_FILE.replace('.csv', '.xlsx')
    print(f"Saving to {xlsx_file}...")
    df_result.to_excel(xlsx_file, index=False, engine='openpyxl')

    elapsed = time.time() - start_time

    # ======================================================================
    # STATS
    # ======================================================================
    print(f"\n{'='*60}")
    print(f"GLOBAL R-URL PROCESSING COMPLETE in {elapsed:.0f}s")
    print(f"{'='*60}")

    # Vergelijk oud vs nieuw voor de global URLs
    print(f"\n--- Resultaten voor {len(patched_df):,} global R-URLs ---")

    success = patched_df['success'].sum()
    total = len(patched_df)
    print(f"Success rate: {success:,}/{total:,} ({success/total*100:.1f}%)")

    print(f"\n--- Tier verdeling (global URLs) ---")
    tiers = patched_df['reliability_tier'].value_counts().sort_index()
    for tier in ['A', 'B', 'C', 'D']:
        count = tiers.get(tier, 0)
        print(f"  Tier {tier}: {count:>6,} ({count/total*100:.1f}%)")

    ab = tiers.get('A', 0) + tiers.get('B', 0)
    print(f"  Production ready (A+B): {ab:>6,} ({ab/total*100:.1f}%)")

    print(f"\n--- Match type verdeling ---")
    types = patched_df['match_type'].value_counts()
    for mt, count in types.items():
        print(f"  {mt:>30s}: {count:>6,} ({count/total*100:.1f}%)")

    # Visits impact
    if 'visits' in df.columns:
        global_visits = df.loc[global_mask, 'visits'].sum()
        matched_visits = 0
        for _, row in patched_df.iterrows():
            if row.get('success', False):
                visit_row = df.loc[
                    df['original_url'] == row['original_url'], 'visits'
                ]
                if not visit_row.empty:
                    matched_visits += visit_row.values[0]
        print(f"\n--- Visits impact ---")
        print(f"  Totaal visits global URLs: {global_visits:,.0f}")
        print(f"  Visits nu gematcht:        {matched_visits:,.0f} ({matched_visits/global_visits*100:.1f}%)")

    # Top 10 matches
    print(f"\n--- Top 10 matches (by score) ---")
    matched = patched_df[patched_df['success'] == True].nlargest(10, 'match_score')
    for _, row in matched.iterrows():
        kw = row['keyword'][:30]
        redir = str(row.get('redirect_url', ''))[-50:]
        print(f"  {kw:<30s} -> ...{redir}  (score:{row['match_score']}, tier:{row['reliability_tier']})")

    # Overall stats V29
    print(f"\n--- Overall stats V29 ---")
    total_all = len(df_result)
    success_all = df_result['success'].sum()
    print(f"Total rows: {total_all:,}")
    print(f"Success rate: {success_all:,} ({success_all/total_all*100:.1f}%)")
    overall_tiers = df_result['reliability_tier'].value_counts().sort_index()
    for tier in ['A', 'B', 'C', 'D']:
        count = overall_tiers.get(tier, 0)
        print(f"  Tier {tier}: {count:>7,} ({count/total_all*100:.1f}%)")
    ab_all = overall_tiers.get('A', 0) + overall_tiers.get('B', 0)
    print(f"  Production ready (A+B): {ab_all:>7,} ({ab_all/total_all*100:.1f}%)")


if __name__ == '__main__':
    import multiprocessing
    multiprocessing.set_start_method('spawn', force=True)
    main()
