"""
Category Keyword Service

Combines a keyword with category names (in singular and plural Dutch forms),
looks up search volumes via the Keyword Planner service, and aggregates
results per deepest category and main category.

Singular/plural forms are pre-computed and stored in category_forms.json.
Categories are preloaded from categories.xlsx at startup.
"""
import json
import os
import re
import pandas as pd
from collections import defaultdict
from typing import List, Dict, Tuple
from backend.keyword_planner_service import get_search_volumes

_DIR = os.path.dirname(__file__)

# Load pre-computed singular/plural forms
_FORMS_PATH = os.path.join(_DIR, "category_forms.json")
with open(_FORMS_PATH, "r", encoding="utf-8") as _f:
    CATEGORY_FORMS: Dict[str, List[str]] = json.load(_f)

# Preload categories from Excel
_CATEGORIES_PATH = os.path.join(_DIR, "categories.xlsx")
_df = pd.read_excel(_CATEGORIES_PATH)
_df.columns = ['maincat', 'maincat_id', 'deepest_cat', 'cat_id']
PRELOADED_CATEGORIES: List[Dict] = []
for _, _row in _df.iterrows():
    _mc = str(_row['maincat']).strip()
    _dc = str(_row['deepest_cat']).strip()
    if _mc and _dc and _mc != 'nan' and _dc != 'nan':
        PRELOADED_CATEGORIES.append({
            "maincat": _mc,
            "maincat_id": str(_row['maincat_id']),
            "deepest_cat": _dc,
            "cat_id": str(_row['cat_id']),
        })
print(f"[CATEGORY_KEYWORDS] Preloaded {len(PRELOADED_CATEGORIES)} categories from {_CATEGORIES_PATH}")


def clean_facet_value(raw_value: str) -> str:
    """
    Clean a facet value by removing HTML comments and normalizing whitespace.
    Keeps & as-is (e.g., 'Black & Decker').
    """
    cleaned = re.sub(r'<!--.*?-->', '', raw_value)
    cleaned = cleaned.strip()
    cleaned = ' '.join(cleaned.split())
    return cleaned


def parse_sic_sod(raw_value: str) -> Dict:
    """
    Parse SIC: and SOD: from a facet value.

    Returns dict with:
      - sic: SIC text or None (form used AFTER the category name)
      - sod: SOD text or None (form used BEFORE the category name)
      - plain: remaining text outside comments, or None
    """
    sic_match = re.search(r'<!--\s*SIC:\s*(.*?)\s*-->', raw_value)
    sod_match = re.search(r'<!--\s*SOD:\s*(.*?)\s*-->', raw_value)

    sic = sic_match.group(1).strip() if sic_match else None
    sod = sod_match.group(1).strip() if sod_match else None

    plain = re.sub(r'<!--.*?-->', '', raw_value).strip()
    plain = ' '.join(plain.split()) if plain else None

    return {"sic": sic, "sod": sod, "plain": plain}


def generate_facet_combinations(
    facet_values: List[Dict],
    categories: List[Dict],
) -> Tuple[List[str], Dict[str, Dict]]:
    """
    Generate keyword combinations from facet values × category names.

    For SIC/SOD facet values (both present):
      - SOD + cat_form  (e.g., "zwarte schoenen", "zwarte schoen")
      - cat_form + SIC  (e.g., "schoenen zwart", "schoen zwart")

    For normal facet values:
      - facet + cat_form  and  cat_form + facet

    Args:
        facet_values: List of dicts with at least 'facet_value' key
        categories: List of category dicts (maincat, maincat_id, deepest_cat, cat_id)

    Returns:
        (all_keywords, combination_map)
    """
    all_keywords = []
    combination_map = {}

    for facet in facet_values:
        raw = facet["facet_value"]
        parsed = parse_sic_sod(raw)
        has_sic_sod = bool(parsed["sic"] and parsed["sod"])

        if has_sic_sod:
            before_text = parsed["sod"].lower()
            after_text = parsed["sic"].lower()
        else:
            cleaned = clean_facet_value(raw)
            if not cleaned:
                # Fallback: use SIC, SOD, or plain if available
                cleaned = parsed["sod"] or parsed["sic"] or parsed["plain"] or ""
            if not cleaned:
                continue
            before_text = cleaned.lower()
            after_text = cleaned.lower()

        for cat in categories:
            deepest_cat = cat["deepest_cat"]
            forms = get_category_forms(deepest_cat)

            for form in forms:
                form = form.strip().lower()
                if not form:
                    continue

                combo_before = f"{before_text} {form}"
                combo_after = f"{form} {after_text}"

                for combo in [combo_before, combo_after]:
                    if combo not in combination_map:
                        combination_map[combo] = {
                            "maincat": cat["maincat"],
                            "maincat_id": cat.get("maincat_id", ""),
                            "deepest_cat": deepest_cat,
                            "cat_id": cat.get("cat_id", ""),
                            "facet_value": raw,
                            "facet_name": facet.get("facet_name", ""),
                        }
                        all_keywords.append(combo)

    return all_keywords, combination_map


def get_category_forms(deepest_cat: str) -> List[str]:
    """
    Look up pre-computed singular/plural forms for a category name.
    Falls back to just the lowercased name if not found in the mapping.
    """
    forms = CATEGORY_FORMS.get(deepest_cat)
    if forms:
        return list(set(forms))
    # Fallback: just use the name as-is
    return [deepest_cat.lower()]


def generate_combinations(keyword: str, categories: List[Dict]) -> Tuple[List[str], Dict[str, Dict]]:
    """
    Generate all keyword+category combinations.

    For each deepest_cat, generates:
      - keyword + plural, plural + keyword
      - keyword + singular, singular + keyword

    Args:
        keyword: The user's keyword (e.g., "nike")
        categories: List of dicts with keys: maincat, maincat_id, deepest_cat, cat_id

    Returns:
        (all_keywords, combination_map)
        - all_keywords: flat list of keyword strings to query
        - combination_map: maps each keyword string back to its category info
    """
    all_keywords = []
    combination_map = {}

    for cat in categories:
        deepest_cat = cat["deepest_cat"]
        forms = get_category_forms(deepest_cat)

        for form in forms:
            form = form.strip()
            if not form:
                continue
            combo_1 = f"{keyword.lower()} {form}"
            combo_2 = f"{form} {keyword.lower()}"
            for combo in [combo_1, combo_2]:
                if combo not in combination_map:
                    combination_map[combo] = {
                        "maincat": cat["maincat"],
                        "maincat_id": cat.get("maincat_id", ""),
                        "deepest_cat": deepest_cat,
                        "cat_id": cat.get("cat_id", ""),
                    }
                    all_keywords.append(combo)

    return all_keywords, combination_map


def process_category_keywords(keyword: str, categories: List[Dict]) -> Dict:
    """
    Main entry point: combine keyword with all categories, look up volumes, aggregate.

    Also generates combinations for maincat names themselves (treated as a
    deepest_cat row where deepest_cat = maincat and cat_id = maincat_id).

    Args:
        keyword: User keyword (e.g., "nike")
        categories: List of category dicts from Excel

    Returns:
        Dict with:
          - deepest_cat_results: list of {maincat, deepest_cat, search_volume}
          - maincat_results: list of {maincat, search_volume}
          - grand_total: int
          - stats: processing stats
    """
    # Add maincat-level entries: maincat name as deepest_cat, maincat_id as cat_id
    seen_maincats = set()
    maincat_entries = []
    for cat in categories:
        mc = cat["maincat"]
        if mc not in seen_maincats:
            seen_maincats.add(mc)
            maincat_entries.append({
                "maincat": mc,
                "maincat_id": cat.get("maincat_id", ""),
                "deepest_cat": mc,
                "cat_id": cat.get("maincat_id", ""),
            })

    all_categories = categories + maincat_entries
    all_keywords, combination_map = generate_combinations(keyword, all_categories)

    print(f"[CATEGORY_KEYWORDS] Keyword: '{keyword}', Categories: {len(categories)}, Combinations: {len(all_keywords)}")

    # Get search volumes for all combinations
    sv_result = get_search_volumes(all_keywords)
    results = sv_result.get("results", [])

    # Build lookup: normalized_keyword -> search_volume
    volume_lookup = {}
    for r in results:
        norm = r.get("normalized_keyword", "").lower()
        vol = r.get("search_volume", 0)
        if norm in volume_lookup:
            volume_lookup[norm] = max(volume_lookup[norm], vol)
        else:
            volume_lookup[norm] = vol

    # Aggregate per deepest_cat
    deepest_volumes = {}
    for combo, cat_info in combination_map.items():
        key = (cat_info["maincat"], cat_info["deepest_cat"], cat_info.get("maincat_id", ""), cat_info.get("cat_id", ""))
        # Normalize the combo the same way clean_keyword does
        norm_combo = re.sub(r'[-_]', ' ', combo)
        norm_combo = re.sub(r'[^a-zA-Z0-9\s]', '', norm_combo)
        norm_combo = ' '.join(norm_combo.split()).lower()

        vol = volume_lookup.get(norm_combo, 0)
        if key not in deepest_volumes:
            deepest_volumes[key] = 0
        deepest_volumes[key] += vol

    # Build deepest_cat results
    deepest_cat_results = []
    for (maincat, deepest_cat, maincat_id, cat_id), vol in deepest_volumes.items():
        deepest_cat_results.append({
            "maincat": maincat,
            "maincat_id": maincat_id,
            "deepest_cat": deepest_cat,
            "cat_id": cat_id,
            "search_volume": vol,
        })

    deepest_cat_results.sort(key=lambda x: (x["maincat"], x["deepest_cat"]))

    # Aggregate per maincat
    maincat_volumes = {}
    for r in deepest_cat_results:
        mc = r["maincat"]
        if mc not in maincat_volumes:
            maincat_volumes[mc] = 0
        maincat_volumes[mc] += r["search_volume"]

    maincat_results = [
        {"maincat": mc, "search_volume": vol}
        for mc, vol in sorted(maincat_volumes.items(), key=lambda x: x[1], reverse=True)
    ]

    grand_total = sum(r["search_volume"] for r in deepest_cat_results)

    return {
        "deepest_cat_results": deepest_cat_results,
        "maincat_results": maincat_results,
        "grand_total": grand_total,
        "stats": {
            "total_categories": len(categories),
            "total_combinations_queried": len(all_keywords),
            "unique_keywords_queried": sv_result.get("unique_keywords_queried", 0),
            "customer_ids_used": sv_result.get("customer_ids_used", 0),
        },
    }


def _normalize_keyword(kw: str) -> str:
    """Normalize a keyword the same way clean_keyword does in keyword_planner_service."""
    norm = re.sub(r'[-_]', ' ', kw)
    norm = re.sub(r'[^a-zA-Z0-9\s]', '', norm)
    return ' '.join(norm.split()).lower()


def process_facet_volumes(maincat_name: str, facet_rows: List[Dict]) -> Dict:
    """
    Process facet values for a maincat: generate keyword combinations with
    all deepest categories in that maincat, look up search volumes, and
    aggregate per facet row.

    Output matches input CSV columns + search_volume in column H.

    Args:
        maincat_name: Main category name (e.g., "Huishoudelijk")
        facet_rows: List of dicts with keys matching the input CSV columns:
                    main_category, main_category_id, facet_name, bucket,
                    url_name, id, facet_value

    Returns:
        Dict with:
          - results: list of dicts (input columns + search_volume)
          - stats: processing statistics
          - grand_total: sum of all search volumes
    """
    mc_cats = [c for c in PRELOADED_CATEGORIES if c["maincat"] == maincat_name]

    if not mc_cats:
        print(f"[FACET_VOLUMES] No deepest categories found for maincat '{maincat_name}'")
        return {
            "results": [{**row, "search_volume": 0} for row in facet_rows],
            "stats": {"facet_values": len(facet_rows), "deepest_cats": 0},
            "grand_total": 0,
        }

    # Generate all keyword combinations, tracking which facet rows produced each keyword
    all_keywords_ordered = []
    keyword_seen = set()
    keyword_to_rows = defaultdict(set)
    skipped = 0

    for idx, facet in enumerate(facet_rows):
        raw = facet["facet_value"]
        parsed = parse_sic_sod(raw)
        has_sic_sod = bool(parsed["sic"] and parsed["sod"])

        if has_sic_sod:
            before_text = parsed["sod"].lower()
            after_text = parsed["sic"].lower()
        else:
            cleaned = clean_facet_value(raw)
            if not cleaned:
                cleaned = parsed["sod"] or parsed["sic"] or parsed["plain"] or ""
            if not cleaned:
                skipped += 1
                continue
            before_text = cleaned.lower()
            after_text = cleaned.lower()

        for cat in mc_cats:
            forms = get_category_forms(cat["deepest_cat"])
            for form in forms:
                form = form.strip().lower()
                if not form:
                    continue
                for combo in [f"{before_text} {form}", f"{form} {after_text}"]:
                    keyword_to_rows[combo].add(idx)
                    if combo not in keyword_seen:
                        keyword_seen.add(combo)
                        all_keywords_ordered.append(combo)

    print(f"[FACET_VOLUMES] Maincat '{maincat_name}': {len(facet_rows)} facet values × "
          f"{len(mc_cats)} deepest cats = {len(all_keywords_ordered)} unique keywords "
          f"(skipped {skipped} empty values)")

    # Look up search volumes
    sv_result = get_search_volumes(all_keywords_ordered)

    # Build normalized volume lookup
    volume_lookup = {}
    for r in sv_result.get("results", []):
        norm = _normalize_keyword(r.get("normalized_keyword", ""))
        vol = r.get("search_volume", 0)
        volume_lookup[norm] = max(volume_lookup.get(norm, 0), vol)

    # Sum volumes per facet row
    row_volumes = [0] * len(facet_rows)
    for combo, row_indices in keyword_to_rows.items():
        norm = _normalize_keyword(combo)
        vol = volume_lookup.get(norm, 0)
        for idx in row_indices:
            row_volumes[idx] += vol

    # Build output: input CSV columns + search_volume
    results = []
    for idx, row in enumerate(facet_rows):
        results.append({
            "main_category": row["main_category"],
            "main_category_id": row["main_category_id"],
            "facet_name": row["facet_name"],
            "bucket": row["bucket"],
            "url_name": row["url_name"],
            "id": row["id"],
            "facet_value": row["facet_value"],
            "search_volume": row_volumes[idx],
        })

    grand_total = sum(row_volumes)

    return {
        "results": results,
        "stats": {
            "facet_values": len(facet_rows),
            "skipped": skipped,
            "deepest_cats": len(mc_cats),
            "unique_keywords": len(all_keywords_ordered),
            "batches": sv_result.get("batches_used", 0),
            "customer_ids_used": sv_result.get("customer_ids_used", 0),
        },
        "grand_total": grand_total,
    }
