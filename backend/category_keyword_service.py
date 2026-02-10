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
