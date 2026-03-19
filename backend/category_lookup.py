"""
Category lookup from cat_urls.csv.

Maps URL parts (e.g. "meubilair_389369_389525") to category names
(maincat="Meubels", deepest_cat="Loveseats") without relying on API product data.
"""

import csv
import os
from typing import Optional, Tuple

# Lookup dict: url_part -> (maincat, deepest_cat)
# e.g. "meubilair_389369" -> ("Meubels", "Bankstellen")
_URL_TO_CATEGORY: dict = {}

def _load():
    if _URL_TO_CATEGORY:
        return
    csv_path = os.path.join(os.path.dirname(__file__), "data", "cat_urls.csv")
    try:
        with open(csv_path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                url_name = row.get("url_name", "").strip("/")
                maincat = row.get("maincat", "")
                deepest_cat = row.get("deepest_cat", "")
                if url_name:
                    _URL_TO_CATEGORY[url_name] = (maincat, deepest_cat)
        print(f"[CategoryLookup] Loaded {len(_URL_TO_CATEGORY)} category mappings")
    except Exception as e:
        print(f"[CategoryLookup] Failed to load cat_urls.csv: {e}")


def lookup_category(main_category: Optional[str], category: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Look up category names from URL parts.

    Args:
        main_category: Main category URL part (e.g. "meubilair")
        category: Subcategory URL part (e.g. "meubilair_389369_389525"), or None for top-level

    Returns:
        (maincat_name, deepest_cat_name) e.g. ("Meubels", "Loveseats"),
        or (None, None) if not found.
    """
    _load()
    key = category if category else main_category
    if not key:
        return None, None
    result = _URL_TO_CATEGORY.get(key)
    if result:
        return result
    return None, None
