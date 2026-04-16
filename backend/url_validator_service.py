"""
URL Validator Service

Validates beslist.nl category/facet URLs against the Taxonomy API v2
without crawling the live site. Checks:
  - URL structure (double /products/, double maincats, query params, etc.)
  - Category exists and is enabled
  - Facets are linked to the category
  - Facet values exist and have seoPriority
"""

import csv
import re
import requests
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

# Taxonomy API
TAX_BASE = "http://producttaxonomyunifiedapi-prod.azure.api.beslist.nl"
TAX_HEADERS = {"X-User-Name": "SEO_JOEP", "Accept": "application/json"}
TAX_TIMEOUT = 30

# Data files
MAINCAT_CSV = Path(__file__).parent / "maincat_mapping.csv"
CAT_URLS_CSV = Path(__file__).parent / "data" / "cat_urls.csv"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class ValidationIssue:
    severity: str   # "error", "warning", "info"
    code: str
    message: str
    component: str = ""  # which part of the URL triggered it


@dataclass
class ParsedUrl:
    raw: str
    path: str = ""
    maincat_slug: str = ""
    subcat_slug: str = ""
    facets: List[Tuple[str, str]] = field(default_factory=list)  # (facet_slug, value_id)
    query_params: Dict = field(default_factory=dict)
    fragment: str = ""
    structural_errors: List[ValidationIssue] = field(default_factory=list)


@dataclass
class ValidationResult:
    url: str
    status: str = "valid"          # valid / warning / error
    maincat_name: str = ""
    category_name: str = ""
    facets_valid: int = 0
    facets_total: int = 0
    issues: List[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Taxonomy cache  (singleton, lazy-loaded, TTL 1 hour)
# ---------------------------------------------------------------------------
class TaxonomyCache:
    TTL = 3600  # seconds

    def __init__(self):
        # CSV-based lookups
        self._maincat_by_slug: Dict[str, dict] = {}   # slug -> {id, name, url}
        self._cat_by_slug: Dict[str, dict] = {}        # slug -> {cat_id, maincat, deepest_cat}
        # API-based lookups (lazily populated)
        self._category_detail: Dict[int, dict] = {}    # cat_id -> API detail
        self._category_facets: Dict[int, list] = {}    # cat_id -> [facet dicts]
        self._facet_values: Dict[int, Dict[int, dict]] = {}  # facet_id -> {value_id -> value dict}
        self._facet_by_slug: Dict[str, dict] = {}      # facet urlSlug -> facet dict (global)
        self._last_csv_load: float = 0
        self._session = requests.Session()

    # --- CSV loading ---
    def _ensure_csv_loaded(self):
        if time.time() - self._last_csv_load < self.TTL and self._maincat_by_slug:
            return
        self._load_csvs()

    def _load_csvs(self):
        # maincat_mapping.csv  (maincat;maincat_url;maincat_id)
        self._maincat_by_slug.clear()
        if MAINCAT_CSV.exists():
            with open(MAINCAT_CSV, encoding="utf-8") as f:
                for row in csv.DictReader(f, delimiter=";"):
                    slug = row["maincat_url"].strip("/")
                    self._maincat_by_slug[slug] = {
                        "id": int(row["maincat_id"]),
                        "name": row["maincat"],
                        "slug": slug,
                    }

        # cat_urls.csv  (maincat;deepest_cat;url_name;cat_id)
        self._cat_by_slug.clear()
        if CAT_URLS_CSV.exists():
            with open(CAT_URLS_CSV, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    slug = row.get("url_name", "").strip("/")
                    if not slug:
                        continue
                    self._cat_by_slug[slug] = {
                        "cat_id": int(row.get("cat_id", 0)),
                        "maincat": row.get("maincat", ""),
                        "deepest_cat": row.get("deepest_cat", ""),
                        "slug": slug,
                    }

        self._last_csv_load = time.time()

    # --- API helpers ---
    def _api_get(self, path: str, params: dict = None) -> Optional[dict]:
        try:
            r = self._session.get(
                f"{TAX_BASE}{path}", headers=TAX_HEADERS,
                params=params or {}, timeout=TAX_TIMEOUT,
            )
            if r.status_code == 200:
                return r.json()
            return None
        except Exception as e:
            print(f"[URL_VALIDATOR] API error {path}: {e}")
            return None

    # --- Public lookups ---
    def get_maincat(self, slug: str) -> Optional[dict]:
        self._ensure_csv_loaded()
        return self._maincat_by_slug.get(slug)

    def get_category(self, slug: str) -> Optional[dict]:
        self._ensure_csv_loaded()
        return self._cat_by_slug.get(slug)

    def get_category_detail(self, cat_id: int) -> Optional[dict]:
        if cat_id in self._category_detail:
            return self._category_detail[cat_id]
        detail = self._api_get(f"/api/Categories/{cat_id}", {"locale": "nl-NL"})
        if detail:
            self._category_detail[cat_id] = detail
        return detail

    def get_category_facets(self, cat_id: int) -> List[dict]:
        """Return facets linked to a category (with urlSlugs from labels)."""
        if cat_id in self._category_facets:
            return self._category_facets[cat_id]
        data = self._api_get("/api/CategoryFacets", {"categoryId": cat_id, "locale": "nl-NL"})
        if data is None:
            return []
        facets = data if isinstance(data, list) else data.get("items", [])
        # Enrich with slug from labels
        result = []
        for link in facets:
            facet = link.get("facet", {})
            labels = facet.get("labels", [])
            nl = next((l for l in labels if l.get("locale") == "nl-NL"), {})
            facet_info = {
                "facet_id": facet.get("id"),
                "name": nl.get("name", ""),
                "slug": nl.get("urlSlug", ""),
                "enabled": facet.get("isEnabled", True),
                "noindex": facet.get("noIndexNoFollow", False),
            }
            result.append(facet_info)
            if facet_info["slug"]:
                self._facet_by_slug[facet_info["slug"]] = facet_info
        self._category_facets[cat_id] = result
        return result

    def get_facet_values(self, facet_id: int) -> Dict[int, dict]:
        """Return {value_id: {id, name, seoPriority}} for a facet."""
        if facet_id in self._facet_values:
            return self._facet_values[facet_id]
        page = 1
        all_vals: Dict[int, dict] = {}
        while True:
            data = self._api_get(
                f"/api/Facets/{facet_id}/values",
                {"locale": "nl-NL", "pageSize": 500, "page": page},
            )
            if not data:
                break
            items = data.get("items", data) if isinstance(data, dict) else data
            if not items:
                break
            for v in items:
                labels = v.get("labels", [])
                name = ""
                if labels:
                    name = labels[0].get("nameInColumn", "") or labels[0].get("nameOnDetail", "")
                all_vals[v["id"]] = {
                    "id": v["id"],
                    "name": name,
                    "seoPriority": v.get("seoPriority", False),
                }
            # Check if there's a next page
            total = data.get("totalCount", 0) if isinstance(data, dict) else 0
            if len(all_vals) >= total or len(items) < 500:
                break
            page += 1
        self._facet_values[facet_id] = all_vals
        return all_vals

    def clear(self):
        """Force cache reset."""
        self._category_detail.clear()
        self._category_facets.clear()
        self._facet_values.clear()
        self._facet_by_slug.clear()
        self._last_csv_load = 0

    def stats(self) -> dict:
        self._ensure_csv_loaded()
        return {
            "maincats_loaded": len(self._maincat_by_slug),
            "categories_loaded": len(self._cat_by_slug),
            "category_details_cached": len(self._category_detail),
            "category_facets_cached": len(self._category_facets),
            "facet_values_cached": len(self._facet_values),
        }


# Module-level singleton
_cache = TaxonomyCache()


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------
def parse_beslist_url(url: str) -> ParsedUrl:
    """Parse a beslist.nl category URL into components."""
    raw = url.strip()
    result = ParsedUrl(raw=raw)

    # Strip protocol + domain
    path = raw
    if "://" in path:
        parsed = urlparse(path)
        path = parsed.path
        result.query_params = parse_qs(parsed.query)
        result.fragment = parsed.fragment
    elif "?" in path:
        path, qs = path.split("?", 1)
        result.query_params = parse_qs(qs)

    if "#" in path:
        path, result.fragment = path.split("#", 1)

    # Strip /r/{bucket}/ segments (e.g. /r/2_delige/) — these are query/bucket
    # parts that should be ignored for validation
    path = re.sub(r'/r/[^/]+/', '/', path)
    path = path.replace('//', '/')

    result.path = path

    # Split on /c/ to separate category from facets
    if "/c/" in path:
        cat_part, facet_part = path.split("/c/", 1)
    else:
        cat_part = path
        facet_part = ""

    # Parse category path: /products/{maincat}/{subcat}/
    cat_part = cat_part.strip("/")
    segments = [s for s in cat_part.split("/") if s]

    if segments and segments[0] == "products":
        segments = segments[1:]

    if segments:
        result.maincat_slug = segments[0]
    if len(segments) > 1:
        result.subcat_slug = segments[1]

    # Parse facets: facet_slug~value_id~~facet_slug~value_id
    if facet_part:
        facet_part = facet_part.strip("/")
        pairs = facet_part.split("~~")
        for pair in pairs:
            if "~" in pair:
                parts = pair.split("~", 1)
                result.facets.append((parts[0], parts[1]))
            elif pair:
                result.facets.append((pair, ""))

    return result


# ---------------------------------------------------------------------------
# Structural validation
# ---------------------------------------------------------------------------
def check_structural_errors(url: str, parsed: ParsedUrl) -> List[ValidationIssue]:
    """Check for structural URL problems without hitting the API."""
    issues: List[ValidationIssue] = []
    path = parsed.path
    raw = parsed.raw.lower()

    # Double /products/products/
    if "/products/products/" in raw:
        issues.append(ValidationIssue("error", "DOUBLE_PRODUCTS",
                                      "URL contains /products/products/ — double prefix", "path"))

    # Double maincat slug (e.g. /schoenen/schoenen_...)
    if parsed.maincat_slug and parsed.subcat_slug:
        mc = parsed.maincat_slug.lower()
        sc = parsed.subcat_slug.lower()
        # Check if subcat starts with maincat repeated (e.g. schoenen/schoenen_...)
        # This is NORMAL: /products/schoenen/schoenen_123/ — maincat repeated in subcat slug
        # What's NOT normal: /products/schoenen/schoenen/ (exact duplicate, no cat id)
        if sc == mc:
            issues.append(ValidationIssue("error", "DOUBLE_MAINCAT",
                                          f"Subcategory slug '{sc}' is an exact duplicate of maincat '{mc}'",
                                          "category"))

    # Missing /products/ prefix
    if not path.startswith("/products/") and "products" not in path[:20].lower():
        issues.append(ValidationIssue("error", "MISSING_PRODUCTS_PREFIX",
                                      "URL path doesn't start with /products/", "path"))

    # Query parameters
    if parsed.query_params:
        param_names = list(parsed.query_params.keys())
        tracking = [p for p in param_names if p.startswith(("utm_", "gclid", "fbclid", "gad_source"))]
        if tracking:
            issues.append(ValidationIssue("warning", "TRACKING_PARAMS",
                                          f"Contains tracking parameters: {', '.join(tracking)}", "query"))
        other = [p for p in param_names if p not in tracking]
        if other:
            issues.append(ValidationIssue("warning", "QUERY_PARAMS",
                                          f"Contains query parameters: {', '.join(other)}", "query"))

    # Fragment
    if parsed.fragment:
        issues.append(ValidationIssue("warning", "FRAGMENT_PRESENT",
                                      f"Contains fragment: #{parsed.fragment}", "fragment"))

    # Malformed facet pairs
    for facet_slug, value_id in parsed.facets:
        if not facet_slug:
            issues.append(ValidationIssue("error", "EMPTY_FACET_SLUG",
                                          "Empty facet name in facet~value pair", "facets"))
        if not value_id:
            issues.append(ValidationIssue("error", "EMPTY_FACET_VALUE",
                                          f"Empty value for facet '{facet_slug}'", "facets"))

    # Duplicate facet names
    facet_names = [f[0] for f in parsed.facets if f[0]]
    seen = set()
    for fn in facet_names:
        if fn in seen:
            issues.append(ValidationIssue("warning", "DUPLICATE_FACET",
                                          f"Facet '{fn}' appears more than once in URL", "facets"))
        seen.add(fn)

    # Spaces or special chars in path
    if " " in parsed.path or "+" in parsed.path:
        issues.append(ValidationIssue("error", "INVALID_CHARS",
                                      "URL path contains spaces or '+' characters", "path"))

    # Uppercase in path (beslist URLs should be lowercase)
    path_to_check = parsed.path
    if path_to_check != path_to_check.lower():
        issues.append(ValidationIssue("warning", "UPPERCASE_IN_PATH",
                                      "URL path contains uppercase characters", "path"))

    # /r/ bucket in path (redirect/bucket segment)
    if "/r/" in parsed.path:
        issues.append(ValidationIssue("info", "HAS_BUCKET",
                                      "URL contains /r/ bucket segment", "path"))

    # /page_ pagination
    if "/page_" in raw or "page_" in raw:
        issues.append(ValidationIssue("warning", "PAGINATION",
                                      "URL contains pagination parameter", "path"))

    # sortby, device, shop_id parameters
    for param in ["sortby", "device", "shop_id"]:
        if param in raw:
            issues.append(ValidationIssue("warning", "UNWANTED_PARAM",
                                          f"URL contains '{param}' — likely not a canonical URL", "query"))

    return issues


# ---------------------------------------------------------------------------
# Taxonomy validation
# ---------------------------------------------------------------------------
def validate_against_taxonomy(parsed: ParsedUrl) -> Tuple[List[ValidationIssue], str, str, int, int]:
    """
    Validate URL components against Taxonomy API.
    Returns: (issues, maincat_name, category_name, facets_valid, facets_total)
    """
    issues: List[ValidationIssue] = []
    maincat_name = ""
    category_name = ""
    facets_valid = 0
    facets_total = len(parsed.facets)
    cat_id = None

    # 1. Validate maincat
    if parsed.maincat_slug:
        mc = _cache.get_maincat(parsed.maincat_slug)
        if mc:
            maincat_name = mc["name"]
        else:
            issues.append(ValidationIssue("error", "MAINCAT_NOT_FOUND",
                                          f"Maincat slug '{parsed.maincat_slug}' not found in taxonomy",
                                          "maincat"))
            return issues, maincat_name, category_name, facets_valid, facets_total
    else:
        issues.append(ValidationIssue("error", "NO_MAINCAT",
                                      "No maincat slug found in URL", "maincat"))
        return issues, maincat_name, category_name, facets_valid, facets_total

    # 2. Validate category
    if parsed.subcat_slug:
        cat = _cache.get_category(parsed.subcat_slug)
        if cat:
            category_name = cat["deepest_cat"]
            cat_id = cat["cat_id"]
            # Check hierarchy: cat's maincat should match
            if cat["maincat"] != maincat_name:
                issues.append(ValidationIssue("error", "HIERARCHY_MISMATCH",
                                              f"Category '{category_name}' belongs to '{cat['maincat']}', "
                                              f"not '{maincat_name}'", "category"))
        else:
            issues.append(ValidationIssue("error", "CATEGORY_NOT_FOUND",
                                          f"Category slug '{parsed.subcat_slug}' not found in taxonomy",
                                          "category"))
            return issues, maincat_name, category_name, facets_valid, facets_total
    else:
        # URL is at maincat level (e.g., /products/schoenen/c/merk~123)
        mc = _cache.get_maincat(parsed.maincat_slug)
        if mc:
            cat_id = mc["id"]
            category_name = f"{maincat_name} (maincat level)"

    # 3. Check category enabled status via API
    if cat_id:
        detail = _cache.get_category_detail(cat_id)
        if detail:
            if not detail.get("isEnabled", True):
                issues.append(ValidationIssue("error", "CATEGORY_DISABLED",
                                              f"Category '{category_name}' (id={cat_id}) is disabled",
                                              "category"))
        else:
            issues.append(ValidationIssue("warning", "CATEGORY_API_UNAVAILABLE",
                                          f"Could not verify category {cat_id} via API", "category"))

    # 4. Validate facets
    if not parsed.facets or not cat_id:
        return issues, maincat_name, category_name, facets_valid, facets_total

    category_facets = _cache.get_category_facets(cat_id)
    facet_slug_map = {f["slug"]: f for f in category_facets if f.get("slug")}

    for facet_slug, value_str in parsed.facets:
        if not facet_slug or not value_str:
            continue

        # Look up facet by slug
        facet_info = facet_slug_map.get(facet_slug)
        if not facet_info:
            issues.append(ValidationIssue("error", "FACET_NOT_LINKED",
                                          f"Facet '{facet_slug}' is not linked to category '{category_name}'",
                                          "facets"))
            continue

        # Check facet enabled
        if not facet_info.get("enabled", True):
            issues.append(ValidationIssue("warning", "FACET_DISABLED",
                                          f"Facet '{facet_slug}' ({facet_info['name']}) is disabled",
                                          "facets"))

        # Check facet noindex
        if facet_info.get("noindex", False):
            issues.append(ValidationIssue("info", "FACET_NOINDEX",
                                          f"Facet '{facet_slug}' ({facet_info['name']}) has noIndexNoFollow=true",
                                          "facets"))

        # Check facet value
        try:
            value_id = int(value_str)
        except ValueError:
            issues.append(ValidationIssue("error", "INVALID_VALUE_ID",
                                          f"Facet value '{value_str}' for '{facet_slug}' is not a numeric ID",
                                          "facets"))
            continue

        values = _cache.get_facet_values(facet_info["facet_id"])
        val = values.get(value_id)
        if not val:
            issues.append(ValidationIssue("error", "VALUE_NOT_FOUND",
                                          f"Value {value_id} not found in facet '{facet_slug}' ({facet_info['name']})",
                                          "facets"))
            continue

        # Check seoPriority
        if not val.get("seoPriority", False):
            issues.append(ValidationIssue("info", "VALUE_LOW_SEO_PRIORITY",
                                          f"Value {value_id} ('{val['name']}') in facet '{facet_slug}' "
                                          f"has seoPriority=false", "facets"))

        facets_valid += 1

    return issues, maincat_name, category_name, facets_valid, facets_total


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def validate_urls(urls: List[str]) -> dict:
    """Validate a batch of URLs. Returns summary + per-URL results."""
    _cache._ensure_csv_loaded()

    results = []
    total_valid = 0
    total_warnings = 0
    total_errors = 0

    for url in urls:
        url = url.strip()
        if not url:
            continue

        parsed = parse_beslist_url(url)

        # Structural checks
        structural = check_structural_errors(url, parsed)

        # Taxonomy checks
        tax_issues, mc_name, cat_name, fv, ft = validate_against_taxonomy(parsed)

        all_issues = structural + tax_issues

        # Determine overall status
        has_error = any(i.severity == "error" for i in all_issues)
        has_warning = any(i.severity == "warning" for i in all_issues)

        if has_error:
            status = "error"
            total_errors += 1
        elif has_warning:
            status = "warning"
            total_warnings += 1
        else:
            status = "valid"
            total_valid += 1

        results.append(ValidationResult(
            url=url,
            status=status,
            maincat_name=mc_name,
            category_name=cat_name,
            facets_valid=fv,
            facets_total=ft,
            issues=[{"severity": i.severity, "code": i.code,
                     "message": i.message, "component": i.component}
                    for i in all_issues],
        ))

    return {
        "total": len(results),
        "valid": total_valid,
        "warnings": total_warnings,
        "errors": total_errors,
        "results": [vars(r) for r in results],
    }


def get_cache_stats() -> dict:
    return _cache.stats()


def clear_cache():
    _cache.clear()
    return {"status": "ok", "message": "Cache cleared"}
