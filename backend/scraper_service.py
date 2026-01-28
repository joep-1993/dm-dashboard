import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Tuple
import re
import time
import random
from urllib.parse import urlencode
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# User agent - Custom identifier for Beslist scraper
USER_AGENT = "Beslist script voor SEO"

# Create a persistent session with retry logic
def create_session():
    """Create a requests session with retry logic and connection pooling"""
    session = requests.Session()

    # Configure retries
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session

# Global session for connection reuse
_session = create_session()

def get_scraper_ip() -> Optional[str]:
    """Get the IP address used by the scraper for outbound requests"""
    try:
        response = requests.get("https://api.ipify.org?format=json", timeout=5)
        if response.status_code == 200:
            return response.json().get("ip")
    except Exception as e:
        print(f"Failed to get scraper IP: {str(e)}")
    return None

def clean_url(url: str) -> str:
    """Remove query parameters from URL"""
    return url.split("?")[0] if url else ""

def is_valid_url(url: str) -> bool:
    """Check if URL is valid (not empty, not #, not javascript:)"""
    if not url or not isinstance(url, str):
        return False
    url = url.strip().lower()
    if not url or url == "#" or url.startswith("javascript:"):
        return False
    return True

def scrape_product_page(url: str, conservative_mode: bool = False) -> Optional[Dict]:
    """
    Scrape a product listing page and extract:
    - h1 title
    - list of products (title, url, description)

    Args:
        url: URL to scrape
        conservative_mode: If True, use conservative rate (max 2 URLs/sec). Default: False (optimized rate)

    Returns:
        - Dict with scraped data on success
        - Dict with {'error': '503'} if rate limited (503 error)
        - None for other failures (timeout, network error, etc)
    """
    try:
        # Clean URL first
        clean = clean_url(url)

        # Select delay based on mode
        if conservative_mode:
            # Conservative mode: max 2 URLs per second (0.5-0.7 second delay)
            delay = 0.5 + random.uniform(0, 0.2)
        else:
            # Optimized delay based on rate limit testing (0.2-0.3 second)
            # Testing showed no rate limiting even at faster rates - user-agent appears whitelisted
            delay = 0.2 + random.uniform(0, 0.1)

        time.sleep(delay)

        # Make HTTP request with browser-like headers using persistent session
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none"
        }
        response = _session.get(clean, headers=headers, timeout=30)

        # Handle 202 (Cloudflare queuing) - should be rare with whitelisted IP
        # Only retry once with shorter wait time
        if response.status_code == 202:
            print(f"Got 202 for {clean}, retrying after 2s...")
            time.sleep(2)
            response = _session.get(clean, headers=headers, timeout=30)

        # Check status code
        if response.status_code != 200:
            status_msg = {
                403: "Access denied (403 Forbidden)",
                503: "Service unavailable (503)",
                500: "Server error (500)",
                502: "Bad gateway (502)",
                504: "Gateway timeout (504)"
            }.get(response.status_code, f"HTTP error ({response.status_code})")
            print(f"Scraping failed: {status_msg} for {clean}")
            # Return special indicator for 503 errors (rate limiting)
            if response.status_code == 503:
                return {'error': '503'}
            return None

        # Check for hidden 503 errors in HTML body (Beslist.nl returns 200 with 503 message)
        # This happens when rate limited - we should retry later, not mark as "no products"
        # Use more specific checks to avoid false positives from URLs/IDs containing "503"
        response_lower = response.text.lower()
        if 'service unavailable' in response_lower or '503 service' in response_lower or 'error 503' in response_lower:
            print(f"Scraping failed: Hidden 503 (rate limited) for {clean}")
            return {'error': '503'}

        # Parse HTML with lxml (2-3x faster than html.parser)
        soup = BeautifulSoup(response.text, 'lxml')

        # Extract h1 title
        h1_element = soup.select_one("h1.productsTitle--tHP5S")
        h1_title = h1_element.get_text(strip=True) if h1_element else "No Title Found"

        # Check if this is a grouped page (contains FacetValueV2)
        is_grouped = "FacetValueV2" in response.text

        # Extract product containers
        product_containers = soup.select("div.product--WiTVr")
        products = []

        for i, container in enumerate(product_containers[:70]):  # Max 70 as in n8n workflow
            # Extract title
            title_element = container.select_one("h2.product_title--eQD3J")
            title = title_element.get_text(strip=True) if title_element else "No Title"

            # Extract description - if not present, use title as fallback
            desc_element = container.select_one("div.productInfo__description--S1odY")
            listview_content = desc_element.get_text(strip=True) if desc_element else title

            # Extract product URL from <a> tag with class productLink--zqrcp
            link_element = container.select_one("a.productLink--zqrcp")
            product_url = ""
            if link_element and link_element.get("href"):
                href = link_element.get("href")
                # Make absolute URL if relative
                if href.startswith("/"):
                    product_url = "https://www.beslist.nl" + href
                else:
                    product_url = href

            # Only add if both URL and content are valid
            if is_valid_url(product_url) and listview_content:
                products.append({
                    "title": title,
                    "url": product_url,
                    "listviewContent": listview_content
                })

        return {
            "url": clean,
            "h1_title": h1_title,
            "products": products,
            "is_grouped": is_grouped
        }

    except requests.RequestException as e:
        print(f"Request error for {url}: {str(e)}")
        return None
    except Exception as e:
        print(f"Scraping error for {url}: {str(e)}")
        return None

def sanitize_content(content: str) -> str:
    """
    Sanitize HTML content for SQL insertion:
    - Escape single quotes
    - Decode HTML entities
    """
    if not content:
        return ""

    # Replace HTML entities
    sanitized = (content
        .replace("&amp;", "&")
        .replace("&quot;", '"')
        .replace("&apos;", "'")
        .replace("&#039;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ")
        .replace("&euro;", "€")
        .replace("&copy;", "©")
        .replace("&trade;", "™")
    )

    # Escape single quotes for SQL (double them)
    sanitized = sanitized.replace("'", "''")

    return sanitized


# Product Search API configuration
PRODUCT_SEARCH_API_URL = "https://productsearch-v2.api.beslist.nl/search/products"

# Mapping of mainCategory URL names to IDs (for Product Search API)
# Source: maincat_ids_new.xlsx - DO NOT EDIT without updating source file
MAIN_CATEGORY_IDS = {
    "autos": 37000,
    "baby_peuter": 8,
    "boeken": 701,
    "cadeaus_gadgets_culinair": 262,
    "computers": 6,
    "dieren_accessoires": 34000,
    "gezond_mooi": 286,
    "elektronica": 655,
    "voor_volwassenen": 452,
    "eten_drinken": 11,
    "fietsen": 38000,
    "films-series": 700,
    "cddvdrom": 4,
    "horloge": 30000,
    "huishoudelijke_apparatuur": 12000,
    "kantoorartikelen": 361,
    "mode": 137,
    "klussen": 35000,
    "meubilair": 10,
    "mode_accessoires": 33000,
    "accessoires": 40000,
    "muziekinstrument": 31000,
    "parfum_aftershave": 29000,
    "main_sanitair": 27000,
    "schoenen": 32000,
    "sieraden_horloges": 347,
    "software": 155,
    "speelgoed_spelletjes": 332,
    "sport_outdoor_vrije-tijd": 206,
    "tuin_accessoires": 36000,
    "huis_tuin": 165,
}


def parse_beslist_url(url: str) -> Tuple[Optional[str], Optional[str], Dict[str, List[str]]]:
    """
    Parse a Beslist.nl URL and extract category and filter information.

    URL formats supported:
    - /products/{maincat}/{category}/c/{filters}  (with filters)
    - /products/{maincat}/{category}/             (without filters)
    - /products/{maincat}/c/{filters}             (top-level with filters)
    - /products/{maincat}/                        (top-level without filters)

    Filters format: facet1~value1~~facet2~value2

    Returns:
        Tuple of (main_category_name, category_urlname, filters_dict)
        filters_dict maps facet names to list of filter value IDs
    """
    # Remove domain if present
    if url.startswith("http"):
        url = "/" + url.split("/", 3)[-1]

    # Remove trailing slash for consistent parsing
    url = url.rstrip("/")

    # Pattern 1: /products/{maincat}/{category}/c/{filters}
    # or: /products/{maincat}/c/{filters}
    match = re.match(r'^/products/([^/]+)(?:/([^/]+))?/c/(.+)$', url)

    if match:
        main_category = match.group(1)
        category = match.group(2)  # May be None for top-level categories
        filters_str = match.group(3)

        # Parse filters: facet1~value1~~facet2~value2 or facet1~value1~~facet1~value2
        filters: Dict[str, List[str]] = {}
        if filters_str:
            # Split by ~~ to get individual filter pairs
            filter_pairs = filters_str.split("~~")
            for pair in filter_pairs:
                if "~" in pair:
                    facet_name, value_id = pair.split("~", 1)
                    if facet_name not in filters:
                        filters[facet_name] = []
                    filters[facet_name].append(value_id)

        return main_category, category, filters

    # Pattern 2: /products/{maincat}/{category} (without /c/ filters)
    # or: /products/{maincat}
    match = re.match(r'^/products/([^/]+)(?:/([^/]+))?$', url)

    if match:
        main_category = match.group(1)
        category = match.group(2)  # May be None for top-level categories
        return main_category, category, {}  # Empty filters dict

    return None, None, {}


def build_api_params(main_category: str, category: Optional[str], filters: Dict[str, List[str]]) -> Dict:
    """
    Build API query parameters from parsed URL components.
    """
    main_cat_id = MAIN_CATEGORY_IDS.get(main_category)
    if not main_cat_id:
        return {}

    params = {
        "mainCategory": main_cat_id,
        "sort": "popularity",
        "sortDirection": "desc",
        "limit": 76,
        "offset": 0,
        "isBot": "false",
        "countryLanguage": "nl-nl",
        "experiment": "topProducts",
        "trackTotalHits": "false",
    }

    # Add category if present
    if category:
        params["category"] = category

    # Add filters - API expects filters[facetName][index]=valueId
    for facet_name, value_ids in filters.items():
        for i, value_id in enumerate(value_ids):
            params[f"filters[{facet_name}][{i}]"] = value_id

    return params


def extract_selected_facets(api_response: Dict) -> List[Dict[str, str]]:
    """
    Extract selected facet values from API response.

    Returns list of dicts with:
    - facet_name: Name of the facet group (e.g., "Kleur", "Serie")
    - facet_value: Display value (e.g., "Geel")
    - detail_value: Value for content generation (e.g., "Gele" - Dutch adjective form)
    """
    selected = []

    facets = api_response.get("facets", [])
    for facet_group in facets:
        facet_name = facet_group.get("name", "")
        values = facet_group.get("values", [])

        for value in values:
            if value.get("selected", False):
                selected.append({
                    "facet_name": facet_name,
                    "facet_value": value.get("facetValue", ""),
                    "detail_value": value.get("detailValue", value.get("facetValue", ""))
                })

    return selected


def build_product_subject(selected_facets: List[Dict[str, str]], category_name: str = "") -> str:
    """
    Build a product subject/name from selected facet values.

    Logic:
    - Colors (Kleur) come first as adjectives (using detailValue which has proper Dutch form)
    - Product names/series come next
    - Other attributes (brand, target group) follow
    - Category name is appended when needed for context

    Category is added when:
    - Subject is just a brand name (e.g., "Garmin" → "Garmin Accu's")
    - Subject is just a color/material (e.g., "Zwarte" → "Zwarte Klimplantrekken")
    - Subject is brand + target group (e.g., "Nike Heren" → "Nike Heren sneakers")

    Category is NOT added when:
    - Subject contains a specific product/model/series name (e.g., "iPhone 15")
    - Subject contains a product type facet (e.g., "Pistonmachines")

    Example: [{"facet_name": "Kleur", "detail_value": "Gele"},
              {"facet_name": "Serie", "detail_value": "iPhone 15"}]
    Returns: "Gele iPhone 15"
    """
    if not selected_facets:
        return category_name  # Just return category if no facets

    # Categorize facets by type
    colors = []
    materials = []
    product_names = []  # Serie, Modelnaam, Type - these are specific product identifiers
    brands = []
    target_groups = []  # Doelgroep (Heren, Dames, etc.)
    other = []

    # Facet names that typically contain specific product/model names
    # When these are present, we don't need the category name
    product_name_facets = {"serie", "modelnaam", "modelnaam_mob", "model"}
    # Product type facets - these describe what the product IS
    product_type_facets = {"type", "type_koffiezetter", "t_klimplantrek"}
    # Facet names for colors
    color_facets = {"kleur", "kleurtint", "kleurtint_paars", "kleurtint_blauw", "kleurtint_groen"}
    # Facet names for materials
    material_facets = {"materiaal"}
    # Facet names for target groups
    target_group_facets = {"doelgroep", "doelgroep_schoenen", "doelgroep_mode"}
    # Facet names for brands
    brand_facets = {"merk"}

    has_specific_product = False  # Track if we have a specific product identifier

    for facet in selected_facets:
        facet_name_lower = facet["facet_name"].lower()
        detail_value = facet["detail_value"]

        if any(c in facet_name_lower for c in color_facets):
            colors.append(detail_value)
        elif any(m in facet_name_lower for m in material_facets):
            materials.append(detail_value)
        elif any(p in facet_name_lower for p in product_name_facets):
            product_names.append(detail_value)
            has_specific_product = True
        elif any(t in facet_name_lower for t in product_type_facets):
            product_names.append(detail_value)
            has_specific_product = True
        elif any(t in facet_name_lower for t in target_group_facets):
            target_groups.append(detail_value)
        elif any(b in facet_name_lower for b in brand_facets):
            brands.append(detail_value)
        else:
            other.append(detail_value)

    # Build subject: colors/materials first (as adjectives), then product names, then brands, then target groups
    parts = colors + materials + product_names + brands + target_groups + other

    # Decide whether to append category name
    # Add category when:
    # 1. No specific product name/type is present AND
    # 2. We only have generic facets (brand, color, material, target group)
    needs_category = (
        not has_specific_product and
        category_name and
        len(parts) > 0
    )

    if needs_category:
        # Convert category to lowercase for natural reading
        # e.g., "Sneakers" stays as is, but we want it at the end
        parts.append(category_name.lower())

    return " ".join(parts)


def scrape_product_page_api(url: str) -> Optional[Dict]:
    """
    Scrape a product listing page using the Product Search API.

    This method:
    1. Parses the URL to extract category and filter information
    2. Calls the Product Search API
    3. Extracts selected facet values and builds a product subject
    4. Returns product data with the fabricated subject

    Returns:
        Dict with:
        - url: Original URL
        - h1_title: Original page title (from breadcrumb/category)
        - product_subject: Fabricated subject from selected facets (e.g., "Gele iPhone 15")
        - products: List of products with title, url, listviewContent
        - is_grouped: Whether page has facet filters
        - selected_facets: Raw selected facet data
    """
    try:
        clean = clean_url(url)

        # Parse URL
        main_category, category, filters = parse_beslist_url(clean)

        if not main_category:
            print(f"[API] Could not parse URL: {clean}")
            return None

        # Build API parameters
        params = build_api_params(main_category, category, filters)

        if not params:
            print(f"[API] Unknown main category: {main_category}")
            return None

        # Minimal delay for API calls (internal API, less restrictive)
        time.sleep(0.02 + random.uniform(0, 0.03))

        # Make API request
        headers = {
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }

        response = _session.get(PRODUCT_SEARCH_API_URL, params=params, headers=headers, timeout=30)

        if response.status_code != 200:
            print(f"[API] Request failed with status {response.status_code} for {clean}")
            if response.status_code == 503:
                return {'error': '503'}
            return None

        data = response.json()

        # Extract selected facets
        selected_facets = extract_selected_facets(data)

        # Get category name from the deepest category level (for use in subject building)
        categories = data.get("products", [{}])[0].get("categories", []) if data.get("products") else []
        deepest_category_name = categories[-1].get("name", "") if categories else ""

        # Build product subject from selected facets, passing category for context when needed
        product_subject = build_product_subject(selected_facets, deepest_category_name)

        # Extract products
        products = []
        api_products = data.get("products", [])[:70]  # Max 70 products

        for idx, product in enumerate(api_products):
            # Skip orResult products - only include exact matches (type="result")
            product_type = product.get("type", "")
            if product_type == "orResult":
                continue

            title = product.get("title", product.get("description", "No Title"))[:100]
            description = product.get("description", title)[:150]
            shop_count = product.get("shopCount", 0)

            # Get plpUrl for product link
            plp_url = product.get("plpUrl", "")
            if plp_url and not plp_url.startswith("http"):
                plp_url = "https://www.beslist.nl" + plp_url

            # Include products if they have at least 2 shops (reliable availability)
            if plp_url and description and shop_count >= 2:
                products.append({
                    "title": title,
                    "url": plp_url,
                    "listviewContent": description
                })

        # Use product_subject as h1_title when facets are present, otherwise use category
        h1_title = product_subject if product_subject else (deepest_category_name if deepest_category_name else main_category)

        return {
            "url": clean,
            "h1_title": h1_title,
            "product_subject": product_subject,
            "products": products,
            "is_grouped": len(filters) > 0,
            "selected_facets": selected_facets
        }

    except requests.RequestException as e:
        print(f"[API] Request error for {url}: {str(e)}")
        return None
    except Exception as e:
        print(f"[API] Error for {url}: {str(e)}")
        return None
