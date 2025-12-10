#!/usr/bin/env python3
"""
SEO Content Generator from Product Search API.

Takes URLs from seo_urls file, extracts maincat_id and filters,
queries the Product Search API for product descriptions,
and generates SEO content using GPT.
"""

import csv
import re
import requests
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from urllib.parse import quote

# Configuration
PRODUCT_SEARCH_API = "https://productsearch-v2.api.beslist.nl/search/products"
MAINCAT_MAPPING_FILE = Path(__file__).parent / "maincat_mapping.csv"

# Cache for maincat mapping
_maincat_mapping_cache: Optional[Dict[str, str]] = None


def load_maincat_mapping(filepath: Path = MAINCAT_MAPPING_FILE) -> Dict[str, str]:
    """Load maincat URL to maincat_id mapping from CSV file."""
    global _maincat_mapping_cache

    if _maincat_mapping_cache is not None:
        return _maincat_mapping_cache

    mapping = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            # maincat_url contains values like /gezond_mooi/
            url_part = row['maincat_url'].strip('/')  # Remove leading/trailing slashes
            maincat_id = row['maincat_id']
            mapping[url_part] = maincat_id

    _maincat_mapping_cache = mapping
    return mapping


def parse_seo_url(url: str, maincat_mapping: Dict[str, str]) -> Optional[Dict]:
    """
    Parse a SEO URL to extract maincat_id, category, and filters.

    URL format: https://www.beslist.nl/products/{maincat}/{category}/c/{filters}
    Example: https://www.beslist.nl/products/accessoires/accessoires_2596345/c/kleur~2596368~~merk~2685973

    Returns dict with:
    - maincat_id: The maincat ID from mapping
    - category: The category string (e.g., "accessoires_2596345")
    - filters: Dict of filter_name -> [filter_values]
    """
    # Remove base URL if present
    if url.startswith('https://www.beslist.nl'):
        url = url.replace('https://www.beslist.nl', '')

    # Parse URL structure: /products/{maincat}/{category}/c/{filters}
    # or: /products/{maincat}/c/{filters} (without subcategory)
    match = re.match(r'^/products/([^/]+)/([^/]+)/c/(.+)$', url)
    if match:
        maincat_name = match.group(1)
        category = match.group(2)
        filters_str = match.group(3)
    else:
        # Try pattern without subcategory: /products/{maincat}/c/{filters}
        match = re.match(r'^/products/([^/]+)/c/(.+)$', url)
        if not match:
            return None
        maincat_name = match.group(1)
        category = maincat_name  # Use maincat as category
        filters_str = match.group(2)

    # Look up maincat_id
    maincat_id = maincat_mapping.get(maincat_name)
    if not maincat_id:
        print(f"[SEO_GENERATOR] Warning: Could not find maincat_id for '{maincat_name}'")
        return None

    # Parse filters: format is filter_name~value~~filter_name2~value2
    # Multiple values for same filter: kleur~2596368~~kleur~123456
    filters: Dict[str, List[str]] = {}
    filter_pairs = filters_str.split('~~')
    for pair in filter_pairs:
        if '~' in pair:
            parts = pair.split('~', 1)
            filter_name = parts[0]
            filter_value = parts[1] if len(parts) > 1 else ''
            if filter_name not in filters:
                filters[filter_name] = []
            filters[filter_name].append(filter_value)

    return {
        'maincat_id': maincat_id,
        'category': category,
        'filters': filters,
        'original_url': url
    }


def build_api_url(parsed_url: Dict, limit: int = 30) -> str:
    """
    Build the Product Search API URL from parsed URL components.

    API format:
    https://productsearch-v2.api.beslist.nl/search/products?
        query=&
        filters[merk][0]=100052&
        mainCategory=655&
        category=elektronica_19875536_19934132&
        sort=popularity&
        sortDirection=desc&
        limit=30&
        offset=0&
        isBot=false&
        countryLanguage=nl-nl&
        experiment=topProducts&
        trackTotalHits=false
    """
    params = [
        f"query=",
        f"mainCategory={parsed_url['maincat_id']}",
        f"category={parsed_url['category']}",
        "sort=popularity",
        "sortDirection=desc",
        "limit=30",
        "offset=0",
        "isBot=true",
        "countryLanguage=nl-nl",
        "experiment=topProducts",
        "trackTotalHits=false"
    ]

    # Add filters
    for filter_name, filter_values in parsed_url['filters'].items():
        for i, value in enumerate(filter_values):
            params.append(f"filters%5B{filter_name}%5D%5B{i}%5D={value}")

    return f"{PRODUCT_SEARCH_API}?" + "&".join(params)


def fetch_products(api_url: str) -> List[Dict]:
    """
    Fetch products from the Product Search API.
    Returns list of product dicts with title, description, url, etc.

    API response structure:
    {
        "products": [
            {
                "id": "...",
                "brandName": "...",
                "description": "...",
                "title": "Accu - Asus - Telefoonaccu - Zwart - Lithium-ion",
                "plpUrl": "/p/accu-asus-telefoonaccu-zwart-lithium-ion/40000/4894128108528/",
                "minPrice": 17.2,
                ...
            }
        ]
    }
    """
    headers = {
        'accept': 'application/json'
    }

    try:
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        products = []
        # API returns 'products' array
        api_products = data.get('products', [])
        for p in api_products:
            # Use plpUrl and title directly from API response
            plp_url = p.get('plpUrl', '')
            title = p.get('title', '')

            # Fallback if plpUrl not available
            if not plp_url and p.get('id'):
                categories = p.get('categories', [])
                if categories:
                    deepest = max(categories, key=lambda c: c.get('depth', 0))
                    url_name = deepest.get('urlName', '')
                    plp_url = f"/p/{url_name}/{p['id']}/"

            # Fallback for title if not available
            if not title:
                title = p.get('brandName', '') + ' ' + (p.get('description', '')[:50] if p.get('description') else '')

            product = {
                'title': title,  # Use API title for anchor text
                'description': p.get('description', ''),
                'listviewContent': p.get('description', ''),
                'url': plp_url,  # Use API plpUrl for links
                'price': p.get('minPrice', 0),
                'brand': p.get('brandName', ''),
                'pimId': p.get('pimId', p.get('id', ''))
            }
            products.append(product)

        return products
    except Exception as e:
        print(f"[SEO_GENERATOR] Error fetching products: {e}")
        import traceback
        traceback.print_exc()
        return []


def extract_h1_from_url(url: str) -> str:
    """
    Extract a reasonable H1 title from the URL.
    Converts category like 'accessoires_2596345' to 'Accessoires'
    and includes filter information.
    """
    parsed = parse_seo_url(url, load_maincat_mapping())
    if not parsed:
        return "Producten"

    # Get category name (first part before underscore and number)
    category = parsed['category']
    # Extract readable name from category like "accessoires_2596345_3541068"
    parts = category.split('_')
    category_name = parts[0].replace('-', ' ').title()

    return category_name


def generate_content_from_descriptions(products: List[Dict], h1_title: str) -> str:
    """
    Generate SEO content using GPT based on product descriptions.
    Uses the same prompt structure as gpt_service.py.
    """
    from backend.gpt_service import generate_product_content

    # Prepare products in the format expected by generate_product_content
    formatted_products = []
    for p in products[:30]:  # Limit to 30 products
        formatted_products.append({
            'title': p['title'],
            'url': p['url'],
            'listviewContent': p['description'] or p['listviewContent'] or p['title']
        })

    return generate_product_content(h1_title, formatted_products)


def process_seo_url(url: str) -> Dict:
    """
    Process a single SEO URL: parse, fetch products, generate content.

    Returns dict with:
    - url: Original URL
    - maincat_id: Maincat ID
    - category: Category string
    - filters: Parsed filters
    - api_url: Built API URL
    - products_count: Number of products found
    - content: Generated SEO content
    - success: Whether processing was successful
    - error: Error message if failed
    """
    result = {
        'url': url,
        'maincat_id': None,
        'category': None,
        'filters': None,
        'api_url': None,
        'products_count': 0,
        'content': None,
        'success': False,
        'error': None
    }

    maincat_mapping = load_maincat_mapping()

    # Parse URL
    parsed = parse_seo_url(url, maincat_mapping)
    if not parsed:
        result['error'] = f"Could not parse URL: {url}"
        return result

    result['maincat_id'] = parsed['maincat_id']
    result['category'] = parsed['category']
    result['filters'] = parsed['filters']

    # Build API URL
    api_url = build_api_url(parsed, limit=30)
    result['api_url'] = api_url

    # Fetch products
    print(f"[SEO_GENERATOR] Fetching products from API...")
    products = fetch_products(api_url)
    result['products_count'] = len(products)

    if not products:
        result['error'] = "No products returned from API"
        return result

    # Generate H1 title
    h1_title = extract_h1_from_url(url)
    print(f"[SEO_GENERATOR] Generating content for '{h1_title}' with {len(products)} products...")

    # Generate content
    try:
        content = generate_content_from_descriptions(products, h1_title)
        result['content'] = content
        result['success'] = True
    except Exception as e:
        result['error'] = f"Error generating content: {e}"

    return result


def process_seo_urls_file(filepath: str, limit: int = None) -> List[Dict]:
    """
    Process multiple SEO URLs from a file.

    Args:
        filepath: Path to file containing URLs (one per line)
        limit: Maximum number of URLs to process (None for all)

    Returns:
        List of result dicts from process_seo_url()
    """
    urls = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            url = line.strip()
            if url:
                urls.append(url)
                if limit and len(urls) >= limit:
                    break

    print(f"[SEO_GENERATOR] Processing {len(urls)} URLs...")

    results = []
    for i, url in enumerate(urls):
        print(f"\n[SEO_GENERATOR] Processing URL {i+1}/{len(urls)}: {url[:80]}...")
        result = process_seo_url(url)
        results.append(result)

        if result['success']:
            print(f"[SEO_GENERATOR] Success! Generated content ({len(result['content'])} chars)")
        else:
            print(f"[SEO_GENERATOR] Failed: {result['error']}")

    return results


# Test function
if __name__ == "__main__":
    import sys

    # Default: process first 2 URLs from seo_urls file
    seo_urls_file = Path(__file__).parent.parent / "seo_urls"
    limit = 2

    if len(sys.argv) > 1:
        limit = int(sys.argv[1])

    print(f"Processing first {limit} URLs from {seo_urls_file}...")
    results = process_seo_urls_file(str(seo_urls_file), limit=limit)

    print("\n" + "="*80)
    print("RESULTS")
    print("="*80)

    for i, result in enumerate(results):
        print(f"\n--- URL {i+1} ---")
        print(f"URL: {result['url']}")
        print(f"Maincat ID: {result['maincat_id']}")
        print(f"Category: {result['category']}")
        print(f"Filters: {result['filters']}")
        print(f"Products found: {result['products_count']}")
        print(f"Success: {result['success']}")

        if result['content']:
            print(f"\nGenerated Content:\n{result['content']}")
        elif result['error']:
            print(f"\nError: {result['error']}")
