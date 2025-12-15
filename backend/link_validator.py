import csv
import os
import re
import requests
from typing import List, Dict, Tuple, Optional
from bs4 import BeautifulSoup
from pathlib import Path

from backend.database import get_db_connection, return_db_connection

# Elasticsearch configuration
ES_URL = "https://elasticsearch-job-cluster-eck.beslist.nl"
INDEX_PREFIX = "product_search_v4_nl-nl_"

# Maincat mapping file path (relative to this file)
MAINCAT_MAPPING_FILE = Path(__file__).parent / "maincat_mapping.csv"

# Cache for maincat mapping (loaded once)
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


def extract_from_url(url: str, maincat_mapping: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract maincat_id and pimId from URL.

    Supports two formats:
    1. Old: /p/gezond_mooi/nl-nl-gold-6150802976981/ -> maincat from mapping, pimId with prefix
    2. New: /p/product-name/286/6150802976981/ -> maincat_id and pimId directly from URL

    Returns: (maincat_id, pimId) tuple, with pimId always in 'nl-nl-gold-XXX' format
    """
    url = url.rstrip('/')
    parts = url.split('/')

    # Try old format first: check if any part matches maincat_url mapping
    for url_part, maincat_id in maincat_mapping.items():
        if f"/{url_part}/" in url:
            # Old format - pimId is the last part (already has nl-nl-gold- prefix)
            pim_id = parts[-1] if parts else None
            return maincat_id, pim_id

    # Try new format: /p/product-name/maincat_id/pimId/
    # Pattern: the second-to-last part should be a number (maincat_id)
    # and the last part should also be a number (pimId without prefix)
    if len(parts) >= 2:
        potential_maincat = parts[-2]
        potential_pim_id = parts[-1]

        # Check if both are numeric
        if potential_maincat.isdigit() and potential_pim_id.isdigit():
            # New format detected - add nl-nl-gold- prefix to pimId
            pim_id = f"nl-nl-gold-{potential_pim_id}"
            return potential_maincat, pim_id

    return None, None


def query_elasticsearch(index: str, pim_ids: List[str]) -> Dict[str, str]:
    """Query Elasticsearch for plpUrls given a list of pimIds."""
    if not pim_ids:
        return {}

    query = {
        "_source": ["plpUrl", "pimId"],
        "size": len(pim_ids),
        "query": {
            "terms": {
                "pimId": pim_ids
            }
        }
    }

    url = f"{ES_URL}/{index}/_search"
    response = requests.post(url, json=query, timeout=60)
    response.raise_for_status()

    data = response.json()

    # Map pimId to plpUrl
    result = {}
    for hit in data.get('hits', {}).get('hits', []):
        source = hit.get('_source', {})
        pim_id = source.get('pimId')
        plp_url = source.get('plpUrl')
        if pim_id:
            result[pim_id] = plp_url if plp_url else None

    return result


def extract_hyperlinks_from_content(content: str) -> List[str]:
    """
    Extract all href URLs from HTML content.
    Returns list of relative URLs found in <a href="..."> tags.
    Only returns product URLs (starting with /p/).
    """
    soup = BeautifulSoup(content, 'html.parser')
    links = []

    for link in soup.find_all('a', href=True):
        href = link['href']
        # Only include product URLs (starting with /p/)
        if href.startswith('/p/'):
            links.append(href)

    return links


def lookup_plp_urls_for_content(content: str) -> Dict[str, Optional[str]]:
    """
    Look up correct plpUrls for all product links in content.

    Returns dict mapping original URLs to their correct plpUrls:
    - If plpUrl found: original_url -> correct_plpUrl
    - If product gone: original_url -> None
    """
    maincat_mapping = load_maincat_mapping()
    links = extract_hyperlinks_from_content(content)

    if not links:
        return {}

    # Group links by maincat_id for batch ES queries
    # Structure: {maincat_id: {pim_id: original_url}}
    maincat_groups: Dict[str, Dict[str, str]] = {}
    url_to_pim_id: Dict[str, str] = {}

    for link in set(links):  # deduplicate
        maincat_id, pim_id = extract_from_url(link, maincat_mapping)

        if maincat_id and pim_id:
            url_to_pim_id[link] = pim_id
            if maincat_id not in maincat_groups:
                maincat_groups[maincat_id] = {}
            maincat_groups[maincat_id][pim_id] = link

    # Query each maincat index
    pim_id_to_plp_url: Dict[str, Optional[str]] = {}

    for maincat_id, pim_id_map in maincat_groups.items():
        index = f"{INDEX_PREFIX}{maincat_id}"
        pim_ids = list(pim_id_map.keys())

        try:
            result = query_elasticsearch(index, pim_ids)
            # result maps pim_id -> plpUrl (or empty if not found)
            for pim_id in pim_ids:
                pim_id_to_plp_url[pim_id] = result.get(pim_id)
        except Exception as e:
            print(f"[LINK_VALIDATOR] Error querying ES index {index}: {e}")
            # Mark all as None (will be treated as GONE)
            for pim_id in pim_ids:
                pim_id_to_plp_url[pim_id] = None

    # Build result: original_url -> correct_plpUrl (or None if GONE)
    result = {}
    for link in set(links):
        pim_id = url_to_pim_id.get(link)
        if pim_id:
            plp_url = pim_id_to_plp_url.get(pim_id)
            result[link] = plp_url
        else:
            # Could not extract pim_id from URL - treat as GONE
            result[link] = None

    return result


def replace_url_in_content(content: str, old_url: str, new_url: str) -> str:
    """Replace all occurrences of old_url with new_url in HTML content."""
    # Use BeautifulSoup to properly handle HTML
    soup = BeautifulSoup(content, 'html.parser')

    for link in soup.find_all('a', href=old_url):
        link['href'] = new_url

    return str(soup)


def validate_and_fix_content_links(content: str, content_url: str) -> Dict:
    """
    Validate all product hyperlinks in content and fix them if needed.

    Returns dict with:
    {
        'content_url': str,  # The URL this content belongs to
        'original_content': str,  # Original content
        'corrected_content': str,  # Content with corrected URLs (or original if no changes)
        'has_changes': bool,  # Whether content was modified
        'replaced_urls': List[Dict],  # List of {old_url, new_url}
        'gone_urls': List[str],  # URLs where product is gone (need reprocessing)
        'valid_urls': List[str],  # URLs that were already correct
    }
    """
    result = {
        'content_url': content_url,
        'original_content': content,
        'corrected_content': content,
        'has_changes': False,
        'replaced_urls': [],
        'gone_urls': [],
        'valid_urls': []
    }

    if not content:
        return result

    # Look up correct plpUrls for all links
    url_lookup = lookup_plp_urls_for_content(content)

    if not url_lookup:
        return result

    corrected_content = content

    for original_url, correct_plp_url in url_lookup.items():
        if correct_plp_url is None:
            # Product is GONE - mark for reprocessing
            result['gone_urls'].append(original_url)
        elif correct_plp_url != original_url:
            # URL needs to be replaced
            corrected_content = replace_url_in_content(corrected_content, original_url, correct_plp_url)
            result['replaced_urls'].append({
                'old_url': original_url,
                'new_url': correct_plp_url
            })
            result['has_changes'] = True
        else:
            # URL is already correct
            result['valid_urls'].append(original_url)

    result['corrected_content'] = corrected_content
    return result


def update_content_in_redshift(content_url: str, new_content: str) -> bool:
    """
    Update content in local PostgreSQL pa.content_urls_joep table.
    Returns True if successful.
    Note: Function name kept for backwards compatibility.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            UPDATE pa.content_urls_joep
            SET content = %s
            WHERE url = %s
        """, (new_content, content_url))

        conn.commit()
        print(f"[LINK_VALIDATOR] Updated content for URL: {content_url}")
        return True
    except Exception as e:
        print(f"[LINK_VALIDATOR] Error updating content in PostgreSQL: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_db_connection(conn)


def add_urls_to_werkvoorraad(urls: List[str]) -> int:
    """
    Add URLs to werkvoorraad table for reprocessing.
    Inserts into pa.jvs_seo_werkvoorraad with kopteksten=0.
    Returns number of URLs added.
    """
    if not urls:
        return 0

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        added_count = 0
        for url in urls:
            try:
                # PostgreSQL supports ON CONFLICT
                cur.execute("""
                    INSERT INTO pa.jvs_seo_werkvoorraad (url, kopteksten)
                    VALUES (%s, 0)
                    ON CONFLICT (url) DO UPDATE SET kopteksten = 0
                """, (url,))
                added_count += cur.rowcount
            except Exception as e:
                print(f"[LINK_VALIDATOR] Error adding URL {url} to werkvoorraad: {e}")

        conn.commit()
        print(f"[LINK_VALIDATOR] Added {added_count} URLs to werkvoorraad for reprocessing")
        return added_count
    except Exception as e:
        print(f"[LINK_VALIDATOR] Error adding URLs to werkvoorraad: {e}")
        if conn:
            conn.rollback()
        return 0
    finally:
        if conn:
            return_db_connection(conn)


def validate_and_fix_content_batch(contents: List[Tuple[str, str]],
                                    auto_update_db: bool = False,
                                    auto_add_to_werkvoorraad: bool = False) -> Dict:
    """
    Validate and fix hyperlinks for multiple content items.

    Args:
        contents: List of tuples (content_url, content)
        auto_update_db: If True, automatically update corrected content in PostgreSQL
        auto_add_to_werkvoorraad: If True, automatically add gone URLs to werkvoorraad

    Returns:
        Dict with summary and detailed results
    """
    results = []
    all_gone_urls = []
    total_replaced = 0
    total_updated = 0

    for content_url, content in contents:
        validation = validate_and_fix_content_links(content, content_url)
        results.append(validation)

        # Collect gone URLs
        all_gone_urls.extend(validation['gone_urls'])

        # Update PostgreSQL if content changed and auto_update is enabled
        if validation['has_changes'] and auto_update_db:
            if update_content_in_redshift(content_url, validation['corrected_content']):
                total_updated += 1

        total_replaced += len(validation['replaced_urls'])

    # Add gone URLs to werkvoorraad if enabled
    urls_added_to_werkvoorraad = 0
    if auto_add_to_werkvoorraad and all_gone_urls:
        urls_added_to_werkvoorraad = add_urls_to_werkvoorraad(all_gone_urls)

    return {
        'total_content_items': len(contents),
        'total_urls_replaced': total_replaced,
        'total_content_updated_in_db': total_updated,
        'total_gone_urls': len(all_gone_urls),
        'urls_added_to_werkvoorraad': urls_added_to_werkvoorraad,
        'gone_urls': list(set(all_gone_urls)),  # deduplicated
        'details': results
    }


# Legacy function for backward compatibility
def validate_content_links(content: str, conservative_mode: bool = False) -> Dict:
    """
    Legacy function - now uses Elasticsearch lookup instead of HTTP requests.
    The conservative_mode parameter is kept for backward compatibility but ignored.

    Returns dict with validation results in the old format for compatibility.
    """
    result = validate_and_fix_content_links(content, "")

    # Convert to legacy format
    broken_links = []
    for gone_url in result['gone_urls']:
        broken_links.append({
            'url': gone_url,
            'full_url': f"https://www.beslist.nl{gone_url}",
            'status_code': 'GONE',
            'status_text': 'Product not found in Elasticsearch'
        })

    return {
        'total_links': len(result['valid_urls']) + len(result['gone_urls']) + len(result['replaced_urls']),
        'broken_links': broken_links,
        'valid_links': len(result['valid_urls']),
        'has_broken_links': len(broken_links) > 0,
        'replaced_urls': result['replaced_urls'],
        'corrected_content': result['corrected_content']
    }


# Test function
if __name__ == "__main__":
    # Test with sample content
    test_content = '''
    <p>Check out these products:</p>
    <a href="/p/gezond_mooi/nl-nl-gold-6150802976981/">Product 1</a>
    <a href="/p/product-name/286/6150802976981/">Product 2</a>
    '''

    print("Testing link validator with Elasticsearch lookup...")
    result = validate_and_fix_content_links(test_content, "test-url")
    print(f"Result: {result}")
