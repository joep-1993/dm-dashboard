import csv
import os
import re
import requests
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple, Optional
from bs4 import BeautifulSoup
from pathlib import Path

from backend.database import get_db_connection, return_db_connection

# Elasticsearch configuration
ES_URL = "https://elasticsearch-job-cluster-eck-v9.beslist.nl"
INDEX_PREFIX = "product_search_v4_nl-nl_"

# Max concurrent ES connections.  Validation runs 2×50 worker threads that all
# share this session.  pool_block=True ensures we never exceed the limit — extra
# threads wait for a free connection instead of opening unbounded sockets.
_ES_POOL_SIZE = 50

# Reuse a single HTTP session for all ES queries.  This keeps the TCP+TLS
# connection alive between requests, avoiding a ~3.5 s TLS handshake on
# every query (measured: 3 500 ms -> 27 ms per query).
_es_session = requests.Session()
_es_session.mount("https://", HTTPAdapter(
    pool_connections=1, pool_maxsize=_ES_POOL_SIZE, pool_block=True,
))

# Bare UUID (no V4_ prefix): 8-4-4-4-12 hex. Newer Beslist PLP URLs use this
# format alongside the older V4_<uuid> form; ES stores both in the `id` field.
_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE,
)

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


def extract_from_url(url: str, maincat_mapping: Dict[str, str]) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Extract maincat_id and lookup value from URL.

    Supports three formats:
    1. Old: /p/gezond_mooi/nl-nl-gold-6150802976981/ -> maincat from mapping, pimId with prefix
    2. New numeric: /p/product-name/286/6150802976981/ -> maincat_id and pimId directly from URL
    3. V4 UUID: /p/product-name/137/V4_xxx/ -> maincat_id and plpUrl path (for plpUrl-based lookup)

    Returns: (maincat_id, lookup_value, is_v4_url) tuple
    - For V4 URLs: lookup_value is the relative plpUrl path (search by plpUrl in ES)
    - For other URLs: lookup_value is the pimId (search by pimId in ES)
    """
    url = url.rstrip('/')
    parts = url.split('/')

    # Try old format first: check if any part matches maincat_url mapping
    for url_part, maincat_id in maincat_mapping.items():
        if f"/{url_part}/" in url:
            # Old format - pimId is the last part (already has nl-nl-gold- prefix)
            pim_id = parts[-1] if parts else None
            return maincat_id, pim_id, False

    # Try new formats: /p/product-name/maincat_id/pimId/
    # Pattern: the second-to-last part should be a number (maincat_id)
    if len(parts) >= 2:
        potential_maincat = parts[-2]
        potential_pim_id = parts[-1]

        if potential_maincat.isdigit():
            # UUID-based pimIds: legacy V4_<uuid> and newer bare <uuid>. Both
            # are stored in ES `id` and resolved through the plpUrl path branch
            # (see query_elasticsearch_by_plpurl).
            if potential_pim_id.startswith('V4_') or _UUID_RE.match(potential_pim_id):
                p_index = url.find('/p/')
                if p_index != -1:
                    plp_path = url[p_index:] + '/'
                    return potential_maincat, plp_path, True
                return None, None, False
            # Check for numeric pimId
            elif potential_pim_id.isdigit():
                # New numeric format - add nl-nl-gold- prefix to pimId
                pim_id = f"nl-nl-gold-{potential_pim_id}"
                return potential_maincat, pim_id, False

    return None, None, False


def query_elasticsearch(index: str, pim_ids: List[str], min_offers: int = 2) -> Dict[str, str]:
    """
    Query Elasticsearch for plpUrls given a list of pimIds.

    Args:
        index: Elasticsearch index name
        pim_ids: List of pimIds to look up
        min_offers: Minimum number of offers required (default: 2).
                    Products with fewer offers are treated as "gone".

    Returns:
        Dict mapping pimId to plpUrl. Products with < min_offers return None.
    """
    if not pim_ids:
        return {}

    query = {
        "_source": ["plpUrl", "pimId", "shopCount"],
        "size": len(pim_ids),
        "query": {
            "terms": {
                "pimId": pim_ids
            }
        }
    }

    url = f"{ES_URL}/{index}/_search"
    response = _es_session.post(url, json=query, timeout=60)
    response.raise_for_status()

    data = response.json()

    # Map pimId to plpUrl (only if shopCount >= min_offers)
    result = {}
    for hit in data.get('hits', {}).get('hits', []):
        source = hit.get('_source', {})
        pim_id = source.get('pimId')
        plp_url = source.get('plpUrl')
        shop_count = source.get('shopCount', 0) or 0

        if pim_id:
            # Only return plpUrl if product has enough offers
            if shop_count >= min_offers and plp_url:
                result[pim_id] = plp_url
            else:
                # Treat as "gone" if not enough offers
                result[pim_id] = None

    return result


def query_elasticsearch_by_plpurl(index: str, plp_urls: List[str], min_offers: int = 2) -> Dict[str, str]:
    """
    Query Elasticsearch for products by their plpUrl paths.
    Used for V4 UUID URLs where the pimId in ES differs from the URL.

    Two-phase lookup:
    1. Fast: Try pimId lookup with V4 UUIDs (instant terms query)
    2. Fallback: Wildcard on plpUrl for any not found in phase 1 (slower, shorter timeout)

    Args:
        index: Elasticsearch index name
        plp_urls: List of plpUrl paths to look up (e.g., '/p/product-name/137/V4_xxx/')
        min_offers: Minimum number of offers required (default: 2).

    Returns:
        Dict mapping original plpUrl to the current plpUrl if valid, None if not found or < min_offers.
    """
    if not plp_urls:
        return {}

    # Extract UUID-style id from each plpUrl. Two valid formats:
    #   /p/product-name/maincat/V4_<uuid>/   (legacy)
    #   /p/product-name/maincat/<uuid>/       (newer, no V4_ prefix)
    # Both resolve via ES `id` field — the last URL segment is the id.
    v4_to_original: Dict[str, str] = {}
    for plp_url in plp_urls:
        parts = plp_url.rstrip('/').split('/')
        v4_part = parts[-1] if parts else ''
        if v4_part.startswith('V4_') or _UUID_RE.match(v4_part):
            v4_to_original[v4_part] = plp_url

    if not v4_to_original:
        return {}

    result = {}
    found_v4_parts = set()

    # Phase 1: Fast id-based lookup with V4 UUIDs.
    # V4 UUIDs are stored in the `id` / `groupId` fields of the index — NOT in
    # `pimId` (which uses values like `nl-nl-gold-...`). Matching on pimId used
    # to always miss, causing every V4 link to be silently skipped by the
    # validator (neither replaced when slugs change, nor flagged gone when the
    # product disappears from ES). Querying on `id` fixes both behaviors.
    try:
        v4_uuids = list(v4_to_original.keys())
        query = {
            "_source": ["plpUrl", "id", "shopCount"],
            "size": len(v4_uuids),
            "query": {
                "terms": {
                    "id": v4_uuids
                }
            }
        }

        es_url = f"{ES_URL}/{index}/_search"
        response = _es_session.post(es_url, json=query, timeout=15)
        response.raise_for_status()
        data = response.json()

        for hit in data.get('hits', {}).get('hits', []):
            source = hit.get('_source', {})
            v4_id = source.get('id', '')
            es_plp_url = source.get('plpUrl', '')
            shop_count = source.get('shopCount', 0) or 0

            if v4_id in v4_to_original:
                original_url = v4_to_original[v4_id]
                found_v4_parts.add(v4_id)
                if shop_count >= min_offers and es_plp_url:
                    result[original_url] = es_plp_url
                else:
                    result[original_url] = None

        # V4 UUIDs not in the phase-1 response are treated as GONE (product no
        # longer exists in ES). We query on the authoritative `id` field, so a
        # miss is reliable — no need for a wildcard fallback.
        for v4_uuid, original_url in v4_to_original.items():
            if v4_uuid not in found_v4_parts:
                result[original_url] = None

        if found_v4_parts:
            print(f"[LINK_VALIDATOR] V4 id lookup found {len(found_v4_parts)}/{len(v4_uuids)} products in {index}")
        missing = len(v4_to_original) - len(found_v4_parts)
        if missing > 0:
            print(f"[LINK_VALIDATOR] Marked {missing} V4 URLs as GONE in {index} (not found in ES)")
    except Exception as e:
        print(f"[LINK_VALIDATOR] V4 id lookup failed for {index}: {e} - skipping batch (not marking as gone)")

    return result


def extract_hyperlinks_from_content(content: str) -> List[str]:
    """
    Extract all href URLs from HTML content.
    Returns list of relative URLs found in <a href="..."> tags.
    Handles both relative (/p/...) and absolute (https://www.beslist.nl/p/...) URLs.
    """
    soup = BeautifulSoup(content, 'html.parser')
    links = []

    for link in soup.find_all('a', href=True):
        href = link['href']
        # Include both relative /p/ and absolute beslist.nl/p/ URLs
        if '/p/' in href:
            # Convert absolute URLs to relative for consistent processing
            if href.startswith('https://www.beslist.nl'):
                href = href.replace('https://www.beslist.nl', '')
            links.append(href)

    return links


def _lookup_links(links: List[str]) -> Tuple[Dict[Tuple[str, str], Optional[str]],
                                             Dict[str, Tuple[str, str, bool]],
                                             List[str]]:
    """Resolve product hyperlinks against Elasticsearch.

    Shared core for the kopteksten path (lookup_plp_urls_for_content) and the
    FAQ path (validate_faq_links) so their dead/alive logic can't drift again.

    Given a list of links, returns:
      - lookup_to_plp_url: {(maincat_id, lookup_value): plpUrl or None}.
            Keyed by (maincat_id, lookup_value) — NOT lookup_value alone — so
            the same pimId/plpUrl under two different maincats can't clobber
            each other. A key is ABSENT only when the ES query failed
            (transient); callers treat absent as "skip, don't mark gone".
      - url_to_lookup: {original_link: (maincat_id, lookup_value, is_v4)}
      - unknown_format_links: links extract_from_url could not parse (a new PLP
            format) — NOT treated as gone.
    """
    maincat_mapping = load_maincat_mapping()

    # Group links by maincat_id for batch ES queries.
    # Separate V4 URLs (query by plpUrl) from regular URLs (query by pimId).
    maincat_pimid_groups: Dict[str, Dict[str, str]] = {}
    maincat_plpurl_groups: Dict[str, Dict[str, str]] = {}
    url_to_lookup: Dict[str, Tuple[str, str, bool]] = {}
    unknown_format_links: List[str] = []

    for link in dict.fromkeys(links):  # dedupe, order-preserving (deterministic)
        # Accept both relative (/p/...) and absolute (https://www.beslist.nl/p/...)
        relative_link = link
        if link.startswith('https://www.beslist.nl'):
            relative_link = link.replace('https://www.beslist.nl', '')

        maincat_id, lookup_value, is_v4 = extract_from_url(relative_link, maincat_mapping)
        if maincat_id and lookup_value:
            url_to_lookup[link] = (maincat_id, lookup_value, is_v4)
            groups = maincat_plpurl_groups if is_v4 else maincat_pimid_groups
            groups.setdefault(maincat_id, {})[lookup_value] = link
        else:
            unknown_format_links.append(link)

    lookup_to_plp_url: Dict[Tuple[str, str], Optional[str]] = {}

    # Query all maincat indices in parallel (pimId + V4 plpUrl lookups).
    # The _es_session connection pool (pool_block=True) throttles actual
    # network concurrency to _ES_POOL_SIZE, so this is safe for the cluster.
    def _query_pimid(maincat_id, pim_id_map):
        index = f"{INDEX_PREFIX}{maincat_id}"
        pim_ids = list(pim_id_map.keys())
        try:
            res = query_elasticsearch(index, pim_ids)
            return {(maincat_id, pid): res.get(pid) for pid in pim_ids}
        except Exception as e:
            print(f"[LINK_VALIDATOR] Error querying ES index {index} by pimId: {e} - skipping batch (not marking as gone)")
            return {}

    def _query_plpurl(maincat_id, plp_url_map):
        index = f"{INDEX_PREFIX}{maincat_id}"
        plp_urls = list(plp_url_map.keys())
        try:
            res = query_elasticsearch_by_plpurl(index, plp_urls)
            return {(maincat_id, u): res[u] for u in plp_urls if u in res}
        except Exception as e:
            print(f"[LINK_VALIDATOR] Error querying ES index {index} by plpUrl: {e} - skipping batch (not marking as gone)")
            return {}

    total_queries = len(maincat_pimid_groups) + len(maincat_plpurl_groups)
    if total_queries <= 1:
        # Single query — no threading overhead needed
        for maincat_id, pim_id_map in maincat_pimid_groups.items():
            lookup_to_plp_url.update(_query_pimid(maincat_id, pim_id_map))
        for maincat_id, plp_url_map in maincat_plpurl_groups.items():
            lookup_to_plp_url.update(_query_plpurl(maincat_id, plp_url_map))
    else:
        with ThreadPoolExecutor(max_workers=min(total_queries, 10)) as pool:
            futures = []
            for maincat_id, pim_id_map in maincat_pimid_groups.items():
                futures.append(pool.submit(_query_pimid, maincat_id, pim_id_map))
            for maincat_id, plp_url_map in maincat_plpurl_groups.items():
                futures.append(pool.submit(_query_plpurl, maincat_id, plp_url_map))
            for future in as_completed(futures):
                lookup_to_plp_url.update(future.result())

    return lookup_to_plp_url, url_to_lookup, unknown_format_links


def lookup_plp_urls_for_content(content: str) -> Tuple[Dict[str, Optional[str]], List[str]]:
    """
    Look up correct plpUrls for all product links in content.

    Returns a tuple (lookup, unknown_format_links):
    - lookup: dict mapping original URLs to their correct plpUrls
      - If plpUrl found: original_url -> correct_plpUrl
      - If product gone: original_url -> None
    - unknown_format_links: URLs whose format extract_from_url could not
      parse. These are NOT classified as gone — a new PLP format being
      introduced should not silently delete content.
    """
    links = extract_hyperlinks_from_content(content)

    if not links:
        return {}, []

    lookup_to_plp_url, url_to_lookup, unknown_format_links = _lookup_links(links)

    # Build result: original_url -> correct_plpUrl (or None if GONE).
    # A key missing from lookup_to_plp_url means the ES query failed — skip it
    # (don't mark gone). Unknown-format URLs are returned separately and are
    # NOT treated as gone — see docstring.
    result: Dict[str, Optional[str]] = {}
    for link, (maincat_id, lookup_value, _) in url_to_lookup.items():
        key = (maincat_id, lookup_value)
        if key in lookup_to_plp_url:
            result[link] = lookup_to_plp_url[key]

    return result, unknown_format_links


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
        'unknown_format_urls': List[str],  # URLs whose format was not recognized
                                            # (e.g. a new PLP format) — flagged but
                                            # NOT treated as gone
    }
    """
    result = {
        'content_url': content_url,
        'original_content': content,
        'corrected_content': content,
        'has_changes': False,
        'replaced_urls': [],
        'gone_urls': [],
        'valid_urls': [],
        'unknown_format_urls': [],
    }

    if not content:
        return result

    # Look up correct plpUrls for all links
    url_lookup, unknown_format_links = lookup_plp_urls_for_content(content)
    result['unknown_format_urls'] = unknown_format_links

    if not url_lookup and not unknown_format_links:
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


# --- FAQ Link Validation ---

def extract_hyperlinks_from_faq_json(faq_json: str) -> List[str]:
    """
    Extract all product href URLs from FAQ JSON content.
    FAQ JSON is a list of {question, answer} objects where answers may contain HTML links.
    Handles both relative (/p/...) and absolute (https://www.beslist.nl/p/...) URLs.
    """
    import json

    links = []
    try:
        faqs = json.loads(faq_json) if faq_json else []
        for faq in faqs:
            answer = faq.get('answer', '')
            # Extract links from each answer using BeautifulSoup
            soup = BeautifulSoup(answer, 'html.parser')
            for link in soup.find_all('a', href=True):
                href = link['href']
                # Include both relative /p/ and absolute beslist.nl/p/ URLs
                if '/p/' in href:
                    links.append(href)
    except json.JSONDecodeError:
        pass

    return links


def validate_faq_links(faq_json: str) -> Dict:
    """
    Validate all product hyperlinks in FAQ JSON content.

    Returns dict with:
    {
        'total_links': int,
        'valid_links': int,
        'gone_links': List[str],            # URLs where ES says product is gone
        'has_gone_links': bool,
        'unknown_format_links': List[str],  # URLs the validator couldn't parse —
                                            # NOT treated as gone. A new PLP format
                                            # being introduced should be surfaced
                                            # for investigation, not silently reset.
        'has_unknown_format_links': bool,
    }
    """
    links = extract_hyperlinks_from_faq_json(faq_json)

    if not links:
        return {
            'total_links': 0,
            'valid_links': 0,
            'gone_links': [],
            'has_gone_links': False,
            'unknown_format_links': [],
            'has_unknown_format_links': False,
        }

    lookup_to_plp_url, url_to_lookup, unknown_format_links = _lookup_links(links)

    # Determine which links are gone vs. valid. A key missing from
    # lookup_to_plp_url means the ES query failed — skip it (don't mark gone).
    # Unknown-format links were collected separately and are NOT counted as gone.
    gone_links = []
    valid_count = 0

    for link, (maincat_id, lookup_value, _) in url_to_lookup.items():
        key = (maincat_id, lookup_value)
        if key not in lookup_to_plp_url:
            continue
        if lookup_to_plp_url[key] is None:
            gone_links.append(link)
        else:
            valid_count += 1

    # Every distinct link is either parsed (url_to_lookup) or unknown-format,
    # so this equals the old len(set(links)).
    total_links = len(url_to_lookup) + len(unknown_format_links)
    return {
        'total_links': total_links,
        'valid_links': valid_count,
        'gone_links': gone_links,
        'has_gone_links': len(gone_links) > 0,
        'unknown_format_links': unknown_format_links,
        'has_unknown_format_links': len(unknown_format_links) > 0,
    }


def mark_faq_failed_unknown_format(url_to_unknown: Dict[str, List[str]]) -> int:
    """Mark FAQ URLs as failed because their content contains links the
    validator could not parse (a new PLP URL format).

    These URLs are NOT reset to pending — content is preserved so it can be
    inspected and the validator's URL parser updated. The job row gets
    `status='failed'` and `last_error='unknown_url_format: <sample>'` so the
    failure surfaces in the dashboard and triggers a code update.

    Args:
        url_to_unknown: mapping of content URL to the list of unrecognized
                        product URLs found in that content.

    Returns:
        Number of FAQ rows updated.
    """
    if not url_to_unknown:
        return 0
    from backend.url_catalog import get_url_id
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        updated = 0
        for url, unknown_urls in url_to_unknown.items():
            url_id = get_url_id(cur, url)
            if url_id is None:
                continue
            sample = unknown_urls[0] if unknown_urls else ''
            last_error = (
                f"unknown_url_format: {len(unknown_urls)} link(s) not parseable "
                f"by validator (e.g. {sample[:160]}). Update extract_from_url "
                f"in link_validator.py and re-run validation."
            )
            cur.execute("""
                UPDATE pa.faq_jobs
                   SET status = 'failed',
                       last_error = %s,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE url_id = %s
            """, (last_error[:1000], url_id))
            updated += 1
        conn.commit()
        print(f"[FAQ_VALIDATOR] Flagged {updated} FAQ URLs as failed (unknown URL format)")
        return updated
    except Exception as e:
        print(f"[FAQ_VALIDATOR] Error flagging FAQ URLs as failed: {e}")
        if conn:
            conn.rollback()
        return 0
    finally:
        if conn:
            return_db_connection(conn)


def reset_faq_to_pending(urls: List[str]) -> int:
    """Reset FAQ URLs to pending so they get regenerated.
    - pa.faq_jobs → status='pending', skip_reason=NULL
    - pa.faq_content_v2 → row deleted
    - pa.url_validation → row deleted (so URL gets re-scraped)
    Returns number of URLs reset.
    """
    if not urls:
        return 0
    from backend.url_catalog import get_url_id
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        reset_count = 0
        for url in urls:
            url_id = get_url_id(cur, url)
            if url_id is None:
                continue
            cur.execute("""
                UPDATE pa.faq_jobs
                   SET status = 'pending',
                       skip_reason = NULL,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE url_id = %s
            """, (url_id,))
            cur.execute("DELETE FROM pa.faq_content_v2 WHERE url_id = %s", (url_id,))
            cur.execute("DELETE FROM pa.url_validation   WHERE url_id = %s", (url_id,))
            reset_count += 1
        conn.commit()
        print(f"[FAQ_VALIDATOR] Reset {reset_count} FAQ URLs to pending")
        return reset_count
    except Exception as e:
        print(f"[FAQ_VALIDATOR] Error resetting FAQ URLs: {e}")
        if conn:
            conn.rollback()
        return 0
    finally:
        if conn:
            return_db_connection(conn)


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
