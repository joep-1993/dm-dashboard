#!/usr/bin/env python3
"""
Lookup plpUrl from Elasticsearch using pimId extracted from input URLs.
Processes in batches of 10K and outputs CSV with original URL and plpUrl (or GONE).

Supports two URL formats:
1. Old format: /p/gezond_mooi/nl-nl-gold-6150802976981/ (maincat_url + pimId with prefix)
2. New format: /p/product-name/286/6150802976981/ (maincat_id + pimId without prefix)
"""

import csv
import re
import requests
from pathlib import Path

# Configuration
ES_URL = "https://elasticsearch-job-cluster-eck.beslist.nl"
INDEX_PREFIX = "product_search_v4_nl-nl_"
BATCH_SIZE = 10000

INPUT_FILE = Path(__file__).parent / "input_urls.csv"
OUTPUT_FILE = Path(__file__).parent / "output_plp_urls.csv"
MAINCAT_MAPPING_FILE = Path("/mnt/c/Users/JoepvanSchagen/Downloads/Python/maincat_mapping.csv")


def load_maincat_mapping(filepath: Path) -> dict[str, str]:
    """Load maincat URL to maincat_id mapping from CSV file."""
    mapping = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            # maincat_url contains values like /gezond_mooi/
            url_part = row['maincat_url'].strip('/')  # Remove leading/trailing slashes
            maincat_id = row['maincat_id']
            mapping[url_part] = maincat_id
    return mapping


def extract_from_url(url: str, maincat_mapping: dict[str, str]) -> tuple[str | None, str | None]:
    """
    Extract maincat_id and pimId from URL.

    Supports two formats:
    1. Old: /p/gezond_mooi/nl-nl-gold-6150802976981/ -> maincat from mapping, pimId with prefix
    2. New: /p/product-name/286/6150802976981/ -> maincat_id and pimId directly from URL

    Handles both relative (/p/...) and absolute (https://www.beslist.nl/p/...) URLs.

    Returns: (maincat_id, pimId) tuple, with pimId always in 'nl-nl-gold-XXX' format
    """
    # Convert absolute URLs to relative for consistent processing
    if url.startswith('https://www.beslist.nl'):
        url = url.replace('https://www.beslist.nl', '')
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


def load_input_urls(filepath: Path) -> list[str]:
    """Load URLs from input CSV file."""
    urls = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            url = line.strip()
            if url:
                urls.append(url)
    return urls


def query_elasticsearch(index: str, pim_ids: list[str], min_offers: int = 2) -> dict[str, str]:
    """
    Query Elasticsearch for plpUrls given a list of pimIds.

    Args:
        index: Elasticsearch index name
        pim_ids: List of pimIds to look up
        min_offers: Minimum number of offers required (default: 2).
                    Products with fewer offers are treated as "GONE".

    Returns:
        Dict mapping pimId to plpUrl. Products with < min_offers return "GONE".
    """
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
    response = requests.post(url, json=query, timeout=60)
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
                # Treat as "GONE" if not enough offers
                result[pim_id] = "GONE"

    return result


def main():
    # Load maincat mapping
    print(f"Loading maincat mapping from {MAINCAT_MAPPING_FILE}...")
    maincat_mapping = load_maincat_mapping(MAINCAT_MAPPING_FILE)
    print(f"Loaded {len(maincat_mapping)} maincat mappings")

    # Load input URLs
    print(f"Loading URLs from {INPUT_FILE}...")
    input_urls = load_input_urls(INPUT_FILE)
    print(f"Loaded {len(input_urls)} URLs")

    # Group URLs by maincat_id for batch processing
    # Structure: {maincat_id: {pim_id: [original_urls]}}
    maincat_groups: dict[str, dict[str, list[str]]] = {}
    url_to_pim_id: dict[str, str] = {}  # Track pimId for each URL
    urls_without_maincat = []

    for url in input_urls:
        maincat_id, pim_id = extract_from_url(url, maincat_mapping)

        if maincat_id and pim_id:
            url_to_pim_id[url] = pim_id
            if maincat_id not in maincat_groups:
                maincat_groups[maincat_id] = {}
            if pim_id not in maincat_groups[maincat_id]:
                maincat_groups[maincat_id][pim_id] = []
            maincat_groups[maincat_id][pim_id].append(url)
        else:
            urls_without_maincat.append(url)

    print(f"Grouped URLs into {len(maincat_groups)} maincat groups")
    if urls_without_maincat:
        print(f"Warning: {len(urls_without_maincat)} URLs could not be mapped to a maincat")

    # Query each maincat index in batches
    pim_id_to_plp_url = {}

    for maincat_id, pim_id_map in maincat_groups.items():
        index = f"{INDEX_PREFIX}{maincat_id}"
        all_pim_ids = list(pim_id_map.keys())
        print(f"\nQuerying index {index} ({len(all_pim_ids)} unique pimIds)...")

        for i in range(0, len(all_pim_ids), BATCH_SIZE):
            batch = all_pim_ids[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(all_pim_ids) + BATCH_SIZE - 1) // BATCH_SIZE

            if total_batches > 1:
                print(f"  Batch {batch_num}/{total_batches} ({len(batch)} pimIds)...")

            result = query_elasticsearch(index, batch)
            pim_id_to_plp_url.update(result)
            print(f"  Found {len(result)} results")

    # Write output CSV
    print(f"\nWriting output to {OUTPUT_FILE}...")
    found_count = 0
    gone_count = 0

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['input_url', 'plp_url'])

        for url in input_urls:
            pim_id = url_to_pim_id.get(url)
            if pim_id:
                plp_url = pim_id_to_plp_url.get(pim_id, "GONE")
                if plp_url == "GONE":
                    gone_count += 1
                else:
                    found_count += 1
            else:
                plp_url = "GONE"
                gone_count += 1

            writer.writerow([url, plp_url])

    print(f"\nDone!")
    print(f"  Found: {found_count}")
    print(f"  Gone: {gone_count}")
    print(f"  Total: {len(input_urls)}")


if __name__ == "__main__":
    main()
