"""
Run facet volume processing for all maincats.
Processes each maincat sequentially, saves progress after each one.
Output: facets_output.csv (same format as input + search_volume column)
"""
import csv
import os
import sys
import time
from collections import defaultdict

# Add project root to path
sys.path.insert(0, '/app')

from backend.category_keyword_service import process_facet_volumes

INPUT_FILE = '/app/backend/facets_input.csv'
OUTPUT_FILE = '/app/backend/facets_output.csv'
PROGRESS_FILE = '/app/backend/facets_progress.txt'

OUTPUT_COLUMNS = [
    'main_category', 'main_category_id', 'facet_name',
    'bucket', 'url_name', 'id', 'facet_value', 'search_volume'
]


def load_completed_maincats():
    """Load list of already completed maincats from progress file."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def mark_completed(maincat_name):
    """Mark a maincat as completed in progress file."""
    with open(PROGRESS_FILE, 'a') as f:
        f.write(f"{maincat_name}\n")


def main():
    start_total = time.time()

    # Load all facet rows grouped by maincat
    facets_by_mc = defaultdict(list)
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            facets_by_mc[row['main_category']].append(row)

    total_facets = sum(len(v) for v in facets_by_mc.values())
    print(f"Loaded {total_facets} facet values across {len(facets_by_mc)} maincats")

    # Check which maincats are already done
    completed = load_completed_maincats()
    if completed:
        print(f"Resuming: {len(completed)} maincats already completed")

    # Sort maincats by size (smallest first for quick progress)
    maincats_sorted = sorted(facets_by_mc.keys(), key=lambda x: len(facets_by_mc[x]))

    # Initialize output file if starting fresh
    if not completed:
        with open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, delimiter=';')
            writer.writeheader()

    processed_total = 0
    grand_total_volume = 0

    for i, mc in enumerate(maincats_sorted, 1):
        if mc in completed:
            print(f"[{i}/{len(maincats_sorted)}] {mc}: already done, skipping")
            continue

        facet_rows = facets_by_mc[mc]
        print(f"\n[{i}/{len(maincats_sorted)}] {mc}: {len(facet_rows)} facets")

        start_mc = time.time()
        try:
            result = process_facet_volumes(mc, facet_rows)
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        elapsed = time.time() - start_mc
        stats = result['stats']
        mc_total = result['grand_total']
        grand_total_volume += mc_total

        print(f"  Done in {elapsed:.1f}s | {stats['unique_keywords']} keywords | "
              f"volume: {mc_total:,} | customer_ids: {stats['customer_ids_used']}")

        # Append results to output CSV
        with open(OUTPUT_FILE, 'a', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, delimiter=';')
            for r in result['results']:
                writer.writerow(r)

        mark_completed(mc)
        processed_total += len(facet_rows)

        elapsed_total = time.time() - start_total
        remaining = len(maincats_sorted) - i
        print(f"  Progress: {processed_total}/{total_facets} facets | "
              f"elapsed: {elapsed_total:.0f}s | {remaining} maincats remaining")

    elapsed_total = time.time() - start_total
    print(f"\n{'='*60}")
    print(f"ALL DONE in {elapsed_total:.0f}s ({elapsed_total/60:.1f} min)")
    print(f"Total facets processed: {processed_total}")
    print(f"Grand total search volume: {grand_total_volume:,}")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
