"""
Run facet volume processing for all maincats using new Excel input.

Input: /app/backend/faet_values_new.xlsx
  - Sheet 'facets': facet values (col B = maincat_id, col G = facet_value)
  - Sheet 'cats': categories (col B = maincat_id, col C = deepest_cat)

Output: writes search_volume into column K of the facets sheet, saves as new file.
"""
import os
import re
import sys
import time
from collections import defaultdict

import pandas as pd

sys.path.insert(0, '/app')

from backend.category_keyword_service import (
    parse_sic_sod, clean_facet_value, get_category_forms, _normalize_keyword
)
from backend.keyword_planner_service import get_search_volumes

INPUT_FILE = '/app/backend/faet_values_new.xlsx'
OUTPUT_FILE = '/app/backend/faet_values_new_output.xlsx'
PROGRESS_FILE = '/app/backend/facets_new_progress.txt'


def load_completed_maincats():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def mark_completed(maincat_id):
    with open(PROGRESS_FILE, 'a') as f:
        f.write(f"{maincat_id}\n")


def process_maincat(facet_rows, categories):
    """
    Process facet values for a maincat: generate keyword combinations with
    all deepest categories, look up search volumes, aggregate per facet row.

    Args:
        facet_rows: list of dicts with 'facet_value' and row index
        categories: list of dicts with 'deepest_cat'

    Returns:
        dict mapping row_index -> search_volume
    """
    all_keywords_ordered = []
    keyword_seen = set()
    keyword_to_rows = defaultdict(set)
    skipped = 0

    for entry in facet_rows:
        idx = entry['idx']
        raw = str(entry['facet_value'])

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

        for cat in categories:
            forms = get_category_forms(cat['deepest_cat'])
            for form in forms:
                form = form.strip().lower()
                if not form:
                    continue
                for combo in [f"{before_text} {form}", f"{form} {after_text}"]:
                    keyword_to_rows[combo].add(idx)
                    if combo not in keyword_seen:
                        keyword_seen.add(combo)
                        all_keywords_ordered.append(combo)

    print(f"    {len(facet_rows)} facet values x {len(categories)} cats = "
          f"{len(all_keywords_ordered)} unique keywords (skipped {skipped} empty)")

    if not all_keywords_ordered:
        return {entry['idx']: 0 for entry in facet_rows}

    # Look up search volumes
    sv_result = get_search_volumes(all_keywords_ordered)

    # Build normalized volume lookup
    volume_lookup = {}
    for r in sv_result.get("results", []):
        norm = _normalize_keyword(r.get("normalized_keyword", ""))
        vol = r.get("search_volume", 0)
        volume_lookup[norm] = max(volume_lookup.get(norm, 0), vol)

    # Sum volumes per facet row
    row_volumes = defaultdict(int)
    for combo, row_indices in keyword_to_rows.items():
        norm = _normalize_keyword(combo)
        vol = volume_lookup.get(norm, 0)
        for idx in row_indices:
            row_volumes[idx] += vol

    return dict(row_volumes)


def main():
    start_total = time.time()

    print("Loading input file...")
    facets_df = pd.read_excel(INPUT_FILE, sheet_name='facets')
    cats_df = pd.read_excel(INPUT_FILE, sheet_name='cats')

    print(f"Facets: {len(facets_df)} rows, Cats: {len(cats_df)} rows")

    # Ensure search_volume column exists and is numeric
    if 'search_volume' not in facets_df.columns:
        facets_df['search_volume'] = 0
    facets_df['search_volume'] = pd.to_numeric(facets_df['search_volume'], errors='coerce').fillna(0).astype(int)

    # Build categories by maincat_id
    cats_by_mcid = defaultdict(list)
    for _, row in cats_df.iterrows():
        mcid = str(int(row['maincat_id'])) if pd.notna(row['maincat_id']) else ''
        dc = str(row['deepest_cat']).strip() if pd.notna(row['deepest_cat']) else ''
        if mcid and dc and dc != 'nan':
            cats_by_mcid[mcid].append({'deepest_cat': dc})

    print(f"Categories loaded for {len(cats_by_mcid)} maincats")

    # Group facet rows by maincat_id
    facets_by_mcid = defaultdict(list)
    for idx, row in facets_df.iterrows():
        mcid = str(int(row['main_category_id'])) if pd.notna(row['main_category_id']) else ''
        fv = str(row['facet_value']) if pd.notna(row['facet_value']) else ''
        if mcid and fv:
            facets_by_mcid[mcid].append({
                'idx': idx,
                'facet_value': fv,
            })

    # Check completed
    completed = load_completed_maincats()
    if completed:
        print(f"Resuming: {len(completed)} maincats already completed")
        # Load previous output if it exists
        if os.path.exists(OUTPUT_FILE):
            try:
                print("Loading previous output for completed results...")
                prev_df = pd.read_excel(OUTPUT_FILE, sheet_name='facets')
                if 'search_volume' in prev_df.columns:
                    for idx in prev_df.index:
                        if idx < len(facets_df):
                            vol = prev_df.at[idx, 'search_volume']
                            if pd.notna(vol) and vol > 0:
                                facets_df.at[idx, 'search_volume'] = int(vol)
            except Exception as e:
                print(f"Could not load previous output: {e}, starting fresh")

    # Sort maincats by number of facets (smallest first)
    mcids_sorted = sorted(facets_by_mcid.keys(), key=lambda x: len(facets_by_mcid[x]))

    total_maincats = len(mcids_sorted)
    processed_count = 0
    grand_total = 0

    for i, mcid in enumerate(mcids_sorted, 1):
        if mcid in completed:
            mc_name = facets_by_mcid[mcid][0]['facet_value'] if facets_by_mcid[mcid] else mcid
            # Get maincat name from the dataframe
            mc_rows = facets_df[facets_df['main_category_id'] == int(mcid)]
            mc_name = mc_rows.iloc[0]['main_category'] if len(mc_rows) > 0 else mcid
            print(f"[{i}/{total_maincats}] {mc_name} ({mcid}): already done, skipping")
            continue

        cats = cats_by_mcid.get(mcid, [])
        facet_entries = facets_by_mcid[mcid]

        # Get maincat name for logging
        mc_rows = facets_df[facets_df['main_category_id'] == int(mcid)]
        mc_name = mc_rows.iloc[0]['main_category'] if len(mc_rows) > 0 else mcid

        print(f"\n[{i}/{total_maincats}] {mc_name} ({mcid}): {len(facet_entries)} facets, {len(cats)} cats")

        if not cats:
            print(f"    No categories found for maincat_id {mcid}, skipping")
            mark_completed(mcid)
            continue

        start_mc = time.time()
        try:
            row_volumes = process_maincat(facet_entries, cats)
        except Exception as e:
            print(f"    ERROR: {e}")
            import traceback
            traceback.print_exc()
            continue

        elapsed = time.time() - start_mc
        mc_total = sum(row_volumes.values())
        grand_total += mc_total

        # Write volumes back to dataframe
        for idx, vol in row_volumes.items():
            facets_df.at[idx, 'search_volume'] = vol

        mark_completed(mcid)
        processed_count += len(facet_entries)

        print(f"    Done in {elapsed:.1f}s | volume: {mc_total:,}")
        print(f"    Progress: {processed_count}/{len(facets_df)} facets | "
              f"{total_maincats - i} maincats remaining")

        # Save after each maincat (in case of interruption)
        print(f"    Saving progress to {OUTPUT_FILE}...")
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            facets_df.to_excel(writer, sheet_name='facets', index=False)
            cats_df.to_excel(writer, sheet_name='cats', index=False)

    elapsed_total = time.time() - start_total
    print(f"\n{'='*60}")
    print(f"ALL DONE in {elapsed_total:.0f}s ({elapsed_total/60:.1f} min)")
    print(f"Total facets processed: {processed_count}")
    print(f"Grand total search volume: {grand_total:,}")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
