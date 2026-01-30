#!/usr/bin/env python3
"""
Script to run REMOVEBUCKET canonical transformation for all rules in Excel file.
"""
import pandas as pd
import re
import csv
from backend.database import get_redshift_connection, return_redshift_connection

def remove_bucket_from_url(url, facet_name):
    """Remove a facet bucket from URL.

    E.g., if facet_name='maat_mode_bovenkleding' and URL contains 'maat_mode_bovenkleding~9231320',
    that entire bucket should be removed.
    """
    # Find the full bucket pattern (facet_name~number)
    pattern = rf'{re.escape(facet_name)}~\d+'
    match = re.search(pattern, url)

    if not match:
        return url

    full_bucket = match.group(0)

    # Now remove the bucket from the URL
    # Case 1: bucket~~other (bucket at start/middle)
    if f"{full_bucket}~~" in url:
        url = url.replace(f"{full_bucket}~~", "")
    # Case 2: other~~bucket (bucket at end)
    elif f"~~{full_bucket}" in url:
        url = url.replace(f"~~{full_bucket}", "")
    # Case 3: /c/bucket (single bucket)
    elif f"/c/{full_bucket}" in url:
        # If this is the only bucket, we may want to remove /c/ entirely
        # For now, just remove the bucket
        url = url.replace(full_bucket, "")

    # Clean up
    url = re.sub(r'~~+', '~~', url)  # Multiple ~~ to single ~~
    url = re.sub(r'/c/~~', '/c/', url)  # /c/~~ to /c/
    url = re.sub(r'~~/c/', '/c/', url)  # ~~/c/ to /c/ (shouldn't happen)
    url = re.sub(r'/c/$', '/', url)  # Trailing /c/ to just /
    url = re.sub(r'/c//', '/', url)  # Empty /c// to /

    return url

def main():
    # Read the Excel file
    df = pd.read_excel('/tmp/canonicals_input.xlsx', sheet_name='Blad1')
    print(f"Loaded {len(df)} rules from Excel")

    # Create a mapping of category_id -> facets to remove
    # Category URL like /mode_432353/ means we look for 'mode_432353' anywhere in URL
    cat_facet_map = {}
    all_facets = set()
    for _, row in df.iterrows():
        cat = row['caturl'].strip('/')  # e.g., mode_432353
        facet = row['facet']
        all_facets.add(facet)
        if cat not in cat_facet_map:
            cat_facet_map[cat] = []
        cat_facet_map[cat].append(facet)

    print(f"Unique facets: {len(all_facets)}")
    print(f"Unique categories: {len(cat_facet_map)}")
    print(f"Sample facets: {list(all_facets)[:10]}")

    # Query Redshift for URLs containing these facets in /c/ bucket paths
    conn = None
    results = []

    try:
        conn = get_redshift_connection()
        cur = conn.cursor()

        # Build query - search for /c/ URLs that contain facet~number pattern
        facet_list = list(all_facets)

        # Build LIKE conditions for facets with ~ pattern
        like_conditions = " OR ".join([f"dv.url LIKE '%%{f}~%%'" for f in facet_list])

        query = f"""
            SELECT
                SPLIT_PART(dv.url, '?', 1) as url,
                COUNT(*) as visits
            FROM datamart.fct_visits fcv
            JOIN datamart.dim_visit dv
                ON fcv.dim_visit_key = dv.dim_visit_key
            WHERE dv.is_real_visit = 1
              AND fcv.dim_date_key BETWEEN 20240101 AND 20260130
              AND dv.url LIKE '%%beslist.nl%%'
              AND dv.url LIKE '%%/products/%%'
              AND dv.url LIKE '%%/c/%%'
              AND ({like_conditions})
            GROUP BY 1
            HAVING COUNT(*) > 0
            ORDER BY 2 DESC
            LIMIT 50000
        """

        print(f"Querying Redshift for URLs with {len(facet_list)} facet patterns...")
        cur.execute(query)
        rows = cur.fetchall()

        all_urls = {}
        for row in rows:
            url = row['url']
            visits = row['visits']
            all_urls[url] = visits

        print(f"Total URLs found: {len(all_urls)}")

        # Now apply the REMOVEBUCKET transformation
        for url, visits in all_urls.items():
            new_url = url
            transformed = False

            # Check each category -> facets mapping
            for cat, facets in cat_facet_map.items():
                # Extract main category ID (e.g., 432353 from mode_432353)
                cat_id = cat.split('_')[-1] if '_' in cat else cat

                # Check if URL belongs to this category (category ID in path)
                if cat_id in url:
                    # Try to remove each facet for this category
                    for facet in facets:
                        if f"{facet}~" in new_url:
                            new_url = remove_bucket_from_url(new_url, facet)
                            transformed = True

            if transformed and new_url != url:
                results.append({
                    'original': url,
                    'canonical': new_url,
                    'visits': visits
                })

        print(f"Transformed URLs: {len(results)}")

        # Save results
        output_path = '/tmp/canonicals_output_removebucket.csv'
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(['original', 'canonical', 'visits'])
            for r in sorted(results, key=lambda x: x['visits'], reverse=True):
                writer.writerow([r['original'], r['canonical'], r['visits']])

        print(f"\nSaved to: {output_path}")

        if results:
            print("\nFirst 15 transformations:")
            for r in sorted(results, key=lambda x: x['visits'], reverse=True)[:15]:
                print(f"  {r['original']}")
                print(f"    -> {r['canonical']} ({r['visits']} visits)")

    finally:
        if conn:
            return_redshift_connection(conn)

if __name__ == "__main__":
    main()
