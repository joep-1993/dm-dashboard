"""
Add visits and revenue to the facet output file.

For each facet row, sums visits and revenue from all Redshift URLs
that contain that facet's bucket value in the URL path.
"""
import csv
import pandas as pd
from collections import defaultdict

URL_DATA = '/home/joepvanschagen/projects/dm-tools/backend/url_visits_revenue.csv'
FACET_FILE = '/home/joepvanschagen/projects/dm-tools/backend/faet_values_new_output.xlsx'


def main():
    print("Loading URL visits/revenue data...")
    url_data = []
    with open(URL_DATA, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            url_data.append({
                'url': row['url'],
                'visits': int(row['visits']),
                'revenue': float(row['revenue']) if row['revenue'] else 0.0,
            })
    print(f"  Loaded {len(url_data)} URLs")

    print("Loading facet output file...")
    facets_df = pd.read_excel(FACET_FILE, sheet_name='facets')
    cats_df = pd.read_excel(FACET_FILE, sheet_name='cats')
    print(f"  Loaded {len(facets_df)} facet rows")

    # Build a lookup: bucket_value -> list of facet row indices
    # Each facet row has a 'bucket' like 'merk~482723'
    # URLs look like '/products/.../c/merk~482723~~soort~123'
    # A facet's visits = sum of all URLs containing its bucket
    print("Building bucket index...")
    buckets = facets_df['bucket'].tolist()
    unique_buckets = set(b for b in buckets if isinstance(b, str) and b)
    print(f"  {len(unique_buckets)} unique buckets")

    # For each URL, find which buckets it contains and accumulate visits/revenue
    bucket_visits = defaultdict(int)
    bucket_revenue = defaultdict(float)

    print("Matching URLs to buckets...")
    for i, ud in enumerate(url_data):
        url = ud['url']
        # Extract the facet part after /c/
        c_idx = url.find('/c/')
        if c_idx == -1:
            continue
        facet_path = url[c_idx + 3:]

        # Split into individual bucket segments
        # URLs use ~~ as separator between facets: 'merk~482723~~soort~123'
        url_buckets = facet_path.split('~~')

        for ub in url_buckets:
            if ub in unique_buckets:
                bucket_visits[ub] += ud['visits']
                bucket_revenue[ub] += ud['revenue']

        if (i + 1) % 500000 == 0:
            print(f"  Processed {i+1}/{len(url_data)} URLs...")

    print(f"  Matched {len(bucket_visits)} buckets with visits data")

    # Write back to dataframe
    print("Writing visits and revenue to facet rows...")
    visits_col = []
    revenue_col = []
    for bucket in buckets:
        if isinstance(bucket, str) and bucket:
            visits_col.append(bucket_visits.get(bucket, 0))
            revenue_col.append(round(bucket_revenue.get(bucket, 0), 2))
        else:
            visits_col.append(0)
            revenue_col.append(0)

    facets_df['visits'] = visits_col
    facets_df['revenue'] = revenue_col

    # Save
    print(f"Saving to {FACET_FILE}...")
    with pd.ExcelWriter(FACET_FILE, engine='openpyxl') as writer:
        facets_df.to_excel(writer, sheet_name='facets', index=False)
        cats_df.to_excel(writer, sheet_name='cats', index=False)

    total_visits = sum(visits_col)
    total_revenue = sum(revenue_col)
    non_zero = sum(1 for v in visits_col if v > 0)
    print(f"\nDone!")
    print(f"  Facets with visits: {non_zero}/{len(facets_df)}")
    print(f"  Total visits: {total_visits:,}")
    print(f"  Total revenue: {total_revenue:,.2f}")


if __name__ == '__main__':
    main()
