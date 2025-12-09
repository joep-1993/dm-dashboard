#!/usr/bin/env python3
"""
Remove malformed rows from CSV file
"""
import csv

input_file = '/mnt/c/Users/JoepvanSchagen/Downloads/test2_final.csv'
output_file = '/mnt/c/Users/JoepvanSchagen/Downloads/test2_clean_final.csv'

removed_rows = []
kept_rows = 0

with open(input_file, 'r', encoding='utf-8-sig') as infile, \
     open(output_file, 'w', encoding='utf-8-sig', newline='') as outfile:

    reader = csv.DictReader(infile, delimiter=';')
    writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames, delimiter=';', quoting=csv.QUOTE_MINIMAL)

    writer.writeheader()

    for i, row in enumerate(reader, start=2):
        try:
            # Validate row
            url = row.get('url', '')
            lang = row.get('country_language', '')
            content_top = row.get('content_top', '')

            # Check for None values (malformed CSV)
            if url is None or lang is None or content_top is None:
                removed_rows.append((i, 'None values detected'))
                continue

            # Validate URL starts with /
            if not url.strip().startswith('/'):
                removed_rows.append((i, f'Invalid URL: {url[:50]}...'))
                continue

            # Validate language is not empty
            if not lang.strip():
                removed_rows.append((i, 'Empty language field'))
                continue

            # Validate content_top is not empty
            if not content_top.strip():
                removed_rows.append((i, 'Empty content_top field'))
                continue

            # Row is valid, write it
            writer.writerow(row)
            kept_rows += 1

        except Exception as e:
            removed_rows.append((i, f'Error: {str(e)}'))

print(f"✓ File cleaned successfully!")
print(f"  Input:  {input_file}")
print(f"  Output: {output_file}")
print(f"\nResults:")
print(f"  Kept: {kept_rows:,} rows")
print(f"  Removed: {len(removed_rows):,} rows")

if removed_rows:
    print(f"\nRemoved rows (showing first 10):")
    for line_num, reason in removed_rows[:10]:
        print(f"  Line {line_num}: {reason}")
