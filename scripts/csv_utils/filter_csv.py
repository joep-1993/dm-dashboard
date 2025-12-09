#!/usr/bin/env python3
"""
Filter CSV to remove lines where content_top column contains ONLY numbers between > and <
Example patterns to remove: >123<, >45678<, etc.
"""
import csv
import re

input_file = '/mnt/c/Users/JoepvanSchagen/Downloads/content_upload_20251021.csv'
output_file = '/mnt/c/Users/JoepvanSchagen/Downloads/content_upload_20251021_filtered.csv'

# Pattern to match >ONLY_NUMBERS< (no other characters between > and <)
pattern = re.compile(r'>\d+<')

removed_count = 0
kept_count = 0

with open(input_file, 'r', encoding='utf-8-sig') as infile, \
     open(output_file, 'w', encoding='utf-8', newline='') as outfile:

    reader = csv.DictReader(infile, delimiter=';')
    writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames, delimiter=';')
    writer.writeheader()

    for row in reader:
        content_top = row.get('content_top', '')

        # Check if content_top contains >numbers< pattern
        if pattern.search(content_top):
            removed_count += 1
        else:
            writer.writerow(row)
            kept_count += 1

print(f"Processing complete!")
print(f"Removed: {removed_count} lines")
print(f"Kept: {kept_count} lines")
print(f"Output saved to: {output_file}")
