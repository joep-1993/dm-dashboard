#!/usr/bin/env python3
"""
Clean CSV file by fixing double-encoded UTF-8 issues
"""
import csv

input_file = '/mnt/c/Users/JoepvanSchagen/Downloads/test2 - kopie.csv'
output_file = '/mnt/c/Users/JoepvanSchagen/Downloads/test2_cleaned.csv'

# Read with UTF-8 (the file is UTF-8, but contains mojibake from double encoding)
with open(input_file, 'r', encoding='utf-8') as infile:
    lines = infile.readlines()

cleaned_lines = []
fixed_count = 0

for line in lines:
    original = line

    # Try to fix double-encoded UTF-8 by encoding to Latin-1 and decoding as UTF-8
    try:
        # This handles cases where UTF-8 bytes were interpreted as Latin-1
        fixed_line = line.encode('latin-1').decode('utf-8')
        if fixed_line != line:
            fixed_count += 1
        line = fixed_line
    except (UnicodeDecodeError, UnicodeEncodeError):
        # If it fails, keep the original
        pass

    # Remove BOM if present
    if line.startswith('\ufeff'):
        line = line[1:]

    # Normalize line endings
    line = line.replace('\r\n', '\n').replace('\r', '\n')

    cleaned_lines.append(line)

# Write as proper UTF-8
with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
    for line in cleaned_lines:
        outfile.write(line)

print(f"✓ File cleaned successfully!")
print(f"  Input:  {input_file}")
print(f"  Output: {output_file}")
print(f"  Fixed {fixed_count} lines with encoding issues")
