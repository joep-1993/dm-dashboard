#!/usr/bin/env python3
"""
Fix CSV to match goed.csv format:
1. Add UTF-8 BOM
2. Use CRLF line endings
3. Keep proper UTF-8 encoding
"""

input_file = '/mnt/c/Users/JoepvanSchagen/Downloads/test2_cleaned.csv'
output_file = '/mnt/c/Users/JoepvanSchagen/Downloads/test2_fixed.csv'

# Read the cleaned file
with open(input_file, 'r', encoding='utf-8') as infile:
    content = infile.read()

# Normalize to LF first
content = content.replace('\r\n', '\n').replace('\r', '\n')

# Convert to CRLF
content = content.replace('\n', '\r\n')

# Write with UTF-8 BOM and CRLF line endings
with open(output_file, 'w', encoding='utf-8-sig', newline='') as outfile:
    outfile.write(content)

print(f"✓ File formatted successfully!")
print(f"  Input:  {input_file}")
print(f"  Output: {output_file}")
print(f"\nChanges applied:")
print(f"  ✓ Added UTF-8 BOM (byte order mark)")
print(f"  ✓ Converted line endings to CRLF (\\r\\n)")
print(f"  ✓ Maintained proper UTF-8 encoding")
