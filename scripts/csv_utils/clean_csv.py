#!/usr/bin/env python3
"""
Clean CSV file:
1. Convert weird symbols to proper UTF-8
2. Remove weird end-of-line characters
3. Normalize line endings
"""
import csv
import re

input_file = '/mnt/c/Users/JoepvanSchagen/Downloads/test2 - kopie.csv'
output_file = '/mnt/c/Users/JoepvanSchagen/Downloads/test2_cleaned.csv'

# Read the file with Latin-1 encoding (which often contains these artifacts)
# Then we'll write it back as proper UTF-8
with open(input_file, 'r', encoding='latin-1') as infile:
    content = infile.read()

# Common replacements for mojibake (encoding artifacts)
replacements = {
    'â€"': '—',  # em dash
    'â€"': '–',  # en dash
    'â€˜': ''',  # left single quote
    'â€™': ''',  # right single quote
    'â€œ': '"',  # left double quote
    'â€': '"',   # right double quote
    'â€¦': '…',  # ellipsis
    'Ã©': 'é',
    'Ã¨': 'è',
    'Ã«': 'ë',
    'Ã¯': 'ï',
    'Ã´': 'ô',
    'Ã¶': 'ö',
    'Ã¼': 'ü',
    'Ã§': 'ç',
    'Ã€': 'À',
    'Ã‰': 'É',
    'Ãˆ': 'È',
    'ÃŠ': 'Ê',
    'Ã': 'Ï',
    'Ã"': 'Ô',
    'Ã–': 'Ö',
    'Ãœ': 'Ü',
    'Ã‡': 'Ç',
    'Ã¡': 'á',
    'Ã ': 'à',
    'Ã¢': 'â',
    'Ã£': 'ã',
    'Ã¤': 'ä',
    'Ã¥': 'å',
    'Ã': 'Á',
    'Ã‚': 'Â',
    'Ãƒ': 'Ã',
    'Ã„': 'Ä',
    'Ã…': 'Å',
    'Ã­': 'í',
    'Ã¬': 'ì',
    'Ã®': 'î',
    'Ã': 'Í',
    'ÃŒ': 'Ì',
    'ÃŽ': 'Î',
    'Ã³': 'ó',
    'Ã²': 'ò',
    'Ãµ': 'õ',
    'Ó': 'Ó',
    'Ò': 'Ò',
    'Ô': 'Ô',
    'Õ': 'Õ',
    'Ãº': 'ú',
    'Ã¹': 'ù',
    'Ã»': 'û',
    'Ãš': 'Ú',
    'Ã™': 'Ù',
    'Ã›': 'Û',
    'Ã±': 'ñ',
    'Ñ': 'Ñ',
    'Ã½': 'ý',
    'Ã¿': 'ÿ',
    'Ý': 'Ý',
}

# Apply all replacements
for old, new in replacements.items():
    content = content.replace(old, new)

# Remove weird line endings and normalize to Unix-style \n
content = content.replace('\r\n', '\n')
content = content.replace('\r', '\n')

# Remove any remaining control characters except newlines and tabs
content = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', content)

# Write as proper UTF-8
with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
    outfile.write(content)

print(f"✓ File cleaned successfully!")
print(f"  Input:  {input_file}")
print(f"  Output: {output_file}")
print(f"\nFixed encoding issues and normalized line endings.")
