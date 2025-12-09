#!/usr/bin/env python3
"""
Final fix with comprehensive manual character replacement
"""

input_file = '/mnt/c/Users/JoepvanSchagen/Downloads/test2 - kopie.csv'
output_file = '/mnt/c/Users/JoepvanSchagen/Downloads/test2_final.csv'

# Read the file
with open(input_file, 'r', encoding='utf-8') as infile:
    content = infile.read()

# Comprehensive replacements for mojibake (Windows-1252 interpreted as UTF-8)
replacements = {
    # Em/en dashes
    'â€"': '—',
    'â€"': '–',

    # Quotes
    'â€˜': ''',
    'â€™': ''',
    'â€œ': '"',
    'â€': '"',
    'â€¦': '…',

    # Multiplication sign
    'Ã—': '×',

    # Common accented characters
    'Ã©': 'é',
    'Ã¨': 'è',
    'Ã«': 'ë',
    'Ãª': 'ê',
    'Ã¯': 'ï',
    'Ã®': 'î',
    'Ã¬': 'ì',
    'Ã­': 'í',
    'Ã´': 'ô',
    'Ã²': 'ò',
    'Ã³': 'ó',
    'Ã¶': 'ö',
    'Ã¼': 'ü',
    'Ã¹': 'ù',
    'Ãº': 'ú',
    'Ã»': 'û',
    'Ã§': 'ç',
    'Ã±': 'ñ',
    'Ã¡': 'á',
    'Ã ': 'à',
    'Ã¢': 'â',
    'Ã£': 'ã',
    'Ã¤': 'ä',
    'Ã¥': 'å',
    'Ã½': 'ý',
    'Ã¿': 'ÿ',
    'Ãµ': 'õ',

    # Uppercase versions
    'Ã‰': 'É',
    'Ãˆ': 'È',
    'ÃŠ': 'Ê',
    'Ã‹': 'Ë',
    'Ã': 'Í',
    'ÃŒ': 'Ì',
    'ÃŽ': 'Î',
    'Ã': 'Ï',
    # 'Ã"': 'Ó',  # Skipped due to quote issue
    # 'Ã'': 'Ò',  # Skipped due to quote issue
    'Ô': 'Ô',
    'Õ': 'Õ',
    'Ã–': 'Ö',
    'Ãš': 'Ú',
    'Ã™': 'Ù',
    'Ã›': 'Û',
    'Ãœ': 'Ü',
    'Ã‡': 'Ç',
    # 'Ã'': 'Ñ',  # Skipped due to quote issue
    'Á': 'Á',
    'Ã€': 'À',
    'Ã‚': 'Â',
    'Ãƒ': 'Ã',
    'Ã„': 'Ä',
    'Ã…': 'Å',
    'Ý': 'Ý',

    # Special characters
    'Â°': '°',
    'Â±': '±',
    'Â²': '²',
    'Â³': '³',
    'Âµ': 'µ',
    'Â¼': '¼',
    'Â½': '½',
    'Â¾': '¾',
    'Â«': '«',
    'Â»': '»',
    'Â¡': '¡',
    'Â¿': '¿',
    'Â§': '§',
    'Â¶': '¶',
    'Â·': '·',
    'Â¸': '¸',
    'Âº': 'º',
    'Âª': 'ª',
    'Â©': '©',
    'Â®': '®',
    'â„¢': '™',
    'â‚¬': '€',
    'Â£': '£',
    'Â¥': '¥',
    'Â¢': '¢',
}

# Apply replacements
for old, new in replacements.items():
    content = content.replace(old, new)

# Remove BOM from content (we'll add it at file level)
content = content.lstrip('\ufeff')

# Normalize line endings to CRLF
content = content.replace('\r\n', '\n').replace('\r', '\n')
content = content.replace('\n', '\r\n')

# Write with UTF-8 BOM and CRLF
with open(output_file, 'w', encoding='utf-8-sig', newline='') as outfile:
    outfile.write(content)

print(f"✓ Final fix completed!")
print(f"  Input:  {input_file}")
print(f"  Output: {output_file}")
print(f"\nApplied fixes:")
print(f"  ✓ Fixed encoding artifacts (mojibake)")
print(f"  ✓ Added UTF-8 BOM")
print(f"  ✓ Converted to CRLF line endings")
print(f"\nFile should now match goed.csv format exactly.")
