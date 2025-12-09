#!/usr/bin/env python3
"""
Complete fix: encoding + format to match goed.csv exactly
"""

input_file = '/mnt/c/Users/JoepvanSchagen/Downloads/test2 - kopie.csv'
output_file = '/mnt/c/Users/JoepvanSchagen/Downloads/test2_complete_fix.csv'

# Step 1: Read with UTF-8 and fix double-encoding
with open(input_file, 'r', encoding='utf-8') as infile:
    lines = infile.readlines()

fixed_lines = []
fixed_count = 0

for line in lines:
    original = line

    # Try to fix double-encoded UTF-8
    try:
        # Encode to latin-1 then decode as utf-8 to fix mojibake
        fixed_line = line.encode('latin-1').decode('utf-8')
        if fixed_line != line:
            fixed_count += 1
        line = fixed_line
    except (UnicodeDecodeError, UnicodeEncodeError):
        # If it fails, keep original
        pass

    # Remove BOM from content if present (we'll add it at file level)
    line = line.lstrip('\ufeff')

    fixed_lines.append(line)

# Step 2: Join lines and normalize line endings
content = ''.join(fixed_lines)

# Normalize to LF first
content = content.replace('\r\n', '\n').replace('\r', '\n')

# Convert to CRLF (Windows style like goed.csv)
content = content.replace('\n', '\r\n')

# Step 3: Write with UTF-8 BOM and proper encoding
with open(output_file, 'w', encoding='utf-8-sig', newline='') as outfile:
    outfile.write(content)

print(f"✓ Complete fix applied successfully!")
print(f"  Input:  {input_file}")
print(f"  Output: {output_file}")
print(f"\nFixes applied:")
print(f"  ✓ Fixed {fixed_count} lines with encoding issues")
print(f"  ✓ Added UTF-8 BOM (byte order mark)")
print(f"  ✓ Converted to CRLF line endings")
print(f"  ✓ File should now match goed.csv format")
