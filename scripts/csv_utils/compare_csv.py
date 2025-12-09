#!/usr/bin/env python3
"""Compare structural differences between CSV files"""
import csv

files = {
    'test2 - kopie.csv': '/mnt/c/Users/JoepvanSchagen/Downloads/test2 - kopie.csv',
    'goed.csv': '/mnt/c/Users/JoepvanSchagen/Downloads/goed.csv'
}

for name, path in files.items():
    print(f"\n=== {name} ===")

    # Check BOM
    with open(path, 'rb') as f:
        first_bytes = f.read(3)
        has_bom = first_bytes == b'\xef\xbb\xbf'
        print(f"BOM (UTF-8): {has_bom}")

    # Check line endings
    with open(path, 'rb') as f:
        content = f.read(1000)
        has_crlf = b'\r\n' in content
        line_ending = 'CRLF (\\r\\n)' if has_crlf else 'LF (\\n)'
        print(f"Line endings: {line_ending}")

    # Check delimiter and quoting
    with open(path, 'r', encoding='utf-8-sig') as f:
        sample = f.read(2000)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=';,')
            print(f"Delimiter: '{dialect.delimiter}'")
            print(f"Quote char: '{dialect.quotechar}'")
            print(f"Double quote: {dialect.doublequote}")
            print(f"Line terminator: {repr(dialect.lineterminator)}")
        except Exception as e:
            print(f"Could not detect dialect: {e}")

    # Count lines and check for empty fields
    with open(path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=';')
        line_count = 0
        empty_content_top = 0
        empty_content_bottom = 0
        empty_content_faq = 0

        for row in reader:
            line_count += 1
            if not row.get('content_top', '').strip():
                empty_content_top += 1
            if not row.get('content_bottom', '').strip():
                empty_content_bottom += 1
            if not row.get('content_faq', '').strip():
                empty_content_faq += 1

        print(f"Total data lines: {line_count}")
        print(f"Empty content_top: {empty_content_top}")
        print(f"Empty content_bottom: {empty_content_bottom}")
        print(f"Empty content_faq: {empty_content_faq}")
