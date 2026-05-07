"""
Deduplicate URLs in kopteksten content.

OBSOLETE after the Big Bang refactor (2026-05-07). The new
pa.kopteksten_content table is keyed on url_id (PK), so duplicate URLs
are structurally impossible. The 60 rows that this script would have
caught were silently merged during the step 2 backfill via
INSERT ... ON CONFLICT (url_id) DO NOTHING.

If you somehow ended up here looking for a dedup tool, what you really
want is the canonicalize_url() function in backend/url_catalog.py. It
applies the canonicalization rules at insert time, which is what
prevents duplicates from accumulating in the first place.

Kept as a no-op stub so anyone running this from muscle memory gets a
clear message instead of a SQL error.
"""

def main():
    print("="*70)
    print("CONTENT DEDUPLICATION (OBSOLETE)")
    print("="*70)
    print(
        "\nThis script is a no-op. After the Big Bang schema refactor,\n"
        "pa.kopteksten_content is keyed on url_id, which makes duplicate\n"
        "URLs structurally impossible. There is nothing to dedupe.\n\n"
        "See backend/url_catalog.py::canonicalize_url() for how URLs are\n"
        "normalized at insert time so this problem can't recur."
    )

if __name__ == "__main__":
    main()
