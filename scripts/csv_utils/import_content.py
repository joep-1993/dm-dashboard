"""
DUPLICATE — superseded by backend/import_content.py.

Both scripts did the same thing pre Big Bang refactor. The backend version
has been migrated to the new schema (pa.urls + pa.kopteksten_content +
pa.kopteksten_jobs); this scripts/ copy was never updated.

Use:
    python -m backend.import_content
"""

def main():
    print("Use backend/import_content.py instead — this script is a stale duplicate.")

if __name__ == "__main__":
    main()
