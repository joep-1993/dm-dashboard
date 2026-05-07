"""URL catalog helpers — single source of truth for URL → url_id mapping.

After the Big Bang refactor every per-tool table (kopteksten_jobs, faq_jobs,
unique_titles_jobs, *_content, url_validation) keys on pa.urls.url_id instead
of the URL string. Use these helpers to resolve a URL to its catalog id; they
canonicalize on the way in so callers don't have to.
"""
from typing import Iterable, Dict, List, Optional


def canonicalize_url(url: str) -> Optional[str]:
    """Mirror of pa.canonicalize_url() in SQL. Returns None for unusable input.

    Rules:
      - strip protocol+host, leading './'
      - reject 'undefined' / non-'/' inputs
      - strip query/fragment
      - trailing slash by structure: '/c/' → no trailing /, otherwise yes
    """
    if not url or url == 'undefined':
        return None
    s = url
    # Strip Beslist host prefix; reject any other absolute URL.
    s_lower = s.lower()
    BESLIST_PREFIXES = (
        'https://www.beslist.nl', 'http://www.beslist.nl',
        'https://beslist.nl',     'http://beslist.nl',
    )
    if s_lower.startswith(('http://', 'https://')):
        matched = False
        for p in BESLIST_PREFIXES:
            if s_lower.startswith(p):
                s = s[len(p):]
                matched = True
                break
        if not matched:
            return None
        if not s:
            s = '/'
    # Fix leading ./
    if s.startswith('./'):
        s = s[1:]
    if not s.startswith('/'):
        return None
    # Strip query / fragment
    s = s.split('?', 1)[0].split('#', 1)[0]
    if '/c/' in s:
        s = s.rstrip('/')
    elif not s.endswith('/'):
        s = s + '/'
    if not s or s == '':
        return None
    return s


def get_url_id(cur, url: str, *, create: bool = True) -> Optional[int]:
    """Resolve a (raw) URL to its pa.urls.url_id, creating the row if missing.

    Returns None if the URL cannot be canonicalized.
    Caller is responsible for committing.
    """
    canonical = canonicalize_url(url)
    if canonical is None:
        return None
    if create:
        cur.execute(
            "INSERT INTO pa.urls (url) VALUES (%s) ON CONFLICT (url) DO NOTHING",
            (canonical,),
        )
    cur.execute("SELECT url_id FROM pa.urls WHERE url = %s", (canonical,))
    row = cur.fetchone()
    if not row:
        return None
    # Cursor row may be RealDictRow (psycopg2 with RealDictCursor) or tuple
    return row['url_id'] if isinstance(row, dict) or hasattr(row, 'get') and 'url_id' in row else row[0]


def bulk_upsert_urls(cur, urls: Iterable[str]) -> Dict[str, int]:
    """Insert many URLs into pa.urls in one round-trip; return {canonical_url: url_id}.

    URLs that fail canonicalization are silently dropped. Caller is
    responsible for committing.
    """
    canonicals: List[str] = []
    seen = set()
    for u in urls:
        c = canonicalize_url(u)
        if c and c not in seen:
            seen.add(c)
            canonicals.append(c)
    if not canonicals:
        return {}
    # Insert missing
    args_str = ','.join(cur.mogrify('(%s)', (c,)).decode('utf-8') for c in canonicals)
    cur.execute(
        f"INSERT INTO pa.urls (url) VALUES {args_str} ON CONFLICT (url) DO NOTHING"
    )
    # Look up all url_ids
    cur.execute("SELECT url, url_id FROM pa.urls WHERE url = ANY(%s)", (canonicals,))
    out = {}
    for row in cur.fetchall():
        if isinstance(row, dict) or (hasattr(row, 'get') and 'url' in row):
            out[row['url']] = row['url_id']
        else:
            out[row[0]] = row[1]
    return out
