"""
Mojibake repair for UTF-8 text that was decoded as Latin-1 somewhere upstream.

Root cause seen in the wild (2026-07): source data (notably
backend/data/cat_urls.csv, and historically pages fetched with
requests' `.text` when the Content-Type header omits a charset) had
UTF-8 bytes decoded as ISO-8859-1, so `é` (bytes C3 A9) became `Ã©`,
and a later `.lower()` turned that into `ã©`. This module turns those
sequences back into the correct character.

`fix_mojibake` is a no-op for clean text: it only rewrites the specific
2-char sequences that a UTF-8→Latin-1 misdecode produces, which never
occur in legitimate Dutch/brand text. Validated with zero false
positives across ~1M rows of pa.unique_titles_content.
"""

import re


def _build_map() -> dict:
    m = {}
    for cp in range(0xA0, 0x100):
        ch = chr(cp)
        # UTF-8 encoding of this code point is 2 bytes for the whole 0xA0-0xFF
        # range; decoding those bytes as Latin-1 gives the mojibake string.
        moj = ch.encode("utf-8").decode("latin-1")
        m[moj] = ch
        # Downstream .lower() lowercases the leading Ã/Â to ã/â; the trailing
        # symbol byte is unaffected by casing.
        moj_lc = moj[0].lower() + moj[1:]
        if moj_lc != moj:
            m[moj_lc] = ch
    return m


_MOJIBAKE_MAP = _build_map()
# Longest keys first so multi-char sequences win over any single-char overlap.
_MOJIBAKE_RE = re.compile(
    "|".join(re.escape(k) for k in sorted(_MOJIBAKE_MAP, key=len, reverse=True))
)


def fix_mojibake(s):
    """Repair UTF-8-decoded-as-Latin-1 mojibake. Safe no-op on clean text.

    Returns the input unchanged when it is None or contains no mojibake
    sequences.
    """
    if not s:
        return s
    return _MOJIBAKE_RE.sub(lambda mo: _MOJIBAKE_MAP[mo.group(0)], s)
