"""V34: size-token recognition for the multi-facet rescue (opt-in).

The matcher's word passes are gated on ``len(word) >= 3`` and use fuzzy
matching, so clothing/shoe sizes like "XL" (2 chars) are never matched and
"XXL" (3 chars) matches noisily. This module recognises size tokens in a
query *deterministically* and maps them onto the live values of a
category's ``maat_*`` facet — no fuzzy matching, no static value-ID table
(value IDs differ per category, so we resolve against the search response's
own facet values instead).

Pure module: no DB, no network. Two public entry points —
``extract_sizes(keyword)`` (recogniser) and ``match_size_value(sizes,
facet_values)`` (resolver against a facet's (id, name) pairs).

Deliberately conservative: ambiguous single letters (S/M/L) are only
recognised next to an explicit "maat"/"size"/"maten" cue or as part of an
X-family token, and a recognised size is only ever *appended* when it
EXACTLY matches a real facet value — so over-recognition can't invent a
landing page that doesn't exist.
"""
from __future__ import annotations

import re

# Canonical letter sizes → accepted spellings (all lowercased). The X-family
# is unambiguous and always recognised; the bare single letters S/M/L/XS are
# gated behind a size cue (see _has_size_cue) because "l"/"m"/"s" show up as
# stray tokens far too often to treat as sizes unconditionally.
_LETTER_SIZES = {
    "XXS":  {"xxs", "2xs"},
    "XS":   {"xs", "extra small", "extra klein"},
    "S":    {"s", "small"},
    "M":    {"m", "medium"},
    "L":    {"l", "large"},
    "XL":   {"xl", "extra large", "extra groot", "x-large", "x large"},
    "XXL":  {"xxl", "2xl", "xx-large", "xx large"},
    "XXXL": {"xxxl", "3xl"},
    "XXXXL": {"xxxxl", "4xl"},
}

# Spellings that are safe without a size cue (the X-family + word forms).
_UNAMBIGUOUS = {
    sp for canon, sps in _LETTER_SIZES.items() for sp in sps
    if canon not in ("S", "M", "L", "XS", "XXS")
}
# Bare letters that need a nearby cue word.
_CUE_GATED = {"s", "m", "l", "xs", "xxs"}

# spelling → canonical, flattened.
_SPELL_TO_CANON = {sp: canon for canon, sps in _LETTER_SIZES.items() for sp in sps}

_SIZE_CUES = {"maat", "maten", "size", "sizes", "mt"}

# numeric size: 1–3 bare digits (38, 116) or a paired/ranged form (122/128,
# 122-128). 4+ digit tokens are excluded so years (2024) and EANs aren't
# mistaken for sizes.
_NUM_SINGLE = re.compile(r"^\d{1,3}$")
_NUM_PAIR = re.compile(r"^\d{1,3}\s*[/\-]\s*\d{1,3}$")
_TOKEN_RE = re.compile(r"[a-z0-9]+(?:[/\-]\d+)?", re.I)


def _digit_groups(s: str) -> frozenset:
    """The set of distinct numbers in a numeric size, so '122/128',
    '122-128' and '122 / 128' all reduce to frozenset({'122','128'})."""
    return frozenset(re.findall(r"\d{1,3}", s or ""))


def _has_size_cue(tokens: list[str]) -> bool:
    return any(t in _SIZE_CUES for t in tokens)


def extract_sizes(keyword: str) -> list[dict]:
    """Recognise size tokens in a query.

    Returns a list of dicts, one per recognised size, in query order:
      {'kind': 'letter', 'canon': 'XL', 'raw': 'xl'}
      {'kind': 'numeric', 'digits': frozenset({'122','128'}), 'raw': '122-128'}

    Letter sizes S/M/L/XS need a size cue word ('maat'/'size'/…) somewhere in
    the query; the X-family and word forms ('extra large') don't.
    """
    if not keyword:
        return []
    low = keyword.lower()
    raw_tokens = [t for t in _TOKEN_RE.findall(low)]
    plain_tokens = re.findall(r"[a-z0-9]+", low)
    cue = _has_size_cue(plain_tokens)
    out: list[dict] = []
    seen: set = set()
    consumed_idx: set = set()  # plain_tokens indices claimed by a multi-word spelling

    # multi-word spellings first ("extra large", "xx large") so their
    # component words don't ALSO match as bare letters ("large" → L).
    # Matched on TOKEN boundaries against plain_tokens — substring matching
    # mis-fires ("x large" is a substring of "xx large").
    _multi = sorted(
        ((re.findall(r"[a-z0-9]+", sp), canon)
         for sp, canon in _SPELL_TO_CANON.items() if (" " in sp or "-" in sp)),
        key=lambda x: -len(x[0]),  # longest spelling first
    )
    for words, canon in _multi:
        n = len(words)
        for i in range(len(plain_tokens) - n + 1):
            if set(range(i, i + n)) & consumed_idx:
                continue
            if plain_tokens[i:i + n] == words and canon not in seen:
                out.append({"kind": "letter", "canon": canon, "raw": " ".join(words)})
                seen.add(canon)
                consumed_idx.update(range(i, i + n))

    consumed = {plain_tokens[i] for i in consumed_idx}
    for tok in raw_tokens:
        if tok in consumed:
            continue
        if tok in _SPELL_TO_CANON:
            canon = _SPELL_TO_CANON[tok]
            if canon in seen:
                continue
            if tok in _CUE_GATED and not cue:
                continue  # bare S/M/L/XS without a "maat" cue → skip
            out.append({"kind": "letter", "canon": canon, "raw": tok})
            seen.add(canon)
        elif _NUM_PAIR.match(tok):
            dg = _digit_groups(tok)
            key = ("num", dg)
            if key not in seen:
                out.append({"kind": "numeric", "digits": dg, "raw": tok})
                seen.add(key)
        elif _NUM_SINGLE.match(tok) and cue:
            # bare numbers are only treated as sizes next to a "maat" cue, so
            # "ek 88" / "nike 90" don't masquerade as sizes.
            dg = _digit_groups(tok)
            key = ("num", dg)
            if key not in seen:
                out.append({"kind": "numeric", "digits": dg, "raw": tok})
                seen.add(key)
    return out


def _value_canon_letter(name: str) -> str | None:
    n = (name or "").strip().lower()
    return _SPELL_TO_CANON.get(n)


def match_size_value(sizes: list[dict], facet_values: list[tuple]) -> tuple | None:
    """Resolve recognised sizes against a single size facet's values.

    facet_values: list of (value_id, value_name). Returns the best
    (value_id, value_name) match or None.

    Matching is exact (no fuzz):
      - letter: canonical label equals the value's canonical label
                ('XL' == 'XL'; 'extra large' query already canonicalised).
      - numeric: digit-group set equal ('122-128' ↔ '122/128'), or a
                single-number value contained in a ranged query
                ('128' matches a '122-128' query).
    A letter match is preferred over numeric when both exist.
    """
    if not sizes or not facet_values:
        return None
    letter_canons = {s["canon"] for s in sizes if s["kind"] == "letter"}
    numeric_groups = [s["digits"] for s in sizes if s["kind"] == "numeric"]

    letter_hit = None
    numeric_hit = None
    for vid, vname in facet_values:
        if vid is None:
            continue
        vcanon = _value_canon_letter(vname)
        if vcanon and vcanon in letter_canons:
            letter_hit = letter_hit or (vid, vname)
            continue
        vdg = _digit_groups(vname)
        if not vdg:
            continue
        for q in numeric_groups:
            if vdg == q or (len(vdg) == 1 and vdg <= q):
                numeric_hit = numeric_hit or (vid, vname)
                break
    return letter_hit or numeric_hit
