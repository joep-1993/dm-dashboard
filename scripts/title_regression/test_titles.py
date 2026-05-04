"""
Regression test harness for the unique-titles AI generation pipeline.

Records baseline outputs from `generate_title_from_api` against a fixed
fixture of URLs, then re-runs and classifies any diff so a code change can
be auto-checked against the baseline before being flipped on for production.

Usage:
    # Capture or refresh the baseline (run once after a code change you have
    # explicitly decided to bless as the new ground truth):
    PYTHONPATH=. python3 scripts/title_regression/test_titles.py --record

    # Compare current code against the recorded baseline:
    PYTHONPATH=. python3 scripts/title_regression/test_titles.py --compare

    # Compare with non-default kwargs (e.g. test the v2 polish prompt):
    PYTHONPATH=. python3 scripts/title_regression/test_titles.py --compare \
        --prompt-mode v2 --halluc-mode v1

The fixture (fixture_urls.json) is a 30-URL sample covering 10 main
categories (gezond_mooi, elektronica, huis_tuin, speelgoed_spelletjes,
huishoudelijke_apparatuur, sieraden_horloges, meubilair, schoenen,
mode_accessoires, klussen) — 3 URLs each. Each URL has at least 2 selected
facets so the AI path is exercised.

Diff classification (in order of severity):
    IDENTICAL       no change.
    WHITESPACE      only whitespace differs.
    CASE            only capitalisation differs.
    WORD_ORDER      same multiset of words, different order. Often acceptable
                    (Dutch grammar permits multiple valid orderings) but worth
                    eyeballing.
    WORDS_DROPPED   baseline has words new doesn't. Likely real regression
                    (silent data loss like the v2 hallucination guard
                    accidentally dropping "make-up" / "No-Frost").
    WORDS_ADDED     new has words baseline doesn't. Potential hallucination —
                    a word leaked through the guard that wasn't in input.
    DIFFERENT       both added and dropped, or otherwise unclassifiable.

Quality probes run on every output regardless of diff:
    - has_consecutive_repeat: any token repeated immediately ("X X")
    - has_compound_repeat: any compound-word duplication
      ("massageolie" + "massage olie") that the existing dedupe missed
    - has_known_hallucination_word: contains a hardcoded common-hallucination
      word (Heren/Dames/Nieuwe/etc.) that's NOT in the original facet input
    - facets_missing_count: number of facet detail_values absent from output
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
except Exception:
    pass

from backend.ai_titles_service import generate_title_from_api  # noqa: E402
from backend.faq_service import fetch_products_api  # noqa: E402

FIXTURE_PATH = Path(__file__).parent / "fixture_urls.json"
BASELINE_PATH = Path(__file__).parent / "baseline.json"

KNOWN_HALLUCINATION_WORDS = {
    'heren', 'dames', 'kinderen', 'jongens', 'meisjes', 'baby',
    'nieuwe', 'nieuw', 'goedkope', 'goedkoop', 'beste', 'kwaliteit',
}


def _tokenize(s: str) -> List[str]:
    return [t for t in re.split(r"\s+", s.strip()) if t]


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip()).lower()


def classify_diff(baseline: Optional[str], current: Optional[str]) -> str:
    if baseline is None and current is None:
        return "IDENTICAL"
    if baseline is None or current is None:
        return "DIFFERENT"
    if baseline == current:
        return "IDENTICAL"
    if baseline.strip() == current.strip():
        return "WHITESPACE"
    if _norm(baseline) == _norm(current):
        return "CASE"
    bt = [t.lower() for t in _tokenize(baseline)]
    ct = [t.lower() for t in _tokenize(current)]
    if Counter(bt) == Counter(ct):
        return "WORD_ORDER"
    bset = Counter(bt)
    cset = Counter(ct)
    only_baseline = bset - cset
    only_current = cset - bset
    if only_baseline and not only_current:
        return "WORDS_DROPPED"
    if only_current and not only_baseline:
        return "WORDS_ADDED"
    return "DIFFERENT"


def quality_probes(title: Optional[str], facet_values: List[str]) -> Dict:
    if not title:
        return {
            "has_consecutive_repeat": False,
            "has_compound_repeat": False,
            "has_known_hallucination_word": False,
            "facets_missing_count": 0,
        }
    tokens = _tokenize(title)
    lower_tokens = [t.lower() for t in tokens]

    consecutive_repeat = any(
        lower_tokens[i] == lower_tokens[i + 1] and len(lower_tokens[i]) >= 3
        for i in range(len(lower_tokens) - 1)
    )

    # Compound repeat: any word X also appears as two adjacent words a+b where a+b == X.
    word_set = set(lower_tokens)
    compound_repeat = False
    for i in range(len(lower_tokens) - 1):
        a, b = lower_tokens[i], lower_tokens[i + 1]
        joined = a + b
        if len(joined) >= 6 and joined in word_set:
            compound_repeat = True
            break

    # Known hallucination word that's not present (case-insensitive) in any facet value.
    facet_blob = " ".join(facet_values).lower()
    has_halluc = any(
        re.search(r"\b" + re.escape(w) + r"\b", title.lower()) and w not in facet_blob
        for w in KNOWN_HALLUCINATION_WORDS
    )

    # Facet detail_values absent from the output (case-insensitive substring).
    title_low = title.lower()
    missing = sum(
        1 for v in facet_values
        if v and v.lower() not in title_low
    )

    return {
        "has_consecutive_repeat": consecutive_repeat,
        "has_compound_repeat": compound_repeat,
        "has_known_hallucination_word": has_halluc,
        "facets_missing_count": missing,
    }


def run_one(url: str, kwargs: Dict) -> Dict:
    t0 = time.time()
    try:
        result = generate_title_from_api(url, **kwargs)
    except Exception as e:
        return {"url": url, "error": str(e), "elapsed_ms": (time.time() - t0) * 1000}
    if not result:
        return {"url": url, "error": "no_result", "elapsed_ms": (time.time() - t0) * 1000}
    return {
        "url": url,
        "h1_title": result.get("h1_title"),
        "original_h1": result.get("original_h1"),
        "elapsed_ms": (time.time() - t0) * 1000,
    }


def collect_facet_values(url: str) -> List[str]:
    """Re-fetch to capture facet values for quality probes — cached when possible."""
    page = fetch_products_api(url, include_related=False)
    if not page:
        return []
    return [f.get("detail_value", "") for f in page.get("selected_facets", []) if f.get("detail_value")]


def record(kwargs: Dict, fixture: List[str]) -> None:
    """Run the pipeline against the fixture and write baseline.json."""
    print(f"Recording baseline with kwargs={kwargs} on {len(fixture)} URLs...")
    baseline = {"kwargs": kwargs, "results": {}}
    for i, url in enumerate(fixture, 1):
        r = run_one(url, kwargs)
        facet_values = collect_facet_values(url)
        baseline["results"][url] = {
            "h1_title": r.get("h1_title"),
            "elapsed_ms": round(r.get("elapsed_ms", 0)),
            "error": r.get("error"),
            "facet_values": facet_values,
        }
        print(f"  [{i:02d}/{len(fixture)}] {r.get('h1_title') or r.get('error')}")
    BASELINE_PATH.write_text(json.dumps(baseline, indent=2, ensure_ascii=False))
    print(f"\nWrote baseline → {BASELINE_PATH}")


def compare(kwargs: Dict, fixture: List[str]) -> int:
    """Run the pipeline and compare against baseline.json. Returns exit code (0 OK, 1 regressions)."""
    if not BASELINE_PATH.exists():
        print(f"ERROR: no baseline at {BASELINE_PATH}. Run --record first.")
        return 2
    baseline = json.loads(BASELINE_PATH.read_text())
    base_results = baseline["results"]
    print(f"Comparing kwargs={kwargs} against baseline (kwargs={baseline['kwargs']}) "
          f"on {len(fixture)} URLs...\n")

    counts = Counter()
    rows = []
    quality_diffs = Counter()
    timings_baseline: List[float] = []
    timings_current: List[float] = []
    for i, url in enumerate(fixture, 1):
        b = base_results.get(url, {})
        b_title = b.get("h1_title")
        c = run_one(url, kwargs)
        c_title = c.get("h1_title")
        cls = classify_diff(b_title, c_title)
        counts[cls] += 1
        timings_baseline.append(b.get("elapsed_ms", 0))
        timings_current.append(c.get("elapsed_ms", 0))
        facet_values = b.get("facet_values", [])
        b_q = quality_probes(b_title, facet_values)
        c_q = quality_probes(c_title, facet_values)
        # Track quality regressions only (current worse than baseline).
        for k in ("has_consecutive_repeat", "has_compound_repeat",
                  "has_known_hallucination_word"):
            if c_q[k] and not b_q[k]:
                quality_diffs[f"introduced_{k}"] += 1
            if not c_q[k] and b_q[k]:
                quality_diffs[f"fixed_{k}"] += 1
        if c_q["facets_missing_count"] > b_q["facets_missing_count"]:
            quality_diffs["dropped_more_facets"] += 1
        elif c_q["facets_missing_count"] < b_q["facets_missing_count"]:
            quality_diffs["preserved_more_facets"] += 1
        rows.append({
            "i": i,
            "url": url,
            "class": cls,
            "baseline": b_title,
            "current": c_title,
            "b_quality": b_q,
            "c_quality": c_q,
        })

    # Print every non-IDENTICAL row.
    print("=" * 90)
    print(f"{'#':<4}{'CLASS':<16} BASELINE / CURRENT")
    print("=" * 90)
    for row in rows:
        marker = "" if row["class"] == "IDENTICAL" else "▲"
        print(f"{row['i']:<4}{row['class']:<16}{marker}")
        if row["class"] != "IDENTICAL":
            print(f"     B: {row['baseline']}")
            print(f"     C: {row['current']}")
            # Show quality regressions inline.
            for k in ("has_consecutive_repeat", "has_compound_repeat",
                      "has_known_hallucination_word"):
                if row["c_quality"][k] and not row["b_quality"][k]:
                    print(f"     ! introduced_{k}")
            if row["c_quality"]["facets_missing_count"] > row["b_quality"]["facets_missing_count"]:
                delta = row["c_quality"]["facets_missing_count"] - row["b_quality"]["facets_missing_count"]
                print(f"     ! dropped {delta} more facet(s)")

    print("\n" + "=" * 90)
    print("SUMMARY")
    print("=" * 90)
    for cls in ("IDENTICAL", "WHITESPACE", "CASE", "WORD_ORDER",
                "WORDS_DROPPED", "WORDS_ADDED", "DIFFERENT"):
        if counts[cls]:
            print(f"  {cls:<16} {counts[cls]}")
    print()
    if quality_diffs:
        print("QUALITY DELTAS")
        for k, v in quality_diffs.most_common():
            print(f"  {k:<40} {v}")
    print()
    import statistics as _st
    print(f"Timings (ms)  baseline median={_st.median(timings_baseline):.0f}  "
          f"current median={_st.median(timings_current):.0f}")

    # Verdict: any introduced quality regression OR any WORDS_DROPPED is a fail.
    introduced = sum(v for k, v in quality_diffs.items() if k.startswith("introduced_"))
    dropped_more = quality_diffs.get("dropped_more_facets", 0)
    words_dropped_class = counts.get("WORDS_DROPPED", 0)
    if introduced or dropped_more or words_dropped_class:
        print(f"\nFAIL: {introduced} introduced quality regressions, "
              f"{dropped_more} dropped-more-facets, "
              f"{words_dropped_class} WORDS_DROPPED rows.")
        return 1
    if counts.get("WORDS_ADDED", 0):
        print(f"\nWARN: {counts['WORDS_ADDED']} WORDS_ADDED rows — verify no hallucinations.")
    print("\nOK: no hard regressions detected.")
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--record", action="store_true", help="Capture baseline.json from current code.")
    p.add_argument("--compare", action="store_true", help="Compare current code to baseline.json.")
    p.add_argument("--prompt-mode", default="v1", choices=["v1", "v2"])
    p.add_argument("--halluc-mode", default="v1", choices=["v1", "v2"])
    args = p.parse_args()

    if not (args.record or args.compare):
        p.error("pass --record or --compare")
    fixture = json.loads(FIXTURE_PATH.read_text())
    kwargs = {"prompt_mode": args.prompt_mode, "halluc_mode": args.halluc_mode}
    if args.record:
        record(kwargs, fixture)
        return 0
    return compare(kwargs, fixture)


if __name__ == "__main__":
    sys.exit(main())
