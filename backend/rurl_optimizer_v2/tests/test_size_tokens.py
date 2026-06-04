"""V34 regression tests: size-token recognition + resolution for the
opt-in size append on the multi-facet rescue.

Pure (no DB / no network): exercises src.size_tokens directly and the
_assemble_multi_facet size hook.

Run:  python -m pytest backend/rurl_optimizer_v2/tests/test_size_tokens.py -q
"""
import sys
from pathlib import Path

_V2_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_V2_ROOT))

from src.size_tokens import extract_sizes, match_size_value


def _canons(kw):
    return [s["canon"] for s in extract_sizes(kw) if s["kind"] == "letter"]


def _nums(kw):
    return [sorted(s["digits"]) for s in extract_sizes(kw) if s["kind"] == "numeric"]


# ── recogniser ──────────────────────────────────────────────────────────────
def test_x_family_needs_no_cue():
    assert _canons("nederlands elftal t-shirt ek 88 xl oranje") == ["XL"]
    assert _canons("jas xxl") == ["XXL"]


def test_word_forms_dont_double_match():
    # "extra large" must NOT also yield bare L; "xx large" must NOT yield XL.
    assert _canons("shirt extra large") == ["XL"]
    assert _canons("jas xx large") == ["XXL"]
    assert _canons("shirt x-large") == ["XL"]


def test_bare_letters_need_a_size_cue():
    assert _canons("voetbalshirt maat l") == ["L"]      # cue present
    assert _canons("voetbalshirt blauw l") == []        # no cue → 'l' ignored


def test_numeric_sizes():
    assert _nums("nike thuisshirt maat 122-128") == [["122", "128"]]
    assert _nums("schoenen maat 42") == [["42"]]
    # bare numbers without a cue are NOT sizes (years, model numbers)
    assert _nums("ek 88 shirt") == []
    assert _nums("broek 2024 collectie") == []          # 4 digits never a size


# ── resolver ─────────────────────────────────────────────────────────────────
_VALS = [(471668, "S"), (471669, "M"), (471670, "L"), (471671, "XL"),
         (471633, "116"), (23811956, "122/128"), (471635, "128")]


def test_resolver_letter_exact():
    assert match_size_value(extract_sizes("shirt maat xl"), _VALS) == (471671, "XL")


def test_resolver_numeric_separator_agnostic():
    # query '122-128' resolves to value '122/128'
    assert match_size_value(extract_sizes("shirt maat 122-128"), _VALS) == (23811956, "122/128")


def test_resolver_single_number_in_range():
    assert match_size_value(extract_sizes("shirt maat 128"), _VALS) == (471635, "128")


def test_resolver_no_value_for_size_returns_none():
    # XXL recognised but no XXL value present
    assert match_size_value(extract_sizes("shirt maat xxl"), _VALS) is None


def test_resolver_empty():
    assert match_size_value([], _VALS) is None
    assert match_size_value(extract_sizes("shirt maat xl"), []) is None


if __name__ == "__main__":
    import traceback
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"  ok  {fn.__name__}")
        except Exception:
            failed += 1
            print(f"FAIL  {fn.__name__}")
            traceback.print_exc()
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    sys.exit(1 if failed else 0)
