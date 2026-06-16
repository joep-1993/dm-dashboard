"""V41 fixes: existing-facet preservation, alphabetical ordering, spurious-brand
suppression in the search-derived append, and the probe-fallback lexical floor."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main_parallel_v2 import (
    _existing_facet_in_url, _canonicalize_facet_order, _spurious_brand_facet,
)
from src.reliability_scorer import _keyword_bridges_value, calculate_reliability_score


# ---- R2: existing facet must survive into the suggestion --------------------
def test_facet_present_single():
    assert _existing_facet_in_url(
        "https://x/products/dieren_accessoires/dieren_accessoires_480779/c/t_reismand~23795956",
        "t_reismand~23795956") is True

def test_facet_dropped_detected():
    # issue #4: max_30_kg landed on type_dierenriemen, t_reismand gone
    assert _existing_facet_in_url(
        "https://x/products/dieren_accessoires/dieren_accessoires_480637/c/type_dierenriemen~3386348",
        "t_reismand~23795956") is False

def test_facet_present_among_multi():
    assert _existing_facet_in_url(
        "https://x/products/d/d_1/c/dier_dierenbenodigdheden~480616~~t_reismand~23795956",
        "t_reismand~23795956") is True

def test_empty_existing_facet_passes():
    assert _existing_facet_in_url("https://x/products/a/a_1/", "") is True

def test_no_facet_segment_but_facet_expected():
    assert _existing_facet_in_url("https://x/products/a/a_1/", "t_reismand~23795956") is False


# ---- issue #2: canonical alphabetical facet order ---------------------------
def test_canonical_reorders_prepended_existing():
    src = "https://x/products/d/d_1/c/t_reismand~23795956~~dier_dierenbenodigdheden~480616"
    out = _canonicalize_facet_order(src)
    assert out == "https://x/products/d/d_1/c/dier_dierenbenodigdheden~480616~~t_reismand~23795956"

def test_canonical_noop_single_facet():
    src = "https://x/products/d/d_1/c/t_reismand~23795956"
    assert _canonicalize_facet_order(src) == src

def test_canonical_noop_already_sorted():
    src = "https://x/products/d/d_1/c/a_x~1~~b_y~2"
    assert _canonicalize_facet_order(src) == src

def test_canonical_preserves_trailing_slash():
    src = "https://x/products/d/d_1/c/z_a~1~~a_b~2/"
    assert _canonicalize_facet_order(src) == "https://x/products/d/d_1/c/a_b~2~~z_a~1/"


# ---- issue #3: probe-fallback lexical floor ---------------------------------
def test_no_bridge_vogelgeluiden_keuken():
    assert _keyword_bridges_value("vogelgeluiden", "Keuken") is False

def test_bridge_exact_token():
    assert _keyword_bridges_value("kunststof tuinstoel", "Kunststof") is True

def test_probe_fallback_floored_when_no_bridge():
    s = calculate_reliability_score(
        match_score=69, facet_count=1, match_type='facet_probe_fallback',
        is_cross_category=False, facet_value_names="Keuken",
        keyword="vogelgeluiden", reason="", match_coverage=100.0, h1_similarity=34)
    assert s == 0

def test_probe_fallback_kept_when_bridge():
    s = calculate_reliability_score(
        match_score=80, facet_count=1, match_type='facet_probe_fallback',
        is_cross_category=False, facet_value_names="Kunststof",
        keyword="kunststof tuinstoel", reason="", match_coverage=100.0, h1_similarity=80)
    assert s > 0
