"""V48 (RC7): a BARE-category search-derived redirect that drops a real query
token is a poor fit and must not sit in tier C on dominance alone. The coverage
penalty is deepened for bare targets and dominance may not offset it. Faceted
targets (whose /c/ value captures the extra token) keep the milder treatment."""
import os, sys
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)

from src.reliability_scorer import score_search_derived, get_reliability_tier


def test_bare_partial_coverage_falls_to_D():
    # "aftakdoos waterdicht" -> bare Aftakdozen: 50% coverage, share ~1.0.
    s = score_search_derived(65, match_coverage=50.0, dom_share=0.99,
                             dom_count=1500, include_coverage=True,
                             target_is_faceted=False)
    assert get_reliability_tier(s) == 'D', s


def test_faceted_partial_coverage_not_over_penalised():
    # Same coverage/dominance but the /c/ facet captures the extra token —
    # the deepened bare penalty must NOT apply.
    s_bare = score_search_derived(65, 50.0, 0.99, 1500, target_is_faceted=False)
    s_fac = score_search_derived(65, 50.0, 0.99, 1500, target_is_faceted=True)
    assert s_fac > s_bare


def test_full_coverage_bare_still_healthy():
    # A fully-covered dominant bare category keeps a solid score.
    s = score_search_derived(65, match_coverage=100.0, dom_share=0.9,
                             dom_count=1500, target_is_faceted=False)
    assert s >= 70, s


def test_dominance_cannot_rescue_low_coverage_bare():
    # High dominance must not lift a poorly-covered bare category out of the hole.
    low = score_search_derived(65, 40.0, 0.99, 5000, target_is_faceted=False)
    assert get_reliability_tier(low) == 'D', low
