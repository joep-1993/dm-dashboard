"""V45 (2026-06-30) — search-derived confidence scoring by query coverage +
category product-count dominance. Locks in the calibration validated against
the user's redirects.txt lists #2/#3 and the 300-URL regression corpus."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.reliability_scorer import score_search_derived, get_reliability_tier


def tier(s):
    return get_reliability_tier(s)


# --- List #2: poorly-fitting redirects must drop OUT of confident territory ---

def test_bare_category_tiny_count_high_share_is_demoted():
    """motorhelm → bare Videocamera's: share 1.0 but only 116 products (noise).
    A bare-category redirect on a thin set must lose confidence."""
    s = score_search_derived(65, match_coverage=100.0, dom_share=1.0,
                             dom_count=116, target_is_faceted=False)
    assert s < 65, s  # dropped below the old flat constant


def test_bare_category_low_coverage_demoted():
    """union_switch → bare cat, 50% coverage on a 1-product set."""
    s = score_search_derived(65, match_coverage=50.0, dom_share=1.0,
                             dom_count=1, target_is_faceted=False)
    assert tier(s) == 'D', s


def test_half_covered_query_cannot_reach_tier_B_on_dominance():
    """aftakdoos_waterdicht: head noun matched, 'waterdicht' dropped (50% cov),
    perfect dominance. Coverage must outweigh dominance -> stays below B."""
    s = score_search_derived(65, match_coverage=50.0, dom_share=0.99,
                             dom_count=1149, target_is_faceted=False)
    assert s < 75, s


# --- List #3: well-fitting redirects should rise toward production tier ---

def test_full_coverage_strong_dominance_reaches_tier_B():
    """usb-ventilator: 100% coverage, share 0.85 over 7487 products."""
    s = score_search_derived(65, match_coverage=100.0, dom_share=0.85,
                             dom_count=7487, target_is_faceted=False)
    assert tier(s) == 'B', s


# --- The faceted vs bare count-guard distinction (the brand-page fix) ---

def test_faceted_thin_brand_page_lands_in_review_not_D():
    """ici_paris → merk page, 4 products. Faceted pages are intentionally narrow,
    so a thin count must NOT nuke them to D the way a bare category would."""
    faceted = score_search_derived(75, match_coverage=0.0, dom_share=1.0,
                                   dom_count=4, target_is_faceted=True)
    bare = score_search_derived(75, match_coverage=0.0, dom_share=1.0,
                                dom_count=4, target_is_faceted=False)
    assert tier(faceted) == 'C', faceted   # review, not discard
    assert bare < faceted                  # bare cat on 4 products is harder-hit


def test_populous_faceted_page_not_count_penalised():
    """A faceted page with a healthy product count keeps its score (vazen, 392)."""
    s = score_search_derived(65, match_coverage=60.0, dom_share=0.49,
                             dom_count=392, target_is_faceted=True,
                             include_coverage=False)
    assert s == 65, s  # neutral: no count penalty above the faceted threshold


def test_no_auto_suppression():
    """Conservative: the function never returns a hard 0 on its own — worst case
    a weak row clamps low but is still emitted for review."""
    s = score_search_derived(65, match_coverage=0.0, dom_share=0.0,
                             dom_count=0, target_is_faceted=False)
    assert s >= 0
    # base 65 - cov18 - dom15 - count15 = 17, not a hard 0
    assert s > 0, s


def test_include_coverage_false_skips_coverage_band():
    """subcategory_name path already folded coverage into its base; V45 must add
    only dominance+count, not re-penalise coverage."""
    with_cov = score_search_derived(65, 0.0, 0.85, 9999, include_coverage=True)
    without_cov = score_search_derived(65, 0.0, 0.85, 9999, include_coverage=False)
    assert without_cov > with_cov
