"""V58 (2026-08-18) — the search-derived url trusted an unchecked category slug.

Found while looking at how V28 picks a category for queries where nothing of the
query is represented in the destination ("garage", "halve maan"). Three of that
family pointed at a subcategory segment that does not exist:

    anwb              -> /products/gezond_mooi/gezond_mooi/
    a.h               -> /products/eten_drinken/eten_drinken/
    optidee bestellen -> /products/huis_tuin/huis_tuin/

_build_redirect_url interpolated dom_cat_url_slug straight into
/products/<maincat>/<slug>/. The fallback sampling path in _classify reads each
product's categories[-1], which is the MAIN category when the product isn't filed
any deeper, so the "dominant subcategory" slug can be the maincat itself — and it
can even disagree with dom_cat_name ('Woonaccessoires' with slug 'huis_tuin').
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.search_derived import _build_redirect_url


def classified(slug, share=1.0, mode='and', sem=0):
    return {'mode': mode, 'dom_cat_share': share, 'dom_cat_url_slug': slug,
            'dom_cat_semantic_score': sem}


def test_maincat_slug_is_refused():
    """The three rows from the f4383643 corpus."""
    assert _build_redirect_url('gezond_mooi', classified('gezond_mooi')) is None
    assert _build_redirect_url('eten_drinken', classified('eten_drinken')) is None
    assert _build_redirect_url('huis_tuin', classified('huis_tuin')) is None


def test_any_slug_without_a_numeric_segment_is_refused():
    """Every real Beslist subcategory slug is <maincat>_<digits>[_<digits>]."""
    for slug in ('mode', 'sport_outdoor_vrije-tijd', 'voor_volwassenen', ''):
        assert _build_redirect_url('mode', classified(slug)) is None


def test_real_subcategory_slugs_still_build():
    assert _build_redirect_url('gezond_mooi', classified('gezond_mooi_560593')) == \
        'https://www.beslist.nl/products/gezond_mooi/gezond_mooi_560593/'
    assert _build_redirect_url('huis_tuin', classified('huis_tuin_505126_557609')) == \
        'https://www.beslist.nl/products/huis_tuin/huis_tuin_505126_557609/'


def test_the_shape_check_runs_after_the_dominance_gate_not_instead_of_it():
    """A well-shaped slug on a weak share is still refused, as before."""
    assert _build_redirect_url(
        'gezond_mooi', classified('gezond_mooi_560593', share=0.1)) is None
