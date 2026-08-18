"""V57 (2026-08-18) — a coverage-picked brand facet the query never named.

Joeps row 102 of redirects_global_f4383643_20260814:
/gezond_mooi/gezond_mooi_560588/r/honden_katten/ (Oordoppen) redirected to
Make-up accessoires /c/merk~23800900 — brand 'Generic' — because 83% of the
search result set carried it.

The guard meant to stop that, brand_match_is_spurious, opens by asking whether
the brand was matched at all and returns False ("not spurious") when it wasn't:
it exists to judge whether an existing MATCH was genuine or accidental. The V29
probe picks a facet by result-set coverage with no lexical claim at all, so the
less the query had to do with the brand, the more certain the guard was that it
was fine. The probe even records `keyword_match: false` — nothing read it.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main_parallel_v2 import _spurious_brand_facet
from src.matcher import KeywordMatcher

M = KeywordMatcher(fuzzy_threshold=80, use_token_coverage=True)


def suppressed(value, keyword, cat, facet='merk'):
    return _spurious_brand_facet(facet, value, keyword, cat, M)


# --- the rows from Joeps file ----------------------------------------------

def test_row_102_brand_is_suppressed():
    assert suppressed('Generic', 'honden katten', 'Make-up accessoires')


def test_the_rest_of_that_family_is_suppressed():
    """Every other unbridged brand redirect the f4383643 corpus produced."""
    for value, keyword, cat in [
        ('Testjezelf', 'honden katten', 'CBD'),          # today's probe pick
        ('ABUS', 'anwb', 'Fietssloten'),
        ('Furygan', 'purdey outlet', 'Motorbroeken'),
        ('5five', 'garage', 'Make-up accessoires'),
        ('Villeroy & Boch', 'halve maan', 'Trapmatten'),
        ('Jura', 'duitsland', 'Keukenapparatuur'),
        ('Douwe Egberts', 'goedkoopste koffie', 'Gemalen koffie'),
    ]:
        assert suppressed(value, keyword, cat), (keyword, value)


# --- real brand queries must survive ---------------------------------------

def test_genuine_brand_queries_are_kept():
    """The point of the facet. A query token that strictly names the brand keeps
    it, including when the query carries a product word alongside."""
    for value, keyword, cat in [
        ('Philips', 'philips', 'Scheerapparaten'),
        ('Nizoral', 'nizoral shampoo', 'Shampoo'),
        ('Ferrero Rocher', 'ferrero rocher', 'Bonbons'),
        ('Castrol', 'castrol edge 5w30', 'Motorolie'),
        ('Illy', 'illy koffiebonen 1kg', 'Koffiebonen'),
        ('Campingaz', 'campingaz gasfles 907', 'Gasflessen'),
        ('Samsonite', 'samsonite koffers outlet', 'Koffers'),
        ('Swiffer', 'swiffer doekjes', 'Schoonmaakdoeken'),
    ]:
        assert not suppressed(value, keyword, cat), (keyword, value)


def test_the_v41_case_is_still_caught():
    """"wc papier" -> merk 'Paper Dreams': 'papier' only fuzz-hits 'Paper' and
    names the product, not a brand. This is what V41 was built for."""
    assert suppressed('Paper Dreams', 'wc papier', 'Toiletpapier')


def test_weight_qualifier_cannot_rescue_a_brand():
    """V40: "max 30 kg" must not keep merk 'Max & Molly'."""
    assert suppressed('Max & Molly', 'max 30 kg', 'Hondenhalsbanden')


# --- scope: only brand/shop axes ------------------------------------------

def test_non_brand_facets_are_untouched():
    """A type/property facet is a different kind of claim and this guard has no
    opinion on it — even when the query names none of it."""
    assert not suppressed('Alleszuigers', 'honden katten', 'Bouwstofzuigers',
                          facet='type_stofzuiger')
    assert not suppressed('Ketoconazol', 'ketoconazol shampoo', 'Shampoo',
                          facet='ingr_shamp')


def test_empty_value_is_not_a_suppression():
    assert not suppressed('', 'honden katten', 'Make-up accessoires')


# --- the new rule strictly subsumes the old one ----------------------------

def test_new_rule_never_keeps_what_v41_dropped():
    """old-True (matched, but not distinctively) implies new-True, so the change
    can only suppress more — never resurrect a facet V41 already dropped."""
    cases = [
        ('Paper Dreams', 'wc papier', 'Toiletpapier'),
        ('Max & Molly', 'max 30 kg', 'Hondenhalsbanden'),
        ('Generic', 'honden katten', 'Make-up accessoires'),
        ('Philips', 'philips', 'Scheerapparaten'),
        ('Nizoral', 'nizoral shampoo', 'Shampoo'),
        ('Architects Paper', 'papier behang', 'Behang'),
    ]
    for value, keyword, cat in cases:
        old = M.brand_match_is_spurious(keyword, value, cat)
        new = suppressed(value, keyword, cat)
        assert not (old and not new), (keyword, value, old, new)


def test_brand_match_is_spurious_refactor_is_behaviour_preserving():
    """brand_match_is_spurious still answers its own question — it must keep
    returning False when the brand was never matched at all, because V39 reads it
    on a path where the facet DID come from a lexical match."""
    assert M.brand_match_is_spurious('honden katten', 'Generic', 'Make-up accessoires') is False
    assert M.brand_match_is_spurious('wc papier', 'Paper Dreams', 'Toiletpapier') is True
    assert M.brand_match_is_spurious('philips', 'Philips', 'Scheerapparaten') is False
