"""V49 (RC4): _extract_enrichment_facets — accent-folded, paren-stripped,
brand-excluded facet selection for enriching a bare category page."""
import os, sys
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)

from src.facet_probe import _extract_enrichment_facets


def _facet(url_name, *values):
    return {"urlName": url_name,
            "values": [{"id": i, "facetValue": n, "count": c} for i, n, c in values]}


def test_paren_qualifier_stripped():
    # "pikachu" must match "Pikachu (pokémon)" despite the franchise parenthetical.
    facets = [_facet("personage", (23600616, "Pikachu (pokémon)", 69),
                     (11900214, "Pokémon", 89))]
    picks = _extract_enrichment_facets(facets, "pikachu")
    assert picks and picks[0]["value_id"] == 23600616


def test_accent_folded():
    # "geisoleerd" (no accent) must match the value "Geïsoleerd".
    facets = [_facet("o_tuinhuis", (23812848, "Geïsoleerd", 1147),
                     (23579302, "Met overkapping", 15869))]
    picks = _extract_enrichment_facets(facets, "geisoleerd tuinhuis")
    assert picks and picks[0]["value_id"] == 23812848


def test_brand_excluded_no_peuterey_trap():
    # A generic query token must NOT pin a single-brand page.
    facets = [_facet("merk", (23822745, "Peuterey", 1))]
    assert _extract_enrichment_facets(facets, "peuter sjaal muts wanten") == []


def test_unrelated_value_not_picked():
    facets = [_facet("kleur", (401049, "Geel", 19)),
              _facet("materiaal", (401167, "Plastic", 8))]
    assert _extract_enrichment_facets(facets, "pikachu") == []
