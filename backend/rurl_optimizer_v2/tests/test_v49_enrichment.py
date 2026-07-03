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


def test_synonym_vintage_matches_retro():
    # 'vintage' is a lexical synonym of the facet value 'Retro'.
    facets = [_facet("bouw_koelkast", (23593989, "Retro", 15),
                     (106766, "Vrijstaand", 17))]
    picks = _extract_enrichment_facets(facets, "vintage")
    assert picks and picks[0]["value_id"] == 23593989


def test_dimension_x_separator():
    # query '200x200' must match the facet value '200 x 200'.
    facets = [_facet("afmeting_bedbodem_bed_matras",
                     (4312336, "100 x 200", 1), (4312358, "200 x 200", 2))]
    picks = _extract_enrichment_facets(facets, "2 persoons bed 200x200")
    assert picks and picks[0]["value_id"] == 4312358


def test_audience_synonym_peuter():
    # 'peuter' (toddler) -> the 'Kind' doelgroep value, not 'Baby'.
    facets = [_facet("doelgroep_mode_accessoires",
                     (457524, "Baby", 1099), (457525, "Kind", 2218))]
    picks = _extract_enrichment_facets(facets, "peuter sjaal muts")
    assert picks and picks[0]["value_id"] == 457525
