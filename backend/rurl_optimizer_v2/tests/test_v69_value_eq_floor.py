"""V69 (2026-09-08) — Joeps melding op
/products/huis_tuin/huis_tuin_505061_505308/r/caravan/ (Woonaccessoires).

Die R-URL landde op Binnenverlichting /c/ruimte_woonaccessoires~24078346
('Caravan') met score 90 = tier A, "veilig voor productie". Elk onderdeel van de
score zei iets lagers: 60 basis (match 100) + 10 exact - 10 voor de
[maincat]-sprong - 10 voor H1-gelijkenis 50 = 50. De 90 kwam van RC5's
value≡query-vloer, en die vloer is precies de vloer: geen enkel component haalde
hem.

RC5 leest alleen de tokens van de query. Een /c/-pagina heeft twee helften —
categorie + facetwaarde — en de vloer kijkt maar naar één. Binnen de eigen
categorie is dat onschadelijk: de zoeker stond al op dat onderwerp en het facet
verkleint het alleen. Bij een categoriesprong claimt de vloer iets wat hij niet
gemeten heeft: 'Caravan' IS de query, maar 'Binnenverlichting' is een
producttype dat de query nooit genoemd heeft.

Het onderscheidende bewijs is hetzelfde dat V65 al aan een merksprong stelt:
liggen de PRODUCTEN van deze query op de bestemming? Voor 'caravan' zegt de
Search API Overgordijnen (0,55 van 23.559), niet Binnenverlichting.

Gemeten op de 100 cross-categorie value≡query-rijen van de 4.998-rijen-run van
2026-08-26: de leider is het eens met 29 (squishy -> Fidgets, airfryer ->
Airfryers, ferrero rocher -> Bonbons) en oneens met 71 (koel -> LED Strips 'Koel
wit', wifi -> Videocamera's, '25 cm' -> Pannen, teer -> Shampoo). De vloer voor
ELKE cross-categorierij laten vallen is te bot — dat zet airfryer -> Airfryers
van 96 naar 36.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main_parallel_v2 import _finalize_redirect
from src.reliability_scorer import calculate_reliability_score, get_reliability_tier

# De rij zoals de cascade hem aanlevert.
KW = 'caravan'
SRC_SUBCAT = 'huis_tuin_505061_505308'          # Woonaccessoires
DEST_URL = ('https://www.beslist.nl/products/huis_tuin/huis_tuin_505064'
            '/c/ruimte_woonaccessoires~24078346')
REASON = "[maincat] Matched 1 facet 'Caravan' (redirected to valid category)"


def _scores(**kw):
    """(met vloer, zonder vloer) voor Joeps rij."""
    args = dict(match_score=100, facet_count=1, match_type='multi',
                is_cross_category=True, facet_value_names='Caravan', keyword=KW,
                reason=REASON, match_coverage=100.0, h1_similarity=50,
                matched_keywords=['caravan'], unmatched_keywords=[])
    args.update(kw)
    return (calculate_reliability_score(**args, value_eq_floor=True),
            calculate_reliability_score(**args, value_eq_floor=False))


# --- 1. de scorer: de vloer is het enige dat 90 maakt -----------------------

def test_the_floor_is_the_only_thing_that_reaches_tier_a():
    floored, bare = _scores()
    assert floored == 90 and get_reliability_tier(floored) == 'A'
    assert bare == 50 and get_reliability_tier(bare) == 'C'


def test_the_floor_is_untouched_inside_the_source_category():
    """Zelfde query, zelfde facet, geen categoriesprong (Plafondventilators
    /r/caravan/ -> dezelfde categorie + ruimte~Caravan). Daar doet de vloer
    precies wat RC5 bedoelde en verandert V69 niets."""
    floored, bare = _scores(is_cross_category=False, reason="Matched 1 facet: Caravan",
                            h1_similarity=67)
    assert floored == 90
    assert get_reliability_tier(bare) == 'B'  # 60 + 20 samecat + 10 exact = 90 -> 80 na H1
    assert bare < floored


def test_the_synonym_clause_is_not_what_v69_withdraws():
    """RC5 heeft twee vloeren. `value_eq_floor=False` haalt alleen de 90 weg;
    een synoniemmatch (waarde != query) houdt zijn 80."""
    both = _scores(match_type='synonym', facet_value_names='Doorzichtig',
                   keyword='transparant', matched_keywords=['transparant'])
    assert both == (80, 80)


# --- 2. de staart: wanneer de vloer wordt teruggetrokken --------------------

class _Parsed:
    def __init__(self, keyword=KW, subcategory_name=SRC_SUBCAT, main_category='huis_tuin'):
        self.keyword = keyword
        self.subcategory_name = subcategory_name
        self.main_category = main_category
        self.existing_facet = ''
        self.full_category_path = f'/products/{main_category}/{subcategory_name}'


class _Facets:
    """FacetFilter-stand-in: de staart gebruikt alleen facet_url_set(). Leeg =
    V61 slaat de pruning over, zodat deze tests alleen over het getal gaan."""

    def facet_url_set(self):
        return set()


def _row(**kw):
    row = {'redirect_url': DEST_URL, 'redirect_category': 'Binnenverlichting',
           'reliability_score': 90, 'reason': REASON, 'match_type': 'multi',
           'facet_fragment': 'ruimte_woonaccessoires~24078346',
           'facet_names': 'ruimte_woonaccessoires',
           'facet_value_names': 'Caravan', 'facet_count': 1}
    row.update(kw)
    return row


def _ctx(**kw):
    ctx = {'keyword': KW, 'parsed': _Parsed(), 'facet_filter': _Facets(),
           'value_floor_score': 90, 'score_without_value_floor': 50,
           'search_leader_slug': 'huis_tuin_505061_5010584',   # Overgordijnen
           'search_leader_name': 'Overgordijnen'}
    ctx.update(kw)
    return ctx


def test_joeps_row_loses_the_floor_and_keeps_its_destination():
    """De melding zelf: 90 -> 50, tier C. De bestemming verandert NIET — V69
    zit in de staart en mag per constructie alleen het getal aanraken."""
    out = _finalize_redirect(_row(), _ctx())
    assert out['reliability_score'] == 50
    assert out['reliability_tier'] == 'C'
    assert out['redirect_url'] == DEST_URL
    assert '[V69] value≡query floor withdrawn (90 -> 50)' in out['reason']
    assert 'Overgordijnen' in out['reason']


def test_the_floor_stands_when_search_puts_the_products_on_the_destination():
    """airfryer (Magnetrons) -> Airfryers /c/type_airfryer~'Enkele airfryer':
    de leider IS de bestemming, dus de sprong is gedekt."""
    out = _finalize_redirect(
        _row(redirect_category='Fauteuils',   # geen brug met de query
             redirect_url='https://www.beslist.nl/products/huis_tuin/huis_tuin_9/c/t_x~1'),
        _ctx(search_leader_slug='huis_tuin_9', search_leader_name='Fauteuils'))
    assert out['reliability_score'] == 90


def test_a_destination_in_another_maincat_is_not_this_leaders_business():
    """De probe is op de BRON-maincat gescoped, dus over een categorie in een
    andere maincat zegt de leider niets - dezelfde grens die V45 aan
    cross_maincat_fallback stelt. Zonder deze voorwaarde zou elke
    cross-maincat-sprong zijn vloer verliezen omdat de leider per definitie
    'oneens' is."""
    out = _finalize_redirect(
        _row(redirect_category='Fauteuils',
             redirect_url='https://www.beslist.nl/products/wonen/wonen_9/c/t_x~1'),
        _ctx())
    assert out['reliability_score'] == 90
    assert 'V69' not in out['reason']


def test_the_floor_stands_on_a_descent_into_the_source_subtree():
    """Een afdaling naar een KIND van de broncategorie is dezelfde vraag, alleen
    smaller — geen substitutie. Zelfde uitzondering als V51/V62 maken."""
    out = _finalize_redirect(
        _row(redirect_url=f'https://www.beslist.nl/products/huis_tuin/{SRC_SUBCAT}_9911/c/ruimte~1'),
        _ctx())
    assert out['reliability_score'] == 90


def test_the_floor_stands_when_a_query_token_names_the_destination_category():
    """camping (Stoelen) -> Campingstoelen 'Camping': de bestemmingscategorie
    draagt de query zelf, wat de leider ook zegt. Zelfde brugtest als V51/V64."""
    out = _finalize_redirect(
        _row(redirect_category='Campingstoelen'),
        _ctx(keyword='camping', parsed=_Parsed(keyword='camping')))
    assert out['reliability_score'] == 90


def test_no_search_evidence_leaves_the_floor_alone():
    """Geen leider (een winkel-only query komt nooit langs de search-probe):
    afwezig bewijs is geen tegenbewijs."""
    out = _finalize_redirect(_row(), _ctx(search_leader_slug='', search_leader_name=''))
    assert out['reliability_score'] == 90


def test_a_later_branch_score_is_not_rc5s_doing():
    """V31's vlakke 60, V65's 60/45, een search-derived basis: als de score niet
    meer het getal van de scorer is, heeft RC5 hem niet gezet en blijft V69 er
    vanaf."""
    out = _finalize_redirect(_row(reliability_score=60), _ctx())
    assert out['reliability_score'] == 60
    assert 'V69' not in out['reason']
