"""V70 (2026-09-09) — een kale merknaam op een maincat-r-url hoort op de
merkpagina VAN DIE MAINCAT.

Joeps rij: /products/klussen/r/parkside/ ging naar
/products/klussen/klussen_486260_488662/c/merk~23796649 — Stofzuigerzakken,
462 producten, H1 "Parkside Stofzuigerzakken" — terwijl de query geen enkel
producttype noemt. /products/klussen/c/merk~23796649 is hetzelfde facet één
niveau hoger: 759 producten, H1 "PARKSIDE Klussen".

De versmalling was geen keuze maar een artefact van de catalogus: die kent
alleen facet-urls PER SUBCATEGORIE (gemeten: nul maincat-niveau-rijen), dus de
count-leader-dedup van stap 4 moest er wel één uitkiezen. Daardoor moet ook de
V61-pruning weten dat een maincat-stuk geldig is — anders gooit die het stuk dat
V70 net zette meteen weer weg.
"""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main_parallel_v2 as mp2
from main_parallel_v2 import (_finalize_redirect, _maincat_brand_index,
                              _v70_brand_page, _v70_fold)
from src.facet_filter import FacetFilter


def _index(rows):
    """Bouw de index opnieuw op; de memo is een procesglobal."""
    mp2._MAINCAT_BRAND_INDEX = None
    df = pd.DataFrame(rows, columns=['facet_id', 'facet_name', 'facet_value_id',
                                     'facet_value_name', 'url',
                                     'main_category_name', 'count'])
    try:
        return _maincat_brand_index(FacetFilter(df))
    finally:
        mp2._MAINCAT_BRAND_INDEX = None


PARKSIDE = [
    (3238, 'merk', 23796649, 'PARKSIDE',
     '/products/klussen/klussen_486260/c/merk~23796649', 'Klussen', 540),
    (3238, 'merk', 23796649, 'PARKSIDE',
     '/products/klussen/klussen_486260_488662/c/merk~23796649', 'Klussen', 405),
    (3238, 'merk', 23796649, 'PARKSIDE',
     '/products/klussen/klussen_2963002/c/merk~23796649', 'Klussen', 13),
]


# --- 1. de vouw -------------------------------------------------------------

def test_fold_maakt_query_en_facetwaarde_vergelijkbaar():
    """De r-url schrijft leestekens als scheidingstekens; de facetwaarde niet."""
    assert _v70_fold('PARKSIDE') == _v70_fold('parkside') == 'parkside'
    assert _v70_fold('Lilo & Stitch') == _v70_fold('lilo--stitch') == 'lilo stitch'
    assert _v70_fold('Ferrero Rocher') == 'ferrero rocher'
    assert _v70_fold('Nescafé Dolce Gusto') == 'nescafe dolce gusto'
    assert _v70_fold('  ') == ''


def test_een_bezitsapostrof_is_geen_woordgrens():
    """Zou 'jack-daniels' op 'jack daniel s' stuklopen, dan pakt de index de
    lege buur-waarde 'Jack Daniels' (1 subcategorie, 1 product) en gaat de r-url
    naar de Jacks-subcategorie in mode."""
    assert _v70_fold("Jack Daniel's") == _v70_fold('jack-daniels') == 'jack daniels'
    assert _v70_fold('Levi\u2019s') == _v70_fold('levis') == 'levis'
    assert _v70_fold("L'Or\u00e9al") == 'loreal'


# --- 2. de index ------------------------------------------------------------

def test_merk_wordt_op_maincat_gesleuteld_niet_op_subcat():
    idx = _index(PARKSIDE)
    assert set(idx) == {'klussen'}
    vid, naam, maincat, _count, _nsub = idx['klussen']['parkside']
    assert (vid, naam, maincat) == ('23796649', 'PARKSIDE', 'Klussen')


def test_counts_en_spreiding_worden_over_de_subcategorieen_opgeteld():
    assert _index(PARKSIDE)['klussen']['parkside'][3] == 540 + 405 + 13
    assert _index(PARKSIDE)['klussen']['parkside'][4] == 3


def test_dezelfde_subcategorie_telt_niet_dubbel():
    idx = _index(PARKSIDE + [PARKSIDE[0]])
    assert idx['klussen']['parkside'][3] == 540 + 405 + 13
    assert idx['klussen']['parkside'][4] == 3


def test_alleen_merk_komt_in_de_index():
    """`winkel` is overal in de engine uitgesloten van matching, en een
    kleur/type-facet is geen antwoord op een kale merkquery."""
    idx = _index(PARKSIDE + [
        (99, 'winkel', 1234, 'Parkside Shop',
         '/products/klussen/klussen_486260/c/winkel~1234', 'Klussen', 900),
        (98, 'kleur', 5678, 'Parkside',
         '/products/klussen/klussen_486260/c/kleur~5678', 'Klussen', 900),
    ])
    assert list(idx['klussen']) == ['parkside']


def test_bij_een_botsing_wint_het_drukste_merk():
    """De vouw is met opzet lossy, dus twee value-ids kunnen op dezelfde sleutel
    landen. Een kale query bedoelt dan het merk met de meeste producten."""
    idx = _index([
        (3238, 'merk', 111, 'B&O', '/products/mc/mc_1/c/merk~111', 'MC', 10),
        (3238, 'merk', 222, 'B & O', '/products/mc/mc_2/c/merk~222', 'MC', 40),
        (3238, 'merk', 222, 'B & O', '/products/mc/mc_3/c/merk~222', 'MC', 5),
    ])
    assert idx['mc']['b o'][0] == '222'


def test_zelfde_merk_in_twee_maincats_blijft_gescheiden():
    idx = _index([
        (3238, 'merk', 23796649, 'PARKSIDE',
         '/products/klussen/klussen_486260/c/merk~23796649', 'Klussen', 540),
        (3238, 'merk', 887766, 'PARKSIDE',
         '/products/huishoudelijke_apparatuur/ha_1/c/merk~887766', 'Huishoudelijk', 20),
    ])
    assert idx['klussen']['parkside'][0] == '23796649'
    assert idx['huishoudelijke_apparatuur']['parkside'][0] == '887766'


def test_waarden_zonder_label_worden_overgeslagen():
    """13 waarden in de snapshot dragen geen label."""
    idx = _index([
        (3238, 'merk', 1, None, '/products/mc/mc_1/c/merk~1', 'MC', 5),
        (3238, 'merk', 2, 'Echt', '/products/mc/mc_1/c/merk~2', 'MC', 5),
    ])
    assert list(idx['mc']) == ['echt']


# --- 3. de V61-pruning moet het maincat-stuk laten staan --------------------

class _Parsed:
    def __init__(self, keyword, main_category='klussen'):
        self.keyword = keyword
        self.existing_facet = ''
        self.main_category = main_category
        self.full_category_path = f'/products/{main_category}'
        self.subcategory_id = ''


class _Facets:
    """De staart gebruikt alleen facet_url_set() — en die kent, net als de
    echte catalogus, uitsluitend subcategorie-urls."""

    def facet_url_set(self):
        return {'/products/klussen/klussen_486260_488662/c/merk~23796649'}


def _tail(fragment, maincat_pieces=None):
    row = _finalize_redirect({
        'redirect_url': f'https://www.beslist.nl/products/klussen/c/{fragment}',
        'redirect_category': 'Klussen',
        'reliability_score': 95,
        'match_type': 'maincat_brand_page',
        'facet_fragment': fragment,
        'facet_names': 'merk',
        'facet_value_names': 'PARKSIDE',
        'facet_count': 1,
        'reason': 'V70',
    }, {'keyword': 'parkside', 'parsed': _Parsed('parkside'),
        'facet_filter': _Facets(), 'maincat_pieces': maincat_pieces or set()})
    return row


def test_zonder_de_vrijstelling_zou_de_pruning_het_merk_wissen():
    """Het gedrag dat V70 moest repareren: de url staat niet in de catalogus,
    dus de generieke test gooit het stuk weg en houdt de kale maincat over."""
    row = _tail('merk~23796649')
    assert row['redirect_url'] == 'https://www.beslist.nl/products/klussen/'
    assert row['facet_count'] == 0


def test_met_de_vrijstelling_blijft_de_merkpagina_staan():
    row = _tail('merk~23796649', maincat_pieces={'merk~23796649'})
    assert row['redirect_url'] == \
        'https://www.beslist.nl/products/klussen/c/merk~23796649'
    assert row['facet_fragment'] == 'merk~23796649'
    assert row['facet_count'] == 1
    assert '[V61] dropped' not in row['reason']


def test_de_vrijstelling_geldt_alleen_voor_het_stuk_dat_v70_bouwde():
    """Een tweede, niet-vrijgesteld stuk moet gewoon gesnoeid worden — de
    vrijstelling is geen vrijbrief voor de hele fragmentstring."""
    row = _tail('merk~23796649~~kleur~999', maincat_pieces={'merk~23796649'})
    assert row['redirect_url'] == \
        'https://www.beslist.nl/products/klussen/c/merk~23796649'
    assert '[V61] dropped kleur~999' in row['reason']


# --- 4. welke pagina wint -----------------------------------------------

class _FF:
    """FacetFilter om een kant-en-klare index heen: _v70_brand_page raakt niets
    anders van de filter aan."""

    def __init__(self, index):
        self.index = index


def _page(index, folded, main_category=None):
    mp2._MAINCAT_BRAND_INDEX = index
    try:
        return _v70_brand_page(_FF(index), folded, main_category=main_category)
    finally:
        mp2._MAINCAT_BRAND_INDEX = None


SPREAD = {
    'speelgoed_spelletjes': {'pokemon': ('1', 'Pokémon', 'Speelgoed', 651.0, 9)},
    'huis_tuin': {'pokemon': ('2', 'Pokemon', 'Huis & tuin', 64.0, 4)},
    'baby_peuter': {'pokemon': ('3', 'Pokemon', 'Baby', 17.0, 1)},
}


def test_een_maincat_url_pint_de_maincat():
    """De r-url noemde de categorie al; wij kiezen niets, dus share 1.0."""
    got = _page(SPREAD, 'pokemon', main_category='huis_tuin')
    assert got == ('huis_tuin', '2', 'Pokemon', 'Huis & tuin', 1.0)


def test_zonder_maincat_wint_de_meeste_producten():
    slug, vid, naam, _mc, share = _page(SPREAD, 'pokemon')
    assert (slug, vid, naam) == ('speelgoed_spelletjes', '1', 'Pokémon')
    assert round(share, 3) == round(651 / (651 + 64 + 17), 3)


def test_een_dunne_meerderheid_komt_als_lage_share_terug():
    """Joeps 'sol de janeiro': 45 parfumerie tegen 36 drogisterij. De pagina mag
    er komen, maar de aanroeper moet 'm als tier B kunnen scoren."""
    thin = {
        'parfum_aftershave': {'sdj': ('1', 'Sol de Janeiro', 'Parfumerie', 45.0, 3)},
        'gezond_mooi': {'sdj': ('2', 'Sol de Janeiro', 'Drogisterij', 36.0, 2)},
    }
    _slug, _vid, _naam, _mc, share = _page(thin, 'sdj')
    assert _slug == 'parfum_aftershave'
    assert share < 0.7


def test_een_merk_in_een_enkele_subcategorie_gaat_niet_omhoog():
    """Joeps besluit: dan houdt de subcategoriepagina exact dezelfde producten
    en zegt haar H1 meer ('Culterra Tuinmest')."""
    one = {'tuin_accessoires': {'culterra': ('1', 'Culterra', 'Tuinartikelen', 2.0, 1)}}
    assert _page(one, 'culterra', main_category='tuin_accessoires') is None
    assert _page(one, 'culterra') is None


def test_de_spreidingseis_geldt_ook_bij_het_kiezen_van_de_maincat():
    """baby_peuter zit in de index met 1 subcategorie; die mag nooit winnen, ook
    niet als hij toevallig de meeste producten had."""
    skewed = dict(SPREAD)
    skewed['baby_peuter'] = {'pokemon': ('3', 'Pokemon', 'Baby', 9999.0, 1)}
    assert _page(skewed, 'pokemon')[0] == 'speelgoed_spelletjes'


def test_onbekend_merk_en_lege_query_geven_niets():
    assert _page(SPREAD, 'digimon') is None
    assert _page(SPREAD, '') is None
    assert _page(SPREAD, 'pokemon', main_category='klussen') is None
