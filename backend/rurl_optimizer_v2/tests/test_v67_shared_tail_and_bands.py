"""V66/V67 (2026-09-02) — Joeps observatie op
/products/tuin_accessoires/tuin_accessoires_504072/r/opbergkast_voor_balkon/.

Die R-URL redirectte naar de KALE `meubilair_389371_6383260` (Opbergkasten) met
score 72. Na V60 (de facetpool was afgekapt, dus `ruimte~4945789` 'Balkon'
bestond niet voor de optimizer) landt hij op `/c/ruimte~4945789` — een betere
match, met score 60.

Twee oorzaken, beide hier vastgepind:

1. De ladder van de cross-maincat fallback hing aan `dom_cat_share`: hoe
   dominant de KALE categorie is over de HELE query. Als het kwalificerende
   token een facetwaarde is ('balkon' = een `ruimte`-waarde) kan het die share
   niet scherper maken — het spreidt de AND-set over zustercategorieën
   (Opbergkasten 271, Wandkasten 93, Voorraadkasten 88, Dressoirs 66,
   Archiefkasten 50 -> 0.38). De juiste facet VERLAAGT dus het getal dat hem
   beoordeelt. V66 gaf de dekking een eigen sport; V67 haalde die sport er weer
   uit en liet `score_search_derived` de banden doen, zodat dekking, dominantie
   en producttelling elk één keer spreken en de constante alleen nog de
   identiteitsclaim draagt.

2. De branch returnt op de plek zelf en sloeg daarmee de hele staart over:
   h1_overlap ging als 0 de export in terwijl 'Opbergkasten Balkon' vs
   'opbergkast voor balkon' 100/100 is, en de V61-pruning en de V64-cap kwamen er
   ook niet aan te pas. Gemeten op de 4.998-rijen-run van 2026-08-26: 28 rijen
   namen die return, 12 kwamen in aanmerking voor de lift, 9 wisselden van tier.
   V67 maakt de staart één aanroep (`_finalize_redirect`).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main_parallel_v2 import (_cross_maincat_base, _finalize_redirect,
                              _keyword_words, _v55_lift)
from src.reliability_scorer import (ADJ_ONLY_FLOOR, _is_size_or_colour,
                                    get_reliability_tier, score_search_derived)

KW = 'opbergkast voor balkon'


def _score(agreed, name_score, cov, share, count, faceted):
    """De samengestelde score zoals _cross_maincat_fallback_fields hem maakt."""
    return score_search_derived(_cross_maincat_base(agreed, name_score),
                                match_coverage=cov, dom_share=share,
                                dom_count=count, target_is_faceted=faceted)


# --- 1. de basis + de banden ------------------------------------------------

def test_base_carries_only_the_identity_claim():
    """Exacte naam + search-agreement houdt de oude 72; bijna-exact zit tussen de
    oude 60 en 72; zonder agreement blijft het de oude 45."""
    assert _cross_maincat_base(True, 100) == 72
    assert _cross_maincat_base(True, 99) == 72
    assert _cross_maincat_base(True, 96) == 65
    assert _cross_maincat_base(False, 100) == 45


def test_faceted_destination_beats_the_bare_one_on_the_same_evidence():
    """De kern van V67. Zelfde categorie, zelfde bewijs (share 0.38 over 271
    producten), enige verschil: het facet dekt het token dat de kale categorie
    laat vallen. Joeps rij tegen zijn eigen kale variant."""
    faceted = _score(True, 99, cov=100, share=0.38, count=271, faceted=True)
    bare = _score(True, 99, cov=50, share=0.38, count=271, faceted=False)
    assert faceted == 72, faceted
    assert bare < faceted
    assert get_reliability_tier(bare) == 'D', bare


def test_joeps_row_lands_where_it_did_before():
    """V66 gaf deze rij 72 via een eigen sport; V67 komt via de banden op
    hetzelfde getal uit — dat was de calibratie-eis, niet een toevalligheid."""
    assert _score(True, 99, cov=100, share=0.38, count=271, faceted=True) == 72


def test_dominance_still_pays_when_it_rests_on_products():
    """'miele stofzuiger borstels' -> klussen 'Borstels' /c/merk~Miele: naam 100,
    AND-share 0.99, dekking 67% ('stofzuiger' valt weg). Blijft tier B, zoals de
    V48-RC7-sport bedoelde."""
    s = _score(True, 100, cov=67, share=0.99, count=1500, faceted=True)
    assert get_reliability_tier(s) == 'B', s


def test_no_search_agreement_cannot_buy_tier_b_on_coverage():
    """Een sprong die de Search API niet bevestigt, hoe goed de naam ook dekt:
    'fietsen berging' -> Hogedrukreinigers /c/t_hdrukrein~'Fietsen' mag nooit
    boven review-niveau komen."""
    s = _score(False, 100, cov=100, share=0.9, count=5000, faceted=True)
    assert s < 75, s
    s_low = _score(False, 100, cov=50, share=0.9, count=5000, faceted=True)
    assert get_reliability_tier(s_low) == 'D', s_low


def test_thin_evidence_no_longer_rides_on_a_flat_constant():
    """Share 1.0 over 3 producten was 72 zolang `verified` een sport was. Nu
    zakt de basis naar 45 (geen agreement onder de V62-vloer) en straft de
    count-band een kale bestemming verder af."""
    s = _score(False, 99, cov=100, share=1.0, count=3, faceted=False)
    assert get_reliability_tier(s) == 'D', s


# --- 2. de overgeslagen V55-lift -------------------------------------------

def test_lift_applies_to_the_faceted_destination():
    score, tier, reason, overlap, qcov = _v55_lift(
        KW, 72, 'Opbergkasten', 'Balkon', '[V36 cross-maincat fallback] ...')
    assert (overlap, qcov) == (100, 100)
    assert score == 82 and tier == 'B'
    assert '[V55] H1 overlap 100' in reason


def test_lift_leaves_the_bare_destination_alone():
    """Zonder het facet valt 'balkon' weg: overlap 67, onder de vloer van 90."""
    score, tier, reason, overlap, qcov = _v55_lift(KW, 72, 'Opbergkasten', '', 'x')
    assert overlap == 67 and score == 72 and reason == 'x'


def test_lift_reports_the_fields_the_export_shows():
    """De early-return schreef h1_overlap/h1_query_coverage hard op 0; de helper
    levert de echte waarden zodat de reviewsheet niet meer liegt."""
    _, _, _, overlap, qcov = _v55_lift(
        'woonkamer radiator', 80, 'Radiatoren', 'Woonkamer', '')
    assert overlap == 100 and qcov == 100


def test_lift_never_lowers_and_respects_the_ceiling():
    assert _v55_lift(KW, 60, 'Tuinkasten', '', '')[0] == 60      # overlap 0
    assert _v55_lift(KW, 85, 'Opbergkasten', 'Balkon', '')[0] == 89  # capped
    assert _v55_lift(KW, 0, 'Opbergkasten', 'Balkon', '')[0] == 0    # hard reject stays


def test_coverage_uses_the_house_definition_not_the_h1_recall():
    """De A/B-vondst: de H1-recall laat commerciële filler vallen maar GEEN maten,
    dus 'bloempotten 20 liter' -> Bloempotten las als 33% gedekt en zakte van 80
    naar 38. `_tokens_not_represented` (dezelfde teller die V62 en V64 gebruiken)
    weet dat een maat geen inhoudswoord is. 'printer en computer tafel' zakt onder
    BEIDE metrieken, en die degradatie hoort te blijven staan."""
    from main_parallel_v2 import _keyword_words, _tokens_not_represented
    from src.reliability_scorer import h1_overlap_parts

    def house_cov(kw, target):
        words = _keyword_words(kw)
        allt = _tokens_not_represented(words, '')
        left = _tokens_not_represented(words, target)
        return round(100 * (len(allt) - len(left)) / len(allt)) if allt else 100

    assert house_cov('bloempotten 20 liter', 'Bloempotten') == 100
    assert h1_overlap_parts('bloempotten 20 liter', 'Bloempotten', '')[1] == 33
    assert house_cov('printer en computer tafel', 'Printers') == 33
    # en dus: de eerste blijft tier B, de tweede zakt naar D
    assert get_reliability_tier(_score(True, 100, 100, 0.99, 1500, False)) == 'B'
    assert get_reliability_tier(_score(True, 99, 33, 0.99, 1500, False)) == 'D'


# --- 2b. de maat/kleur-vloer (Joeps besluit 2026-09-02) ---------------------

def _cov_and_left(kw, target):
    from main_parallel_v2 import _keyword_words, _tokens_not_represented
    words = _keyword_words(kw)
    allt = _tokens_not_represented(words, '')
    left = _tokens_not_represented(words, target)
    return (round(100 * (len(allt) - len(left)) / len(allt)) if allt else 100), left


def _scored(kw, target, count=250, share=0.5, faceted=False):
    cov, left = _cov_and_left(kw, target)
    return score_search_derived(_cross_maincat_base(True, 100), match_coverage=cov,
                                dom_share=share, dom_count=count,
                                target_is_faceted=faceted, unrepresented=left)


def test_a_dropped_size_or_colour_word_is_tier_c_not_d():
    """Joeps regel: een kale categorie die alleen een maat- of kleurwoord laat
    vallen verdient tier C. De banden zetten deze rijen op 48."""
    assert get_reliability_tier(_scored('grote wasknijpers', 'Wasknijpers')) == 'C'
    assert get_reliability_tier(_scored('witte bloempotten', 'Bloempotten')) == 'C'
    assert _scored('grote wasknijpers', 'Wasknijpers') == ADJ_ONLY_FLOOR


def test_the_floor_does_not_rescue_a_dropped_product_word():
    """Zelfde dekking (50%), ander woord: 'kokers voor posters' -> een pagina met
    posters, 'toiletsteunen hulpmiddelen' -> de brede parent. Die blijven D."""
    for kw, target in [('kokers voor posters', 'Schilderijen & posters'),
                       ('toiletsteunen hulpmiddelen', 'Hulpmiddelen'),
                       ('lopers gang', 'Rode lopers')]:
        assert get_reliability_tier(_scored(kw, target)) == 'D', kw


def test_materials_and_shapes_are_not_in_the_set():
    """Bewust: een materiaal IS productintentie — de V64-cap leunt erop dat
    'houten' in "Hout" gezien wordt — en een vorm is geen maat of kleur."""
    assert get_reliability_tier(_scored('houten tafel', 'Tafels')) == 'D'
    assert not _is_size_or_colour('houten') and not _is_size_or_colour('rond')
    assert _is_size_or_colour('grote') and _is_size_or_colour('witte')


def test_the_floor_cannot_manufacture_a_tier_c():
    """De A/B-vondst: 'voor lange oren' -> Noren komt uit een ONVERIFIEERDE
    cross-maincat-sprong (basis 45), zakt naar 26, en zou door de vloer op 50
    komen omdat 'lange' het enige onvertegenwoordigde token is — terwijl het
    token dat als gedekt gold, de containment-bridge was die 'oren' voor de
    staart van 'Noren' aanzag. De vloer herstelt, hij verzint niet."""
    onverifieerd = score_search_derived(45, match_coverage=50, dom_share=0.2,
                                        dom_count=30, unrepresented=['lange'])
    assert onverifieerd < ADJ_ONLY_FLOOR, onverifieerd
    verifieerd = score_search_derived(72, match_coverage=50, dom_share=0.5,
                                      dom_count=250, unrepresented=['grote'])
    assert verifieerd == ADJ_ONLY_FLOOR


def test_the_floor_only_lifts_never_lowers():
    """Een volledig gedekte rij die toevallig een maatwoord miste, mag er niet
    door omlaag getrokken worden."""
    high = score_search_derived(72, 100, 0.99, 5000, unrepresented=['grote'])
    assert high > ADJ_ONLY_FLOOR, high


def test_measures_need_no_floor_they_are_out_of_the_denominator():
    cov, left = _cov_and_left('bloempotten 20 liter', 'Bloempotten')
    assert (cov, left) == (100, [])


# --- 3. de gedeelde staart (_finalize_redirect) -----------------------------

class _Parsed:
    def __init__(self, keyword, existing_facet='', full_category_path='/products/mc/mc_9'):
        self.keyword = keyword
        self.existing_facet = existing_facet
        self.full_category_path = full_category_path


class _Facets:
    """Staat voor FacetFilter: alleen facet_url_set() wordt door de staart gebruikt."""

    def __init__(self, urls=()):
        self._urls = set(urls)

    def facet_url_set(self):
        return self._urls


def _ctx(keyword, **kw):
    ctx = {'keyword': keyword, 'parsed': _Parsed(keyword), 'facet_filter': _Facets()}
    ctx.update(kw)
    return ctx


def _row(**kw):
    row = {'redirect_url': 'https://www.beslist.nl/products/mc/mc_1/',
           'redirect_category': 'Opbergkasten', 'reliability_score': 72,
           'reason': 'x', 'match_type': 'cross_maincat_fallback',
           'facet_fragment': '', 'facet_names': '', 'facet_value_names': '',
           'facet_count': 0}
    row.update(kw)
    return row


def test_tail_applies_the_lift_and_reports_the_h1_fields():
    """Waar de early return `h1_overlap: 0` hardcodeerde."""
    out = _finalize_redirect(_row(facet_value_names='Balkon'), _ctx(KW))
    assert (out['h1_overlap'], out['h1_query_coverage']) == (100, 100)
    assert out['reliability_score'] == 82 and out['reliability_tier'] == 'B'


def test_tail_prunes_a_fragment_the_destination_page_does_not_have():
    row = _row(redirect_url='https://www.beslist.nl/products/mc/mc_1/c/kleur~99',
               facet_fragment='kleur~99', facet_names='kleur',
               facet_value_names='Blauw', facet_count=1)
    ctx = _ctx('blauwe kast')
    ctx['facet_filter'] = _Facets({'/products/mc/mc_1/c/kleur~1'})
    out = _finalize_redirect(row, ctx)
    assert out['redirect_url'] == 'https://www.beslist.nl/products/mc/mc_1/'
    assert out['facet_fragment'] == '' and out['facet_count'] == 0
    assert out['facet_value_names'] == ''
    assert '[V61] dropped kleur~99' in out['reason']


def test_tail_caps_a_destination_that_accounts_for_a_quarter_of_the_query():
    """Het voorbeeld uit de V64-onderbouwing: 'slush', 'puppy' en 'siroop' staan
    nergens op de bestemming."""
    kw = 'slush puppy siroop framboos'
    out = _finalize_redirect(
        _row(redirect_category='Sportvoeding', facet_value_names='Framboos',
             reliability_score=60), _ctx(kw))
    assert out['reliability_score'] == 45 and out['reliability_tier'] == 'D'
    assert '[V64] capped at 45' in out['reason']


def test_the_cap_has_the_last_word_over_the_v62_withdrawal():
    """De volgorde is inhoudelijk: V62 kan de score TERUGZETTEN naar die van de
    scorer, en de cap moet daar daarna nog overheen kunnen. Zou de cap eerst
    lopen, dan kwam een ingetrokken restore er weer bovenuit."""
    kw = 'slush puppy siroop framboos'
    url = 'https://www.beslist.nl/products/mc/mc_9/'
    out = _finalize_redirect(
        _row(redirect_url=url, redirect_category='Sportvoeding',
             facet_value_names='Framboos', reliability_score=60),
        _ctx(kw, v62_restored=True, guard_url=url, fallback_score=70,
             matched_keywords=[], unmatched_keywords=['slush']))
    assert '[V62] restore withdrawn' in out['reason']
    assert out['reliability_score'] == 45, out['reliability_score']


def test_the_tail_derives_keyword_words_when_a_caller_omits_them():
    """De footgun: een nieuwe return-plek die `keyword_words` vergeet, moet niet
    stil de twee dekkingsstappen uitzetten."""
    kw = 'slush puppy siroop framboos'
    ctx = _ctx(kw)
    assert 'keyword_words' not in ctx
    out = _finalize_redirect(
        _row(redirect_category='Sportvoeding', facet_value_names='Framboos',
             reliability_score=60), ctx)
    assert out['reliability_score'] == 45
    assert _keyword_words(kw) == ['slush', 'puppy', 'siroop', 'framboos']


def test_a_row_without_a_redirect_passes_through_with_a_tier():
    out = _finalize_redirect(_row(redirect_url=None, reliability_score=0), _ctx(KW))
    assert out['reliability_tier'] == 'D'
    assert out['h1_overlap'] == 0 and out['reason'] == 'x'


# --- 4. de twee samen -------------------------------------------------------

def test_end_state_for_joeps_row():
    """Basis 72 (identiteit) + dekking 100% (+8) + dominantie 0.38 (-8) = 72,
    dan de V55-lift naar 82 = tier B. Tegen 60 -> 70 -> tier C vóór deze twee."""
    base_scored = _score(True, 99, cov=100, share=0.38, count=271, faceted=True)
    out = _finalize_redirect(
        _row(reliability_score=base_scored, facet_value_names='Balkon'), _ctx(KW))
    assert (base_scored, out['reliability_score'], out['reliability_tier']) == (72, 82, 'B')


# --- V68: de vloer generaliseert van maat/kleur naar "geen productwoord" ------

def _scored68(kw, target, vocab=None, count=250, share=0.5, faceted=False):
    """Als _scored, maar met de V68-woordenlijst en de querytokens."""
    from src.reliability_scorer import set_category_vocabulary, _bridge_stem
    set_category_vocabulary({_bridge_stem(w) for w in (vocab or [])})
    cov, left = _cov_and_left(kw, target)
    try:
        return score_search_derived(_cross_maincat_base(True, 100), match_coverage=cov,
                                    dom_share=share, dom_count=count,
                                    target_is_faceted=faceted, unrepresented=left,
                                    keyword_tokens=kw.lower().split())
    finally:
        set_category_vocabulary(set())


def test_v68_lifts_a_dropped_qualifier_that_is_not_a_size_or_colour():
    """'professionele' staat in geen categorienaam, dus het is een bijzaak."""
    assert _scored68('professionele mandoline', 'Mandolines',
                     vocab=['mandolines', 'keukensnijders']) == ADJ_ONLY_FLOOR


def test_v68_keeps_a_dropped_product_noun_in_tier_d():
    """'dispenser' IS een categorienaam, dus de bestemming verkoopt iets anders."""
    assert _scored68('wattenschijfjes dispenser', 'Wattenschijfjes',
                     vocab=['wattenschijfjes', 'dispensers']) < ADJ_ONLY_FLOOR


def test_v68_sees_the_head_of_a_dutch_compound():
    """'slang' is de kop van 'Tuinslangen' — een suffix, geen vrije containment."""
    from src.reliability_scorer import set_category_vocabulary, _is_qualifier, _bridge_stem
    set_category_vocabulary({_bridge_stem(w) for w in ['tuinslangen', 'crepepapier']})
    try:
        assert _is_qualifier('slang') is False      # kop van tuinslang
        assert _is_qualifier('crepe') is True       # PREFIX van crepepapier, geen kop
    finally:
        set_category_vocabulary(set())


def test_v68_will_not_lift_when_the_head_itself_went_unrepresented():
    """'Deep Blue Sea' -> Dekbedovertrekken: geen van de woorden is een
    productwoord, maar de bestemming gaat ook niet over de kop van de query."""
    assert _scored68('deep blue sea', 'Dekbedovertrekken',
                     vocab=['dekbedovertrekken']) < ADJ_ONLY_FLOOR


def test_v68_without_a_vocabulary_is_exactly_v67():
    """De noodrem (RURL_V68_PRODUCT_NOUNS=0) mag de regel niet OMKEREN — dat deed
    hij wel toen de lege lijst elk woord als bijzaak las, en de A/B ving het."""
    from src.reliability_scorer import set_category_vocabulary, _is_qualifier
    set_category_vocabulary(set())
    assert _is_qualifier('grote') is True        # maat blijft een bijzaak
    assert _is_qualifier('dispenser') is False   # al het andere niet
