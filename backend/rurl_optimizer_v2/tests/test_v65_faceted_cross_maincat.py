"""V65 (2026-09-02) — Joeps observatie op
/products/tuin_accessoires/tuin_accessoires_504072/r/opbergkast_voor_balkon/.

Die R-URL redirectte naar de KALE `meubilair_389371_6383260` (Opbergkasten) met
score 72. Na V60 (de facetpool was afgekapt, dus `ruimte~4945789` 'Balkon'
bestond niet voor de optimizer) landt hij op `/c/ruimte~4945789` — een betere
match, met score 60.

Twee oorzaken, beide hier vastgepind:

1. De ladder van de cross-maincat fallback hangt aan `dom_cat_share`: hoe
   dominant de KALE categorie is over de HELE query. Als het kwalificerende
   token een facetwaarde is ('balkon' = een `ruimte`-waarde) kan het die share
   niet scherper maken — het spreidt de AND-set over zustercategorieën
   (Opbergkasten 271, Wandkasten 93, Voorraadkasten 88, Dressoirs 66,
   Archiefkasten 50 -> 0.38). De juiste facet VERLAAGT dus het getal dat hem
   beoordeelt. V65 geeft de dekking van de bestemming een eigen sport op de
   ladder, met V64's categorie-guard eromheen.

2. De branch returnt op de plek zelf en sloeg daarmee de V55 H1-lift over:
   h1_overlap ging als 0 de export in terwijl 'Opbergkasten Balkon' vs
   'opbergkast voor balkon' 100/100 is. Gemeten op de 4.998-rijen-run van
   2026-08-26: 28 rijen namen die return, 12 kwamen in aanmerking voor de lift,
   9 daarvan wisselden van tier.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main_parallel_v2 import _cross_maincat_rung, _v55_lift
from src.reliability_scorer import get_reliability_tier

KW = 'opbergkast voor balkon'


# --- 1. de ladder ----------------------------------------------------------

def test_verified_rungs_unchanged():
    """V45/V48/V64 blijven precies zoals ze waren."""
    assert _cross_maincat_rung(True, True, False, 100, 0.95) == 80
    assert _cross_maincat_rung(True, True, False, 99, 0.6) == 72
    assert _cross_maincat_rung(False, True, False, 99, 0.38) == 60
    assert _cross_maincat_rung(False, False, False, 99, 0.0) == 45


def test_faceted_full_coverage_earns_the_verified_rung():
    """De opbergkast-rij: share 0.38 dus niet `verified`, maar de bestemming
    dekt de hele query via ruimte~Balkon."""
    assert _cross_maincat_rung(False, True, True, 99, 0.38) == 72


def test_facet_coverage_cannot_bypass_the_v64_category_guard():
    """`facet_covers` wordt alleen berekend wanneer `_agreed` waar is — een
    facetwaarde die één token echoot terwijl de query de CATEGORIE niet noemt
    ('fietsen berging' -> Hogedrukreinigers /c/t_hdrukrein~'Fietsen') mag nooit
    op 72 landen. De aanroepplek geeft facet_covers=False; dit pint dat de
    ladder zelf ook niet de agreed-sport overslaat."""
    assert _cross_maincat_rung(False, False, False, 60, 0.2) == 45


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


# --- de twee samen ---------------------------------------------------------

def test_end_state_for_joeps_row():
    """72 (sport) -> 82 (lift) -> tier B, tegen 60 -> 70 -> tier C vandaag."""
    rung = _cross_maincat_rung(False, True, True, 99, 0.38)
    score = _v55_lift(KW, rung, 'Opbergkasten', 'Balkon', '')[0]
    assert (rung, score, get_reliability_tier(score)) == (72, 82, 'B')
