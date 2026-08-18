"""V55 (2026-08-18) — symmetric H1 overlap and its score lift.

Joep's observation on redirects_global_f4383643_20260814: row 110,
"ketoconazol shampoo" -> Shampoo /c/ingr_shamp~23982436 ('Ketoconazol'), scored
70 while the destination's H1 names exactly what the searcher named. His idea:
compare the R-URL H1 with the C-URL H1 and lift the score on high overlap.

The V26 signal that was already there could not express this: token_set_ratio
returns 100 whenever one side is a subset of the other, so the bare category
"Shampoo" and the enriched "Shampoo Ketoconazol" both scored 100. These tests
lock in the symmetric replacement and the deliberately narrow lift.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.reliability_scorer import (apply_h1_overlap_lift, compute_h1_overlap,
                                    compute_h1_similarity, get_reliability_tier,
                                    h1_overlap_parts)


# --- the defect in the V26 metric this replaces -----------------------------

def test_v26_cannot_separate_dropped_qualifier_from_kept_one():
    """Documents WHY V55 exists: token_set_ratio saturates on subsets."""
    bare = compute_h1_similarity('ketoconazol shampoo', None, 'Shampoo', '')
    kept = compute_h1_similarity('ketoconazol shampoo', None, 'Shampoo', 'Ketoconazol')
    assert bare == kept == 100


def test_v55_separates_them():
    bare = compute_h1_overlap('ketoconazol shampoo', 'Shampoo', '')
    kept = compute_h1_overlap('ketoconazol shampoo', 'Shampoo', 'Ketoconazol')
    assert kept == 100, kept
    assert bare < 90, bare  # qualifier dropped -> no lift


# --- symmetry: both sides count --------------------------------------------

def test_query_token_the_h1_omits_costs_overlap():
    _, q_cov, _ = h1_overlap_parts('ketoconazol shampoo', 'Shampoo', '')
    assert q_cov == 50, q_cov


def test_facet_value_the_query_never_asked_for_costs_overlap():
    """"Shampoo Aloe vera" for a ketoconazol query: wrong filter bolted on."""
    ov, q_cov, t_cov = h1_overlap_parts('ketoconazol shampoo', 'Shampoo', 'Aloe vera')
    assert ov < 50, ov
    assert t_cov < 50, t_cov


def test_alien_category_noun_damps_overlap():
    """"rubberen tegels" -> Zwembadgrondzeilen /c/…Rubber~~…Tegels. Both facet
    values echo the query; the CATEGORY does not. Must stay under the lift
    floor — this is the row an H1 check must not vouch for."""
    ov = compute_h1_overlap('rubberen tegels', 'Zwembadgrondzeilen', 'Rubber, Tegels')
    assert ov < 90, ov
    assert apply_h1_overlap_lift(71, ov, 100) == 71


# --- Dutch morphology ------------------------------------------------------

def test_plural_and_double_vowel_forms_bridge():
    assert compute_h1_overlap('panelen', 'Paneel', '') == 100
    assert compute_h1_overlap('grote tuinstoel', 'Tuinstoelen', 'Groot') == 100


def test_diacritics_fold():
    assert compute_h1_overlap('geisoleerd tuinhuis', 'Tuinhuizen', 'Geïsoleerd') >= 90


def test_commercial_tail_is_ignored_on_both_sides():
    assert compute_h1_overlap('tuinstoel kopen goedkoop', 'Tuinstoelen', '') == 100


def test_short_tokens_do_not_bridge_into_longer_words():
    """'tv' must not bridge 'tv-meubel' — the 4-char floor. Guards the V31
    generic-noun trap from the other direction."""
    ov = compute_h1_overlap('tv', 'Kapstokmeubels', 'TV meubel')
    assert ov < 90, ov


# --- the lift is narrow, one-way and capped --------------------------------

def test_rc4_route_lifts_when_the_destination_is_healthy():
    """RC4 flat-sets 70 for an enriched bare category regardless of how well
    that enriched page actually matches. With twin H1s AND real inventory behind
    the facet, that 70 becomes 80 — scoring the answer instead of the branch.

    Row 110 itself does not reach here: its facet holds one product, see
    test_row_110_gets_no_lift_because_its_destination_holds_one_product."""
    ov, q_cov, _ = h1_overlap_parts('ketoconazol shampoo', 'Shampoo', 'Ketoconazol')
    lifted = apply_h1_overlap_lift(70, ov, q_cov, dest_count=600)
    assert lifted == 80, lifted
    assert get_reliability_tier(lifted) == 'B'


def test_lift_never_lowers_a_score():
    for score in (0, 25, 44, 70, 88, 95, 100):
        for ov in (0, 40, 67, 80, 90, 100):
            assert apply_h1_overlap_lift(score, ov, 100) >= score


def test_lift_never_manufactures_a_tier_a():
    for score in (80, 85, 88, 89):
        assert apply_h1_overlap_lift(score, 100, 100) <= 89
    # a row already at Tier A keeps its score untouched
    assert apply_h1_overlap_lift(95, 100, 100) == 95


def test_hard_rejected_rows_stay_rejected():
    assert apply_h1_overlap_lift(0, 100, 100) == 0


def test_lift_needs_the_query_to_be_covered():
    """A tidy target side can't buy the bonus when the query lost a token."""
    assert apply_h1_overlap_lift(70, 100, 50) == 70


def test_empty_sides_score_zero():
    assert compute_h1_overlap('', 'Shampoo', 'Ketoconazol') == 0
    assert compute_h1_overlap('ketoconazol shampoo', '', '') == 0


# --- known limitation, pinned deliberately ---------------------------------

def test_compound_head_containment_is_a_known_false_positive():
    """'tafel' is contained in 'voetbaltafel', so "inklapbare tafel" ->
    Voetbaltafels /c/…Inklapbaar reads as an H1 twin and earns the lift, even
    though a football table is a different product.

    No token metric separates a hyponym jump (voetbal+tafel) from an inflection
    or a generic prefix (opberg+kast in "kasten 30 cm diep" -> Opbergkasten,
    which IS right) — that is semantics, and the signal that knows the
    difference is search-derived dominance, not the H1.

    Pinned rather than papered over, because the CONSEQUENCE is bounded and that
    is what makes the lift safe. Tighten the METRIC only together with a
    dominance guard — the precision-side variants that kill this row also killed
    8 correct lifts.

    On the f4383643 corpus the dest_count gate happens to catch this one anyway
    (39 products behind Voetbaltafels), which is luck, not design: the metric
    still reads 100. Both facts are asserted so a future change to either the
    metric or the gate has to face them.
    """
    ov, q_cov, _ = h1_overlap_parts('inklapbare tafel', 'Voetbaltafels', 'Inklapbaar')
    assert ov >= 90 and q_cov >= 90          # the metric's false positive, recorded
    assert apply_h1_overlap_lift(54, ov, q_cov, dest_count=39) == 54   # gate catches it
    lifted = apply_h1_overlap_lift(54, ov, q_cov, dest_count=800)      # if it had not
    assert lifted == 64
    assert get_reliability_tier(lifted) == 'C'   # still never reaches production


def test_generic_prefix_containment_is_the_case_worth_keeping():
    """The same containment rule earns "kasten 30 cm diep" -> Opbergkasten
    /c/30 cm~~Ondiep its lift, which is correct. Locked in as the counterweight
    to the test above."""
    ov, q_cov, _ = h1_overlap_parts('kasten 30 cm diep', 'Opbergkasten', '30 cm, Ondiep')
    assert ov >= 90 and q_cov >= 90
    assert apply_h1_overlap_lift(74, ov, q_cov) == 84


def test_nan_fields_read_back_from_the_output_csv_do_not_crash():
    """redirect_category / facet_value_names arrive as '' from the optimizer but
    as a float NaN when a run's csv is read back for analysis."""
    assert compute_h1_overlap('shampoo', float('nan'), float('nan')) == 0
    assert compute_h1_overlap('shampoo', 'Shampoo', float('nan')) == 100


# --- a perfect H1 over an empty page earns nothing --------------------------

def test_row_110_gets_no_lift_because_its_destination_holds_one_product():
    """The test Joep asked for, and the reason the answer is "no".

    "ketoconazol shampoo" -> Shampoo /c/ingr_shamp~23982436 has a flawless H1:
    the page names exactly what the searcher typed, overlap 100. But the
    destination holds ONE product. Verified against the live Search API on
    2026-08-18: filters[ingr_shamp][0]=23982436 on gezond_mooi_560593 returns
    total 1, and 23982436 is not among the 24 ingr_shamp values the category
    surfaces at all (a limit>1 call returns 17.584 — the OR-fallback total, the
    `total` lies that pa-memory warns about; hold out with limit=1).

    V45 had already docked exactly -10 for that thinness via
    FACETED_COUNT_PENALTY_BANDS. Without the dest_count gate the +10 H1 bonus
    cancels it and lands the row on 89 — inside the production tier — on the
    strength of an H1 alone. "The page is about the right thing" and "there is
    nothing on it" are independent, and the second one wins.
    """
    ov, q_cov, _ = h1_overlap_parts('ketoconazol shampoo', 'Shampoo', 'Ketoconazol')
    assert ov == 100 and q_cov == 100
    assert apply_h1_overlap_lift(79, ov, q_cov, dest_count=2) == 79      # gated
    assert apply_h1_overlap_lift(79, ov, q_cov, dest_count=None) == 89   # unknown -> lift
    assert apply_h1_overlap_lift(79, ov, q_cov, dest_count=1084) == 89   # healthy -> lift


def test_thin_destination_gate_uses_the_v45_faceted_threshold():
    """Anything V45 penalises for thinness must not be liftable, or the bonus
    just cancels the penalty."""
    from src.reliability_scorer import (FACETED_COUNT_PENALTY_BANDS,
                                        H1_OVERLAP_MIN_DEST_COUNT)
    assert H1_OVERLAP_MIN_DEST_COUNT == FACETED_COUNT_PENALTY_BANDS[0][0]
    for cnt in (0, 1, 2, 19, 49):
        assert apply_h1_overlap_lift(70, 100, 100, dest_count=cnt) == 70
    for cnt in (50, 200, 5000):
        assert apply_h1_overlap_lift(70, 100, 100, dest_count=cnt) == 80
