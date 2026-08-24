"""V59 (2026-08-21) — facet values that repeat the subcategory noun.

Found on row 223/222 of redirects_global_828a73ad_20260820_094234.xlsx:

    /huishoudelijke_apparatuur/r/elektrische_verwarming_badkamer/
      -> /main_sanitair/main_sanitair_559440/c/ruimte_verwarmingen~19257689
      but NOT  ~~t_verwarming~19254910  ('Elektrische verwarmingen', 12,618 products)

The subcat-name match ("Verwarmingen") absorbs the query token 'verwarming', so
the leftover is only 'elektrische badkamer'. _collect_longest_per_axis_from_leftover
required EVERY token of a facet value to be covered by a leftover token, and
'verwarmingen' inside "Elektrische verwarmingen" can never be — it was eaten by
the subcategory. Every compound facet value built on the category noun was
therefore unmatchable (t_verwarming, s_kasten, ut_*).

The fix lets the matched subcategory name cover those tokens, while still
requiring at least one token to be earned by the leftover.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.facet_filter import FacetValue
from src.matcher import KeywordMatcher
from main_parallel_v2 import _collect_longest_per_axis_from_leftover

MATCHER = KeywordMatcher(fuzzy_threshold=80)


def fv(facet_name, value_id, value_name):
    return FacetValue(facet_id=0, facet_name=facet_name, facet_value_id=value_id,
                      facet_value_name=value_name, url='', count=0)


# The real facet pool of main_sanitair_559440 ("Verwarmingen"), trimmed.
VERWARMINGEN_POOL = [
    fv('t_verwarming', 19254909, 'Plintverwarmingen'),
    fv('t_verwarming', 19254910, 'Elektrische verwarmingen'),
    fv('t_verwarming', 19254911, 'Lage temperatuur verwarmingen'),
    fv('t_verwarming', 23806823, 'Spiegelverwarming'),
    fv('ruimte_verwarmingen', 19257689, 'Badkamer'),
    fv('ruimte_verwarmingen', 19257691, 'Keuken'),
    fv('v_verwarmingen', 1, 'Verwarmingen'),
]


def collect(leftover, pool, subcat_tokens=()):
    return {axis: m.facet_value.facet_value_name for axis, m in
            _collect_longest_per_axis_from_leftover(
                leftover, pool, MATCHER, subcat_tokens=subcat_tokens).items()}


def test_category_noun_no_longer_blocks_the_axis():
    """The row-222 case: 'elektrische' earns the axis, 'verwarmingen' is forgiven."""
    got = collect(['elektrische', 'badkamer'], VERWARMINGEN_POOL,
                  subcat_tokens={'verwarmingen'})
    assert got == {'t_verwarming': 'Elektrische verwarmingen',
                   'ruimte_verwarmingen': 'Badkamer'}


def test_without_subcat_tokens_behaviour_is_unchanged():
    """No subcat context (the pre-V59 call) must give the pre-V59 answer."""
    assert collect(['elektrische', 'badkamer'], VERWARMINGEN_POOL) == \
        {'ruimte_verwarmingen': 'Badkamer'}


def test_a_value_that_only_echoes_the_category_noun_stays_out():
    """v_verwarmingen~'Verwarmingen' is covered by the subcat noun alone —
    attaching it would add a facet the query never asked for."""
    got = collect(['badkamer'], VERWARMINGEN_POOL, subcat_tokens={'verwarmingen'})
    assert got == {'ruimte_verwarmingen': 'Badkamer'}
    assert 'v_verwarmingen' not in got


def test_the_forgiveness_does_not_leak_to_other_tokens():
    """'Lage temperatuur verwarmingen' has two tokens the query never names;
    only the category noun is forgiven, so the value is still rejected."""
    got = collect(['temperatuur'], VERWARMINGEN_POOL, subcat_tokens={'verwarmingen'})
    assert got == {}


def test_singular_plural_noun_still_counts_as_the_category_noun():
    """Subcat "Verwarming" (singular) vs facet token 'verwarmingen'."""
    got = collect(['elektrische'], VERWARMINGEN_POOL, subcat_tokens={'verwarming'})
    assert got == {'t_verwarming': 'Elektrische verwarmingen'}


def test_plain_leftover_match_is_untouched():
    """The original 'tuinkast kunststof' → materiaal~Kunststof behaviour."""
    pool = [fv('materiaal', 1, 'Kunststof'), fv('materiaal', 2, 'Hout')]
    assert collect(['kunststof'], pool, subcat_tokens={'tuinkasten'}) == \
        {'materiaal': 'Kunststof'}


def test_an_invisible_variant_qualifier_blocks_the_forgiveness():
    """'philips airfryer' in subcat "Airfryers" must not narrow to the XL line:
    _coverage_tokens drops 'xl', so without the guard the value looked fully
    covered by 'philips' + the forgiven category noun."""
    pool = [fv('productlijn_koken', 1, 'Philips airfryer XL'),
            fv('productlijn_koken', 2, 'Philips airfryer')]
    got = collect(['philips'], pool, subcat_tokens={'airfryers'})
    assert got == {'productlijn_koken': 'Philips airfryer'}


def test_unnamed_model_number_blocks_the_forgiveness():
    pool = [fv('productlijn', 1, 'Philips 7000 airfryer')]
    assert collect(['philips'], pool, subcat_tokens={'airfryers'}) == {}
    assert collect(['philips', '7000'], pool, subcat_tokens={'airfryers'}) == \
        {'productlijn': 'Philips 7000 airfryer'}


def test_the_guard_only_fires_on_the_forgiven_path():
    """A leftover that names every coverage token keeps the pre-V59 licence to
    ignore short fragments — 'airfryer xl' → 'Airfryer XL' still matches."""
    pool = [fv('productlijn_koken', 1, 'Airfryer XL')]
    assert collect(['airfryer'], pool) == {'productlijn_koken': 'Airfryer XL'}
