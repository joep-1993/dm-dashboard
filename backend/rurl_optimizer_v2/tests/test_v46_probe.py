"""V46 (2026-06-30) — descriptor-aware Stage-1.5 facet selection.

The strict _value_matches_keyword required EVERY value-name token in the query,
so the in-subcat probe found but discarded "USB oplaadbaar"
(opties_ventilator~23795868) for query "usb-ventilator" because "oplaadbaar"
wasn't typed. _value_distinctive_match ignores generic descriptor tokens so the
distinctive token ("usb") alone qualifies the value — without opening the door
to unrelated values."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.facet_probe import _value_distinctive_match, _value_matches_keyword


def test_descriptor_extra_token_now_matches():
    # the exact list-#1 case: opties_ventilator~"USB oplaadbaar"
    assert _value_distinctive_match('usb-ventilator', 'USB oplaadbaar') is True
    # the old strict rule rejected it (regression guard on the motivation)
    assert _value_matches_keyword('usb-ventilator', 'USB oplaadbaar') is False


def test_distinctive_token_must_still_be_named():
    # sibling option values whose distinctive token the query does NOT contain
    assert _value_distinctive_match('usb-ventilator', 'Met timer') is False
    assert _value_distinctive_match('usb-ventilator', 'Met afstandsbediening') is False
    assert _value_distinctive_match('usb-ventilator', 'Oscillerend') is False


def test_unrelated_value_rejected():
    # the classic false-positive the strict rule guarded against
    assert _value_distinctive_match('ketoconazol shampoo', 'Anti roos') is False


def test_descriptor_as_intent_still_matches():
    # when the descriptor IS the intent it's in the query, so it matches anyway
    assert _value_distinctive_match('timer ventilator', 'Met timer') is True
    # (exact form — the probe stemmer doesn't collapse double vowels, so the
    # inflected "draadloze" wouldn't link; that's a separate stemming gap)
    assert _value_distinctive_match('draadloos speaker', 'Draadloos') is True


def test_all_descriptor_value_carries_no_identity():
    # a value made only of descriptors can't qualify anything
    assert _value_distinctive_match('willekeurig', 'Met') is False
