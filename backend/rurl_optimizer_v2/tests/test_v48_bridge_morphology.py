"""V48: Dutch plural-voicing / double-vowel bridging in _keyword_bridges_value.

The bridge must be ADDITIVE — it may only start bridging pairs it missed before
(f/v, s/z, double-vowel), never stop bridging a pair the old raw-stem logic
matched. RC3 (kruimeldief -> Kruimeldieven) and the aftakdoos/doos -> dozen class
depend on this."""
import os, sys
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)

from src.reliability_scorer import _keyword_bridges_value as bridge


def test_voicing_f_to_v():
    # RC3: head noun must bridge its own plural category name.
    assert bridge("kruimeldief met lange steel", "Kruimeldieven")
    assert bridge("dief", "Dieven")


def test_voicing_s_to_z():
    # The doos -> dozen class the scoring-redesign plan flagged as "fix first".
    assert bridge("aftakdoos waterdicht", "Aftakdozen")
    assert bridge("kartonnen doos", "Hobbydozen")


def test_double_vowel():
    assert bridge("poot", "Poten")
    assert bridge("opblaastent", "Tenten")


def test_no_false_bridge():
    # Unrelated tokens must still not bridge.
    assert not bridge("vogelgeluiden", "Keuken")
    assert not bridge("bureaustoel", "Vaatwassers")


def test_additive_preserves_old_matches():
    # Pairs the original raw-stem logic matched must keep matching (0-loss).
    assert bridge("dozen", "Hobbydozen")          # plain plural containment
    assert bridge("raam", "Raamaccessoires")      # short token, prefix
    assert bridge("gepelde pistachenoten", "Noten")
    assert bridge("kunststof tuinstoel", "Kunststof")
