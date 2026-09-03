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


def test_a_word_whose_own_s_is_not_a_plural_marker():
    """Een slot-s kan twee dingen zijn: de meervoudsuitgang (tafel-s) of een
    letter van het woord zelf (doos, huis, glas). Knip je hem altijd af, dan
    stamt 'doos' naar 'do' terwijl 'dozen' naar 'dos' stamt en ontmoet het
    enkelvoud zijn eigen meervoud nooit. Daarom draagt _bridge_stem beide
    vormen en vergelijkt de bridge ze allebei."""
    from src.reliability_scorer import _bridge_stem
    for singular, plural in (('doos', 'dozen'), ('huis', 'huizen'),
                             ('glas', 'glazen'), ('muis', 'muizen')):
        assert _bridge_stem(singular, strip_plural_s=False) == _bridge_stem(plural), singular
    # en een echt meervoud op -s blijft gewoon gestript in de standaardvorm,
    # zodat niets dat eerder bridgede dat nu niet meer doet
    assert _bridge_stem('tafels') == _bridge_stem('tafel')
    assert _bridge_stem('shirts') == _bridge_stem('shirt')


def test_consonant_doubling_is_undone_like_the_vowel_doubling():
    """De spiegel van doos/dozen: in een gesloten lettergreep verdubbelt het
    Nederlands de eindconsonant — fles/flessen, pan/pannen, bus/bussen. De
    klinkerkant zat er al in, deze niet."""
    from src.reliability_scorer import _bridge_stem

    def stems(w):
        return {_bridge_stem(w), _bridge_stem(w, strip_plural_s=False)}

    # Eén van beide vormen moet het meervoud ontmoeten — precies de vraag die
    # _keyword_bridges_value stelt. ('fles' is lang genoeg dat de standaardvorm
    # zijn s verliest; de tweede vorm vangt dat op.)
    for singular, plural in (('fles', 'flessen'), ('pan', 'pannen'),
                             ('bus', 'bussen'), ('mes', 'messen')):
        assert stems(singular) & stems(plural), singular


def test_v62_guard_still_blocks_a_stem_that_is_not_the_head():
    """De keerzijde van de kortestam-regel: 'bor' (van "boren") zit vooraan in
    'bordeauxrod' en dat is geen bridge. Zo kwam kleurtint 'Bordeauxrood' ooit
    achter een Gordijnroedes-redirect aan. Alleen de KOP van een samenstelling
    telt, en die staat in het Nederlands achteraan."""
    assert not bridge("gordijnroede zonder boren", "Bordeauxrood")


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
