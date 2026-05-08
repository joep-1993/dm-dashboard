"""
AI Title Generation Service

Generates SEO-optimized titles using OpenAI based on the N8N workflow.
Processes URLs from unique_titles that need AI-generated titles.
"""
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Queue
from typing import Dict, List, Optional
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from openai import OpenAI

from backend.database import get_db_connection, return_db_connection
from backend.faq_service import fetch_products_api

# Configuration
USER_AGENT = "Beslist script voor SEO"
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")
BASE_URL = "https://www.beslist.nl"

# Words that should be lowercase unless at start of sentence
LOWERCASE_WORDS = {"met", "in", "zonder", "van", "voor", "tot", "op", "aan", "uit", "bij", "naar", "over", "onder", "tegen", "tussen", "door", "om", "en", "of"}

# Reverted v3.2: the unconditional pre-AI SOD→SIC swap for materials was wrong
# in direction. The correct rule (per design) is positional:
#   - facet BEFORE category → SOD (adjective form: "Katoenen pyjama's")
#   - facet AFTER category  → SIC (noun form: "pyjama's van Katoen")
# A blanket swap stripped SOD's -en adjective ending unconditionally, producing
# "Katoen Dames pyjama's" instead of "Katoenen Dames pyjama's". The future
# implementation should be position-aware (either feed both forms with a
# position rule into the AI prompt, or split before/after based on api_h1
# position).
_PREFER_SIC_URL_SLUGS: set = set()


def _norm_for_dedupe(s: str) -> str:
    """Normalize a span for compound-category dedupe.

    Folds the separators that the H1 generator and taxonomy use interchangeably:
    "ovens & fornuizen" / "ovens en fornuizen" / "ovensfornuizen" all collapse
    to the same key. Strips whitespace, hyphens, ampersands, and the standalone
    Dutch conjunction "en" when it sits between separators.
    """
    s = s.lower()
    s = re.sub(r'\s*&\s*', ' ', s)
    s = re.sub(r'(?:^|\s)en(?=\s|$)', ' ', s)
    s = re.sub(r'[\s\-]+', '', s)
    return s


def _strip_pre_clause_duplicates(h1: str) -> str:
    """Remove a bare token sequence that already appears inside a "met"/"zonder" clause.

    The H1 generator and AI prompt cooperate to place feature values like
    "Dierenprint", "leren band", "lange mouwen" inside a "met …" clause AFTER the
    productnaam, but the same bare value also routinely sneaks in BEFORE the
    productnaam (either via the API H1 or because the AI echoed it). This pass
    walks each "met X" / "zonder X" clause and, when the phrase X appears
    verbatim earlier in the H1 as a bare token run (not following another
    met/zonder), drops the earlier occurrence. Only removes one bare occurrence
    per clause.

    Skips the bare match if it itself follows met/zonder (already a clause), so
    legitimate two-clause titles like "schoenen met klittenband met sleehakken
    en klittenband" are not collapsed.
    """
    if not h1:
        return h1
    tokens = h1.split()
    if len(tokens) < 4:
        return h1
    PREPS = {'met', 'zonder'}
    clauses = []  # (start_idx, end_idx_exclusive, phrase_tokens)
    i = 0
    while i < len(tokens):
        if tokens[i].lower() in PREPS:
            j = i + 1
            phrase = []
            while j < len(tokens) and tokens[j].lower() not in PREPS:
                phrase.append(tokens[j])
                j += 1
            if phrase:
                clauses.append((i, j, phrase))
            i = j
        else:
            i += 1
    if not clauses:
        return h1
    drop = set()
    for clause_start, _clause_end, phrase in clauses:
        phrase_str = ' '.join(phrase)
        if len(phrase_str) < 4:
            continue
        plen = len(phrase)
        for k in range(clause_start - plen + 1):
            if any((k + d) in drop for d in range(plen)):
                continue
            if k > 0 and tokens[k - 1].lower() in PREPS:
                continue
            if all(tokens[k + d].lower() == phrase[d].lower() for d in range(plen)):
                for d in range(plen):
                    drop.add(k + d)
                break
    if not drop:
        return h1
    return ' '.join(t for idx, t in enumerate(tokens) if idx not in drop)


def _dedupe_compound_category(h1: str, category_name: str) -> str:
    """Strip a duplicated compound spelling of the category from `h1`.

    When the H1 contains the category in two spelling variants (joined vs split,
    `&` vs ` en `, etc., e.g. "massageolie" + "massage olie", or
    "ovens en fornuizen" + "ovens & fornuizen"), keep the LAST occurrence and
    drop the earlier ones. Last-wins because the canonical taxv2 category name
    is appended at the end of the H1 by the upstream pipeline.
    """
    if not h1 or not category_name:
        return h1
    cat_norm = _norm_for_dedupe(category_name)
    if len(cat_norm) < 4:
        return h1
    words = h1.split()
    matches = []
    n = len(words)
    for start in range(n):
        for end in range(start, n):
            joined = _norm_for_dedupe(' '.join(words[start:end + 1]))
            if len(joined) > len(cat_norm):
                break
            if joined == cat_norm:
                matches.append((start, end + 1))
                break
    if len(matches) < 2:
        return h1
    # Drop earlier matches; keep only the last (canonical) occurrence.
    # Resolve overlaps among the to-drop set by preferring earliest non-overlapping spans.
    drop_set = set()
    last_drop_end = -1
    for s, e in matches[:-1]:
        if s < last_drop_end:
            continue
        for i in range(s, e):
            drop_set.add(i)
        last_drop_end = e
    new_words = [w for i, w in enumerate(words) if i not in drop_set]
    return ' '.join(new_words)


def _dedupe_internal_compounds(h1: str) -> str:
    """Drop earlier compound-spelling repeats inside `h1`.

    Generalises `_dedupe_compound_category`: instead of matching against a known
    category name, scans the H1 itself for any 1-token or 2-token run that
    normalizes (via `_norm_for_dedupe` — strips spaces/`&`/`en`) to the same
    form as another run. When 2+ matches exist, drops the earlier ones and
    keeps the last. Catches API-side joined/split repetitions like:

      "ronde plantentafels planten Tafels"  → "ronde planten Tafels"
                                              (or the reverse, depending on
                                               which side ends up last)
      "Camerastatieven camera statieven"    → "camera statieven"

    Where the existing dedupe-vs-category pass needs a known category_name to
    fire and would miss these because the category here is `Tafels`, not
    `Plantentafels`. 6-char minimum on the normalized form keeps short tokens
    out (so "Maat 38" doesn't accidentally trip it).
    """
    if not h1:
        return h1
    words = h1.split()
    n = len(words)

    def _stem(s: str) -> str:
        # Fold trailing -s (Dutch/English plural) so "plantentafel" and
        # "plantentafels" collapse to the same key. Only strip when the
        # remaining form is still ≥6 chars to avoid mangling short words.
        if s.endswith('s') and len(s) > 6:
            return s[:-1]
        return s

    spans_by_stem: dict = {}
    for start in range(n):
        for end in range(start, min(start + 2, n)):
            joined = _norm_for_dedupe(' '.join(words[start:end + 1]))
            if len(joined) < 6:
                continue
            spans_by_stem.setdefault(_stem(joined), []).append((start, end + 1))

    drop_set: set = set()
    for spans in spans_by_stem.values():
        if len(spans) < 2:
            continue
        spans.sort()
        for s, e in spans[:-1]:
            if any(d in drop_set for d in range(s, e)):
                continue
            for k in range(s, e):
                drop_set.add(k)
    if not drop_set:
        return h1
    return ' '.join(w for i, w in enumerate(words) if i not in drop_set)


def _dedupe_prefix_overlap(h1: str) -> str:
    """Drop a token that's a strict prefix of an adjacent longer token.

    Catches API-side fragmentation where Beslist's H1 renders the same product
    type as both a standalone token and a joined longer form, e.g.:

      "Lichtgroene planten plantentafel Tafels 55 cm lang"
      → "Lichtgroene plantentafel Tafels 55 cm lang"

    Only fires when both tokens are ≥6 chars and the longer is ≥3 chars longer
    (so substantive nouns like "planten" → "plantentafel" trigger but short
    series/brand fragments like "Aqua" prefix of "Aquariums" or "Sweat" prefix
    of "sweaters" don't, where the shorter token is itself an information
    carrier worth keeping). Looks ahead 1-2 positions so an intervening
    adjective doesn't break the match.
    """
    if not h1:
        return h1
    words = h1.split()
    n = len(words)
    drop: set = set()
    # Plural/derivation suffixes: when `b == a + suf`, the two tokens are the
    # same root word repeated (e.g. "Sweat sweaters" via 'ers',
    # "Plant planten" via 'en', "Color Colors" via 's"). Lets us catch
    # short-prefix repeats that the 6-char/+3-diff rule below skips on purpose.
    PLURAL_SUFFIXES = ('s', 'en', 'ers')
    for i in range(n):
        if i in drop:
            continue
        a = words[i].lower().strip('.,!?;:')
        if len(a) < 4:
            continue
        # Lookahead extended from 2 to 4 positions so cases like
        # "instap … Heren schoenen Instappers" (3 tokens between prefix and
        # full form, often inserted by the AI) still trigger.
        for j in range(i + 1, min(i + 5, n)):
            if j in drop:
                continue
            b = words[j].lower().strip('.,!?;:')
            # Skip hyphenated targets: "Fisher" is a prefix of "Fisher-Price",
            # but the real duplication is the multi-token "Fisher Price" form
            # earlier in the H1. Dropping just "Fisher" here would leave
            # "Price" orphaned. Let _dedupe_internal_compounds handle it via
            # _norm_for_dedupe (which strips hyphens and spaces uniformly).
            if '-' in b:
                continue
            # Targeted plural rule: b is a + known plural suffix.
            if any(b == a + suf for suf in PLURAL_SUFFIXES):
                drop.add(i)
                break
            # Generic prefix rule: 6-char floor + ≥3 diff (avoids false drops
            # like "Aqua" → "Aquariums" where the prefix is a separate signal).
            if len(a) >= 6 and len(b) >= len(a) + 3 and b.startswith(a):
                drop.add(i)
                break
    if not drop:
        return h1
    return ' '.join(w for i, w in enumerate(words) if i not in drop)


def _dedupe_facet_values(h1: str, selected_facets: list) -> str:
    """Drop earlier duplicates of any selected-facet detail_value in `h1`.

    Catches facet-vs-facet duplication produced upstream by build_product_subject
    when two facets carry the same or overlapping value (e.g. type_laptop +
    productlijn_laptop both = "Chromebook" → "ASUS Chromebook Chromebook"; or
    merk=Samsung + productlijn_mobtel="Samsung Galaxy" → "Samsung Galaxy Samsung
    Galaxy A56 ..."). The existing `_dedupe_compound_category` only handles
    facet-vs-category duplication; this is the symmetrical safety net for
    facet-vs-facet.

    For each unique facet detail_value of length ≥4, finds all standalone
    token-run occurrences (word-boundary, case-insensitive) and, when there are
    2+, drops every occurrence except the LAST (mirrors `_dedupe_compound_category`'s
    last-wins policy because the canonical/longest form usually trails).

    Longer facet values are processed before shorter ones so e.g. "Samsung
    Galaxy" collapses first; the standalone "Samsung" check then sees only one
    remaining occurrence and is a no-op. The 4-character minimum keeps short
    size/colour codes ("5G", "S", "M") from being mistakenly deduped.
    """
    if not h1 or not selected_facets:
        return h1
    values = sorted(
        {(f.get('detail_value') or '').strip() for f in selected_facets},
        key=lambda v: -len(v.split()),
    )
    for v in values:
        if not v or len(v) < 4:
            continue
        v_tokens = v.split()
        words = h1.split()
        n = len(words)
        plen = len(v_tokens)
        if plen == 0 or plen > n:
            continue
        matches = []
        i = 0
        while i <= n - plen:
            if all(
                words[i + k].lower().strip('.,!?;:') == v_tokens[k].lower()
                for k in range(plen)
            ):
                matches.append((i, i + plen))
                i += plen
            else:
                i += 1
        if len(matches) < 2:
            continue
        drop = set()
        connectors = {'en', '&', 'and', 'of', 'or'}
        for s, e in matches[:-1]:
            for k in range(s, e):
                drop.add(k)
            # Drop a connector immediately AFTER the span ("X en X" → drop
            # "X en", keep last "X"). Falls back to dropping a connector
            # BEFORE the span when it's a list-tail repeat ("with X, Y, X"
            # though we mostly just see the suffix case in practice).
            if e < len(words) and words[e].lower().strip('.,!?;:') in connectors:
                drop.add(e)
            elif s - 1 >= 0 and words[s - 1].lower().strip('.,!?;:') in connectors:
                drop.add(s - 1)
        h1 = ' '.join(w for idx, w in enumerate(words) if idx not in drop)
    return h1


_REDUNDANT_MET_RE = re.compile(r'\bmet\s+(met|zonder)\b', re.IGNORECASE)


def fix_redundant_met(text: str) -> str:
    """Collapse 'met met X' / 'met zonder X' into 'met X' / 'zonder X'.

    Rule 8 of the LLM prompt asks the model to prefix product-feature facet
    values with "met". When the facet value itself already starts with
    "Met" or "Zonder" (e.g. soort_hak value 'Zonder hakken'), the LLM
    dutifully prepends "met" anyway, producing nonsense like
    "schoenen met zonder hakken". Strip the redundant prefix here.
    """
    if not text:
        return text
    return _REDUNDANT_MET_RE.sub(lambda m: m.group(1), text)


def normalize_preposition_case(text: str) -> str:
    """
    Ensure prepositions like 'met', 'in', 'zonder' are lowercase,
    unless they are at the start of the sentence.

    Examples:
        "Blauwe Feestwimpers Met Glitter" -> "Blauwe Feestwimpers met Glitter"
        "Met glitter feestwimpers" -> "Met glitter feestwimpers" (start of sentence)
    """
    if not text:
        return text

    words = text.split()
    result = []

    for i, word in enumerate(words):
        # Check if word (without punctuation) is a preposition
        word_lower = word.lower().rstrip('.,!?;:')
        if word_lower in LOWERCASE_WORDS and i > 0:
            # Not at start, make lowercase but preserve any trailing punctuation
            if word[-1] in '.,!?;:':
                result.append(word_lower + word[-1])
            else:
                result.append(word_lower)
        else:
            result.append(word)

    return ' '.join(result)


# Module-level constants for facet categorization, hoisted out of
# generate_title_from_api so they are not rebuilt per URL and are unit-testable.
_PRODUCT_TYPE_SUFFIXES = (
    'jassen', 'jacks', 'broeken', 'shirts', 'hemden', 'tops', 'blouses',
    'schoenen', 'laarzen', 'sandalen', 'sneakers', 'boots', 'pumps', 'instappers',
    'jurken', 'rokken', 'truien', 'vesten', 'pakken',
    'tassen', 'horloges', 'brillen', 'sieraden',
    'pannen', 'ovens', 'magnetrons', 'koelkasten', 'wasmachines',
    'banken', 'stoelen', 'tafels', 'kasten', 'bedden',
)
_MET_FEATURE_VALUES = {
    'korte mouwen', 'lange mouwen', 'driekwart mouwen',
    'capuchon',
    'ronde hals', 'v-hals', 'col', 'opstaande kraag',
    'rits', 'knopen', 'drukknopen', 'veters',
    'draaiplateau', 'grill',
    'strepen',
}
_SPEC_UNITS_RE = re.compile(
    r'^\d+[\.,]?\d*\s*'
    r'(liter|liters|watt|volt|bar|pk|rpm|mph|kwh|kw'
    r'|cm|mm|meter|m|inch|"'
    r'|kg|gram|g|mg|ml|cl|dl|l'
    r'|persoons|personen|deurs|zits)\b',
    re.IGNORECASE,
)
_SIZE_ABBREVS = {'xs', 'xxs', 's', 'm', 'l', 'xl', 'xxl', 'xxxl',
                 '2xl', '3xl', '4xl', '5xl'}
_ADJECTIVAL_SIZES = {
    'klein', 'kleine', 'groot', 'grote', 'middel', 'middelgroot', 'middelgrote',
    'mini', 'midi', 'maxi',
    'extra groot', 'extra grote', 'extra klein', 'extra kleine',
    'zeer groot', 'zeer grote', 'zeer klein', 'zeer kleine',
}
_ADJ_UNINFLECT = {'brede': 'breed', 'lange': 'lang', 'hoge': 'hoog',
                  'diepe': 'diep', 'smalle': 'smal'}


def is_spec_value(val: str, fname: str) -> bool:
    """Detect if a facet value is a specification that should go at the end."""
    vl = val.lower().strip()
    if vl in _ADJECTIVAL_SIZES:
        return False
    if vl.startswith('maat ') or vl.startswith('wijdte'):
        return True
    if vl in ('grote maten', 'kleine maten'):
        return True
    if _SPEC_UNITS_RE.match(vl):
        return True
    if val.replace('.', '').replace(',', '').replace('-', '').strip().isdigit():
        return True
    if vl in _SIZE_ABBREVS:
        return True
    if fname.startswith('maat') or fname.startswith('wijdte'):
        return True
    if fname.startswith('vermogen'):
        return True
    if fname == 'aantal_puzzelstukjes':
        return True
    return False


def _norm_ws(s: str) -> str:
    """Whitespace+lowercase normalize for prefix-overlap comparisons."""
    return ' '.join(s.lower().split())


# --- Polish prompt variants (v1: detailed, v2: short) ---

_POLISH_PROMPT_V2_TEMPLATE = (
    'Je krijgt een Nederlandse SEO-titel die al de juiste woorden in ongeveer de '
    'juiste volgorde heeft. Lever ALLEEN de gepolijste titel terug (geen uitleg).\n\n'
    'Huidige titel: "{ai_h1}"\n'
    'Facetwaarden die intact moeten blijven: {facet_values_str}\n'
    '{met_section}\n'
    'Regels:\n'
    '1. Voeg geen woorden toe en verwijder geen woorden behalve dubbelingen.\n'
    '2. Mag wel woordvolgorde aanpassen voor natuurlijk Nederlands.\n'
    '3. Verbuig bijvoeglijke naamwoorden ("Nieuw"→"Nieuwe", "Vrijstaand"→"Vrijstaande", "Klein"→"Kleine") waar grammaticaal nodig.\n'
    '4. Eerste woord met hoofdletter; daarna kleine letters behalve eigennamen/merken.\n'
    '5. Bijvoeglijke naamwoorden (kleur/materiaal/stijl/formaat/conditie) VOOR de productnaam, doelgroep direct VOOR de productnaam.\n'
    '6. NOOIT "in", "van" of "voor" toevoegen.\n'
    '7. Als er een "met X" / "zonder X" clause is: NA de productnaam, niet ervoor.\n'
    '8. Maten ("Maat L", "40 cm", "128 GB") en kleurcombinaties helemaal achteraan.'
)


def _build_polish_prompt(ai_h1: str, facet_info: str, facet_values_str: str,
                         met_section: str, met_rule: str, mode: str = 'v1') -> str:
    if mode == 'v2':
        return _POLISH_PROMPT_V2_TEMPLATE.format(
            ai_h1=ai_h1, facet_values_str=facet_values_str, met_section=met_section
        )
    # v1 (default) — long detailed prompt
    return f"""Je bent een SEO-expert. Verbeter deze titel tot een goedlopende en grammaticaal correcte H1 zonder "-".

Huidige titel van API: "{ai_h1}"

Facetten (naam: waarde): {facet_info}

BELANGRIJK - Facetwaarden die INTACT moeten blijven (niet splitsen of herschikken):
{facet_values_str}
{met_section}
Regels:
1. ALLERBELANGRIJKSTE REGEL: Gebruik UITSLUITEND woorden die voorkomen in de titel OF in de facetten hierboven. Voeg ABSOLUUT GEEN nieuwe woorden toe. Geen "Nieuwe", geen extra bijvoeglijke naamwoorden, geen woorden die niet letterlijk in de input staan.
2. Facetwaarden zijn vaste combinaties en mogen NIET opgesplitst worden.
3. Merk ALTIJD vooraan (bijv. "Apple iPhones" niet "iPhones van Apple").
4. Kleuren, materialen en stijlen (bv. "Industriële", "Moderne", "Scandinavische") als bijvoeglijk naamwoord VOOR de doelgroep en VOOR de productnaam, NOOIT aan het einde van de titel (bijv. "blauwe Heren hoodies", "Industriële Zwarte tafels", NIET "Heren blauwe hoodies" of "tafels Industriële").
5. Doelgroepen (Heren, Dames, Kinderen, Jongens, Meisjes, Baby) staan direct VOOR de productnaam maar NA kleuren/materialen, NOOIT met "voor" ervoor.
6. NOOIT "in", "van" of "voor" toevoegen (doelgroep-achtervoegsel wordt automatisch toegevoegd).
{met_rule}8. Als een serie/productlijn de merknaam al bevat, noem het merk NIET apart.
9. ALLE bijvoeglijke naamwoorden uit de facetten moeten VOOR de productnaam staan, NOOIT erna. Dit geldt niet alleen voor formaat ("Klein"/"Kleine", "Groot"/"Grote", "Middel", "Mini", "Maxi") en conditie ("Nieuw"/"Nieuwe"), maar ook voor kenmerken zoals "Waterdicht"/"Waterdichte", "Vrijstaand"/"Vrijstaande", "Luxe", "Modern"/"Moderne", "Klassiek"/"Klassieke", "Inbouw", "Hangend", "Opvouwbaar". Voeg deze woorden NOOIT zelf toe als ze niet in de facetten staan.
   - FOUT: "Rubberen Butterfly Kiss vibrators Kleine"
   - GOED: "Kleine rubberen Butterfly Kiss vibrators"
   - FOUT: "Dames kunststof sporttassen Waterdichte"  (Waterdichte staat na de productnaam)
   - GOED: "Dames kunststof waterdichte sporttassen"
   - FOUT: "Inductie kookplaten Vrijstaande"
   - GOED: "Vrijstaande inductie kookplaten"
   - FOUT: "Houten salontafels Grote"
   - GOED: "Grote houten salontafels"
10. Verbuig bijvoeglijke naamwoorden correct (bijv. "Nieuw" → "Nieuwe" voor de-woorden, "Vrijstaand" → "Vrijstaande").
11. Maak de titel natuurlijk lopend Nederlands.

Geef ALLEEN de verbeterde titel terug, geen uitleg."""


# --- Hallucination-guard variants ---

_HALLUC_V1_CHECKS = ['Heren', 'Dames', 'Kinderen', 'Jongens', 'Meisjes', 'Baby', 'Nieuwe', 'Nieuw']


def _apply_hallucination_guard(improved_h1: str, ai_h1: str,
                                non_size_facets: list, mode: str = 'v1') -> str:
    """Strip hallucinated words from improved_h1.

    v1: only checks 8 hardcoded common-hallucination words.
    v2: prefix-match against the entire input vocabulary; any output word
        that doesn't share a 5+ char prefix with some allowed word
        (length differs by ≤3) is dropped.
    """
    all_input_words = set(w.lower() for w in ai_h1.split())
    for f in non_size_facets:
        all_input_words.update(w.lower() for w in f['detail_value'].split())
    inflected = set()
    for w in all_input_words:
        inflected.add(w + 'e')
        inflected.add(w + 'en')
        if w.endswith('e'):
            inflected.add(w[:-1])
    all_input_words.update(inflected)

    if mode == 'v2':
        # Generic prefix-match: drop any output word that doesn't share a 5+ char
        # prefix with any allowed word, when length differs by ≤3.
        kept_words = []
        for word in improved_h1.split():
            wl = re.sub(r'[^a-zA-Zà-ÿ]', '', word).lower()
            if not wl or len(wl) < 4:
                kept_words.append(word)
                continue
            if wl in all_input_words:
                kept_words.append(word)
                continue
            ok = False
            for aw in all_input_words:
                if len(aw) < 4:
                    continue
                common = 0
                for a, b in zip(wl, aw):
                    if a == b:
                        common += 1
                    else:
                        break
                if common >= 5 and abs(len(wl) - len(aw)) <= 3 and common >= max(len(wl), len(aw)) - 3:
                    ok = True
                    break
            if ok:
                kept_words.append(word)
            # else: dropped
        return ' '.join(kept_words)

    # v1 (default): drop only the 8 known offender words when not in input
    for word in _HALLUC_V1_CHECKS:
        if word.lower() not in all_input_words and word in improved_h1.split():
            improved_h1 = ' '.join(w for w in improved_h1.split() if w != word)
    return improved_h1


def format_dimensions(text: str) -> str:
    """
    Format dimension patterns to include 'x' between measurements.

    Examples:
        "31 cm 115 cm" -> "31 cm x 115 cm"
        "100 cm 50 cm 30 cm" -> "100 cm x 50 cm x 30 cm"
        "2 meter 3 meter" -> "2 meter x 3 meter"
    """
    if not text:
        return text

    # Pattern matches: number + unit, followed by space and another number + unit
    # Units: cm, mm, m, meter, inch, inches, "
    # This pattern finds consecutive dimension patterns and adds 'x' between them
    pattern = r'(\d+(?:[.,]\d+)?\s*(?:cm|mm|m|meter|inch|inches|"))\s+(\d+(?:[.,]\d+)?\s*(?:cm|mm|m|meter|inch|inches|"))'

    # Keep applying the pattern until no more matches (handles 3+ dimensions)
    prev_text = None
    while prev_text != text:
        prev_text = text
        text = re.sub(pattern, r'\1 x \2', text, flags=re.IGNORECASE)

    return text


# Processing state
_processing_state = {
    "is_running": False,
    "should_stop": False,
    "total_urls": 0,
    "processed": 0,
    "successful": 0,
    "failed": 0,
    "skipped": 0,
    "current_url": None,
    "started_at": None,
    "last_error": None,
}
_state_lock = threading.Lock()

# Reusable OpenAI client
_openai_client = None


def get_openai_client() -> OpenAI:
    """Get or create the shared OpenAI client."""
    global _openai_client
    if _openai_client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key:
            _openai_client = OpenAI(api_key=api_key)
    return _openai_client


def create_http_session() -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=5, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_http_session = create_http_session()


def init_ai_titles_columns():
    """No-op after Big Bang — columns live on pa.unique_titles_jobs/_content
    via migration step 1.
    """
    print("[AI_TITLES] Columns live on new tables; init_ai_titles_columns() is a no-op")


def get_processing_status() -> Dict:
    """Get current AI title processing status."""
    with _state_lock:
        return {
            "is_running": _processing_state["is_running"],
            "total_urls": _processing_state["total_urls"],
            "processed": _processing_state["processed"],
            "successful": _processing_state["successful"],
            "failed": _processing_state["failed"],
            "skipped": _processing_state["skipped"],
            "current_url": _processing_state["current_url"],
            "started_at": _processing_state["started_at"].isoformat() if _processing_state["started_at"] else None,
            "last_error": _processing_state["last_error"],
        }


def get_unprocessed_urls(limit: int = 100) -> List[Dict]:
    """Get URLs that need AI title processing (job pending OR content missing).

    Args:
        limit: Maximum URLs to return. If 0, returns all pending URLs.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        query = """
            SELECT u.url, c.title, c.description, c.h1_title
            FROM pa.unique_titles_jobs j
            JOIN pa.urls u ON j.url_id = u.url_id
            LEFT JOIN pa.unique_titles_content c ON c.url_id = j.url_id
            WHERE j.status = 'pending'
              AND (c.title IS NULL OR c.title = '' OR c.h1_title IS NULL OR c.h1_title = '')
            ORDER BY j.created_at DESC
        """
        if limit > 0:
            cur.execute(query + " LIMIT %s", (limit,))
        else:
            cur.execute(query)
        return [dict(row) for row in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def get_unprocessed_count() -> int:
    """Count URLs with status='pending' jobs and no usable content yet."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT COUNT(*) AS count
            FROM pa.unique_titles_jobs j
            LEFT JOIN pa.unique_titles_content c ON c.url_id = j.url_id
            WHERE j.status = 'pending'
              AND (c.title IS NULL OR c.title = '' OR c.h1_title IS NULL OR c.h1_title = '')
        """)
        return cur.fetchone()['count']
    finally:
        cur.close()
        return_db_connection(conn)


def scrape_page_h1(url: str) -> Optional[Dict]:
    """
    Scrape a Beslist page to extract H1 title and discount.

    Returns dict with h1_title and discount, or None on failure.
    """
    try:
        # Build full URL
        full_url = url if url.startswith('http') else f"{BASE_URL}{url}"

        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        response = _http_session.get(full_url, headers=headers, timeout=30)

        if response.status_code != 200:
            print(f"[AI_TITLES] HTTP {response.status_code} for {url}")
            return None

        html = response.text

        # Extract H1 title (using the CSS class from the N8N flow)
        h1_match = re.search(r'<h1[^>]*class="[^"]*productsTitle[^"]*"[^>]*>(.*?)</h1>', html, re.IGNORECASE | re.DOTALL)
        if not h1_match:
            # Fallback: try any h1
            h1_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.IGNORECASE | re.DOTALL)

        h1_title = h1_match.group(1).strip() if h1_match else None

        # Clean H1 from HTML tags
        if h1_title:
            h1_title = re.sub(r'<[^>]+>', '', h1_title).strip()

        # Extract max discount from page
        discount_matches = re.findall(r'<div class="discountLabel[^"]*">-(\d+)(?:<!--.*?-->)?%</div>', html)
        discounts = [int(d) for d in discount_matches]
        discount = max(discounts) if discounts else None

        return {
            "h1_title": h1_title,
            "discount": discount,
        }

    except Exception as e:
        print(f"[AI_TITLES] Scrape error for {url}: {e}")
        return None


def generate_ai_title(h1_title: str, url: str) -> Optional[Dict]:
    """
    Use OpenAI to generate an improved H1 title.

    Based on the N8N flow prompt:
    - Reorder words for better grammar
    - Put brand first
    - Use adjective forms for materials/colors
    """
    client = get_openai_client()
    if not client:
        print("[AI_TITLES] No OpenAI API key configured")
        return None

    prompt = f"""Je bent een SEO-expert. Maak van '{h1_title}' een goedlopende en grammaticaal correcte titel zonder "-". Gebruik UITSLUITEND de woorden die je krijgt - verzin ABSOLUUT GEEN nieuwe woorden, maten, kleuren of andere informatie. Je mag WEL "met", "zonder", "van" en "voor aan" toevoegen waar grammaticaal nodig (zie regels 2 en 8). Overbodige woorden mag je weglaten. Je mag de volgorde aanpassen om een beter lopende zin te maken.

Regels:
1. Zorg dat het merk ALTIJD vooraan in de titel staat, dus "Apple iPhones" in plaats van "iPhones van Apple".
2. Kleuren en materialen als bijvoeglijk naamwoord VOOR het zelfstandig naamwoord, MAAR: maximaal 3 bijvoeglijke naamwoorden VOOR het product. Als er meer dan 3 eigenschappen zijn, verplaats dan materiaal en bevestiging/plaatsing NA het product met "van" (materiaal) of "voor aan" (bevestiging/plaatsing).
   - Bij weinig facetten (1-3 bijvoeglijke naamwoorden):
     - GOED: "Rode schoenen"
     - GOED: "Houten bank"
     - GOED: "Zilveren messing fonteinkranen"
   - Bij veel facetten (4+ bijvoeglijke naamwoorden), verplaats materiaal/bevestiging NA het product:
     - FOUT: "Witte Metalen Klassieke Rechthoekige Muur wijnrekken" (te veel gestapeld!)
     - GOED: "Klassieke witte rechthoekige wijnrekken van metaal voor aan de muur"
     - FOUT: "Zwarte Katoenen Kleine Reistassen met organizer Vakantie"
     - GOED: "Kleine zwarte reistassen van katoen met organizer"
   - NOOIT "in" gebruiken voor materiaal of kleur.
     - FOUT: "fonteinkranen in zilver" of "schoenen in rood"
3. Volgorde van bijvoeglijke naamwoorden VOOR het product: stijl (Klassieke, Moderne) → kleur (witte, rode) → vorm/formaat (rechthoekige, kleine, grote) → [product]. Na het eerste woord altijd kleine letters.
   - FOUT: "Witte Metalen Klassieke wijnrekken"
   - GOED: "Klassieke witte wijnrekken"
   - FOUT: "Fleece Moderne Ronde hondenmanden"
   - GOED: "Moderne ronde hondenmanden van fleece"
4. Doelgroepen (Heren, Dames, Kinderen, Jongens, Meisjes, Baby) staan ALTIJD direct VOOR de productnaam, NOOIT met "voor" ervoor.
   - FOUT: "vesten voor heren"
   - GOED: "Heren vesten"
   - FOUT: "schoenen voor kinderen"
   - GOED: "Kinderen schoenen"
5. Zet maten (zoals Maat S, Maat M, Maat L, Maat XL, Maat 38, Maat 42, etc.) helemaal ACHTERAAN in de titel, ZONDER "met" ervoor. Maten staan altijd los achteraan.
   - FOUT: "Nike Heren Maat L tanktops"
   - GOED: "Nike Heren tanktops Maat L"
   - FOUT: "Maat 42 sneakers"
   - GOED: "Sneakers Maat 42"
   - FOUT: "Blauwe cardigans Maat XS met lange mouwen"
   - GOED: "Blauwe cardigans met lange mouwen Maat XS"
   - FOUT: "Imprimétops met Maat 40" (NOOIT "met" voor maten!)
   - GOED: "Imprimétops Maat 40"
6. Als een serie/productlijn de merknaam al bevat, noem het merk NIET apart.
   - FOUT: "Adidas Groene Kinderen Adidas Originals trainingspakken" (Adidas dubbel)
   - GOED: "Groene Adidas Originals Kinderen trainingspakken"
   - FOUT: "Samsung Samsung Galaxy smartphones"
   - GOED: "Samsung Galaxy smartphones"
7. Zet conditie (Nieuw/Nieuwe) en formaat (Kleine/Grote) als bijvoeglijk naamwoord VOOR de productnaam, nooit erachter.
   - FOUT: "Low frost Tafelmodel D Nieuwe Kleine"
   - GOED: "Nieuwe kleine Low Frost tafelmodel Energieklasse D"
   - FOUT: "Inductie kookplaat Nieuwe"
   - GOED: "Nieuwe inductie kookplaat"
   - FOUT: "Rubberen Butterfly Kiss vibrators Kleine"
   - GOED: "Kleine rubberen Butterfly Kiss vibrators"
8. BELANGRIJK: Producteigenschappen zoals "Korte mouwen", "Lange mouwen", "Capuchon", "Ronde hals", "V-hals" mogen NOOIT los voor de productnaam staan. Voeg ALTIJD "met" toe en zet ze NA de productnaam. Dit geldt ook voor facetwaarden die beginnen met "Met" of "Zonder".
   Bundel alles in ÉÉN "met X, Y en Z" clause. Gebruik "met" maar één keer, daarna komma's en "en".
   LET OP: Maten (Maat S/M/L/XL/38/42 etc.) zijn GEEN producteigenschappen! Zet NOOIT "met" voor maten. Maten staan los achteraan.
   - FOUT: "Heren Slim fit poloshirts Lange mouwen" (ALTIJD "met" toevoegen!)
   - GOED: "Heren Slim fit poloshirts met lange mouwen"
   - FOUT: "Heren poloshirts met borstzak en print met korte mouwen" (twee keer "met")
   - GOED: "Heren poloshirts met korte mouwen, borstzak en print"
   - FOUT: "Puma Heren blauwe joggingbroeken met Maat L" (NOOIT "met" voor maten!)
   - GOED: "Puma Heren blauwe joggingbroeken Maat L"
   - FOUT: "Capuchon Heren jassen met rits"
   - GOED: "Heren jassen met capuchon en rits"
9. Hoofdlettergebruik: alleen het eerste woord met een hoofdletter, daarna kleine letters (behalve merknamen en eigennamen).
   - FOUT: "Klassieke Witte Rechthoekige wijnrekken"
   - GOED: "Klassieke witte rechthoekige wijnrekken"
   - FOUT: "Rode Melamine Mokken"
   - GOED: "Rode melamine mokken"

Voorbeeld:
"Schoenen - Nike - Rode - Met veters" wordt "Rode Nike schoenen met veters".
"Saniclear - Zilver - Messing - Design Fonteinkranen" wordt "Zilveren messing Saniclear design fonteinkranen".
"Nike - Heren - Maat L - Tanktops" wordt "Nike Heren tanktops Maat L".
"Adidas - Groen - Kinderen - Adidas Originals Trainingspakken" wordt "Groene Adidas Originals Kinderen trainingspakken".
"Tafelmodel Low frost D Nieuw Kleine" wordt "Nieuwe kleine Low Frost tafelmodel Energieklasse D".
"Stretch - Heren - Korte mouwen - Met borstzak - Met print - Poloshirts" wordt "Stretch Heren poloshirts met korte mouwen, borstzak en print".
"Dutch Dandies - Heren - Slim fit - Lange mouwen - Poloshirts" wordt "Dutch Dandies Heren Slim fit poloshirts met lange mouwen".
"Witte - Metalen - Klassieke - Rechthoekige - Muur - Wijnrekken" wordt "Klassieke witte rechthoekige wijnrekken van metaal voor aan de muur".
"Fleece - Moderne - Ronde - Hondenmanden" wordt "Moderne ronde hondenmanden van fleece".

Ik wil het antwoord graag in dit json formaat terug:
{{"oude_titel": "{h1_title}", "h1_title": "nieuwe_titel_hier", "url": "{url}"}}

Geef ALLEEN de JSON terug, geen andere tekst."""

    try:
        response = client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.3,
            response_format={"type": "json_object"}
        )

        content = response.choices[0].message.content.strip()

        # Parse JSON response
        import json
        result = json.loads(content)

        return {
            "h1_title": result.get("h1_title", h1_title),
            "original_h1": result.get("oude_titel", h1_title),
        }

    except Exception as e:
        print(f"[AI_TITLES] OpenAI error: {e}")
        return None


def generate_title_from_api(url: str, *, prompt_mode: str = 'v1',
                             halluc_mode: str = 'v2') -> Optional[Dict]:
    """
    Generate title using productsearch API + OpenAI improvement.

    This method:
    1. Fetches H1 and facet data from the productsearch API
    2. Uses OpenAI to improve the H1 while keeping facet values intact
    3. Returns the improved H1 and original H1

    Args:
        url: Page URL.
        prompt_mode: 'v1' (long detailed prompt) or 'v2' (short polish prompt).
        halluc_mode: 'v1' (hardcoded 8-word check) or 'v2' (prefix-match guard).

    Returns dict with h1_title, original_h1, or None on failure.
    """
    # Step 1: Fetch from productsearch API. Skip the FAQ-only post-fetch work
    # (extract_related_plp_urls + the 30-product loop) since the titles flow
    # only consumes h1_title / selected_facets / category_name.
    _t_fetch_start = time.time()
    page_data = fetch_products_api(url, include_related=False)
    _t_fetch_ms = (time.time() - _t_fetch_start) * 1000

    if not page_data:
        print(f"[AI_TITLES] API fetch failed for {url}")
        return None

    if page_data.get("error"):
        print(f"[AI_TITLES] API error for {url}: {page_data.get('error')}")
        return None

    api_h1 = page_data.get("h1_title", "")
    selected_facets = page_data.get("selected_facets", [])
    category_name = page_data.get("category_name", "")
    canonical_category = category_name

    if not api_h1:
        print(f"[AI_TITLES] No H1 from API for {url}")
        return None

    api_h1 = _dedupe_compound_category(api_h1, canonical_category)
    api_h1 = _dedupe_prefix_overlap(api_h1)
    api_h1 = _dedupe_internal_compounds(api_h1)
    api_h1 = _dedupe_facet_values(api_h1, selected_facets)

    # Type-facets carry the product type in their values (e.g. soort_bz="Dahliabollen",
    # t_wanddeco="Wandplaten"), so the category name would be a duplicate in the title.
    # Classification is per (facet_name, category) pair, cached in
    # pa.facet_type_classifications. Single batched DB lookup per URL instead of
    # one per facet (the per-facet path opens a fresh DB connection each call).
    from backend.facet_classifier import batch_classify_facets, _NEVER_TYPE_FACETS
    type_class = batch_classify_facets(selected_facets, category_name)
    # Treat any facet whose URL slug (url_name) is in the policy never-list as
    # NOT a type-facet, regardless of whether the per-(facet_name, category)
    # classification says True. The classifier is keyed on the API facet_name
    # (e.g. "type") which may serve double duty across URL slugs (e.g.
    # "type_productlijn" vs "type"); the URL slug is the more reliable signal
    # for the policy.
    # URL slugs whose facets must NEVER act as type-facets, regardless of
    # what the (facet_name, category) classifier says. Common shape: facet
    # values that LOOK like product types because they happen to contain
    # the category-noun ("Winterschoenen", "Dragon Ball") but are
    # semantically attributes (character, season) — the category itself
    # still has to appear in the H1 to make the page subject clear.
    #   - personage: characters/franchises (Super Mario, Frozen, Anna ...)
    #   - seizoen_schoenen: seasons (Winter, Zomer, Lente, Herfst); some
    #     values store the category-bearing form (e.g. "Winterschoenen")
    #     which trips the LLM classifier, but it's still a season facet.
    _NEVER_URL_SLUGS = {'type_productlijn', 'personage', 'seizoen_schoenen'}
    # URL slugs that ALWAYS act as type-facets, regardless of what the
    # per-(facet_name, category) classifier in pa.facet_type_classifications
    # decided. Use this when the classifier's mixed-verdict tiebreak rule
    # produces a False that the slug's actual values contradict — e.g. t_stoel
    # values like "Relax tuinstoel" carry the singular of the category in
    # every value, so appending "Tuinstoelen" always duplicates.
    _ALWAYS_TYPE_URL_SLUGS = {'t_stoel'}
    has_category_override = any(
        (f.get('url_name') or '').lower() in _ALWAYS_TYPE_URL_SLUGS
        or type_class.get((f.get('facet_name') or '').lower().strip(), False)
        for f in selected_facets
        if (f.get('url_name') or '').lower() not in _NEVER_URL_SLUGS
    )
    if has_category_override and category_name:
        # Strip category_name from end or start of the API H1 if it's already there
        cat_suffix = re.compile(r'\s+' + re.escape(category_name) + r'\s*$', re.IGNORECASE)
        api_h1 = cat_suffix.sub('', api_h1).strip()
        cat_prefix = re.compile(r'^' + re.escape(category_name) + r'\s+', re.IGNORECASE)
        api_h1 = cat_prefix.sub('', api_h1).strip()
        # Prevent downstream logic from re-appending it
        category_name = ''

    # Append category name if missing from H1 (e.g., "Vrijstaande 23 liter" → "Vrijstaande 23 liter magnetrons")
    if category_name and category_name.lower() not in api_h1.lower():
        api_h1 = api_h1.rstrip() + " " + category_name

    # Step 2: Use OpenAI to improve the H1
    client = get_openai_client()
    if not client:
        # If no OpenAI, just return the API H1
        return {
            "h1_title": _dedupe_facet_values(
                _dedupe_internal_compounds(
                    _dedupe_prefix_overlap(
                        _dedupe_compound_category(_strip_pre_clause_duplicates(api_h1), canonical_category)
                    )
                ),
                selected_facets,
            ),
            "original_h1": api_h1,
        }

    # Remove standalone brand if another facet already contains the brand name
    # e.g., Merk="Epson" + Productlijn="Epson EcoTank" → drop the standalone "Epson".
    # Case-insensitive so Merk="Asus" + Productlijn="ASUS Zenbook" also dedupes.
    brand_facet = next((f for f in selected_facets if f['facet_name'].lower() == 'merk'), None)
    if brand_facet:
        brand_name = brand_facet['detail_value']
        brand_lower = brand_name.lower()
        other_values = [f['detail_value'] for f in selected_facets if f is not brand_facet]
        if any(brand_lower in ov.lower() for ov in other_values):
            selected_facets = [f for f in selected_facets if f is not brand_facet]
            # Also strip the standalone brand from the API H1 (case-insensitive)
            if api_h1.lower().count(brand_lower) > 1:
                api_h1 = re.sub(
                    r'\b' + re.escape(brand_name) + r'\b ',
                    '',
                    api_h1,
                    count=1,
                    flags=re.IGNORECASE,
                )
            brand_facet = None  # Brand was deduplicated

    # When BOTH populaire_serie and type_productlijn URL slugs are present, treat
    # the two values as a single inseparable productname chunk that is prepended
    # together (e.g. "Teva Hurricane XLT 2"). Otherwise the AI would happily slot
    # a colour or doelgroep between them ("Teva Hurricane Zwarte XLT 2"). Match
    # on url_name (URL slug) rather than facet_name (display label) since the
    # API exposes the slug under urlName and the display label drifts per category.
    populaire_serie_facet = next(
        (f for f in selected_facets if (f.get('url_name') or '').lower() == 'populaire_serie'),
        None,
    )
    type_productlijn_facet = next(
        (f for f in selected_facets if (f.get('url_name') or '').lower() == 'type_productlijn'),
        None,
    )
    series_combined_chunk = ""
    pre_chunk_modifiers: List[str] = []  # colour values rendered BEFORE the brand+series chunk
    if populaire_serie_facet and type_productlijn_facet:
        ps_val = populaire_serie_facet['detail_value']
        tp_val = type_productlijn_facet['detail_value']
        series_combined_chunk = f"{ps_val} {tp_val}"
        # Strip both values from api_h1 so the AI doesn't see them split up.
        for v in (ps_val, tp_val):
            api_h1 = re.sub(r'\b' + re.escape(v) + r'\b', '', api_h1, count=1, flags=re.IGNORECASE)
        api_h1 = re.sub(r'\s+', ' ', api_h1).strip()
        # Drop both facets from selected_facets so neither becomes a lead_value
        # nor a prompt facet — they'll re-enter via the prepended chunk.
        selected_facets = [
            f for f in selected_facets
            if f is not populaire_serie_facet and f is not type_productlijn_facet
        ]
        # Render colour modifiers BEFORE the brand+series chunk so the final order
        # is "<colour> <merk> <populaire_serie> <type_productlijn> <rest>". Avoids
        # the AI inserting the colour in the middle of the chunk. Only kleur/
        # kleurtint count here (kleurcombi stays as a suffix); materiaal is left
        # alone because it often reads better as an adjective adjacent to the
        # productnaam ("Asics Gel Nimbus leren herenschoenen").
        colour_facets = [
            f for f in selected_facets
            if (f.get('url_name') or '').lower().startswith('kleur')
            and not (f.get('url_name') or '').lower().startswith('kleurcombi')
        ]
        for cf in colour_facets:
            cv = cf['detail_value']
            pre_chunk_modifiers.append(cv)
            api_h1 = re.sub(r'\b' + re.escape(cv) + r'\b', '', api_h1, count=1, flags=re.IGNORECASE)
        api_h1 = re.sub(r'\s+', ' ', api_h1).strip()
        if colour_facets:
            selected_facets = [f for f in selected_facets if f not in colour_facets]

    # Collect brand/productlijn to strip from AI input and prepend in code after
    # This avoids AI misplacing multi-word brands like "The Indian Maharadja".
    # A lead facet is dropped when its full value is the prefix of another remaining
    # facet's value (case-insensitive) — e.g. Productlijn="Lenovo IdeaPad" already
    # covered by Modelnaam="Lenovo Ideapad 5" — to prevent the prepend from causing
    # "Lenovo IdeaPad Lenovo Ideapad 5"-style duplication in the final title.
    lead_values = []  # Will be prepended to final title in order
    for lead_facet_name in ('merk', 'productlijn'):
        lead_facet = next((f for f in selected_facets if f['facet_name'].lower() == lead_facet_name), None)
        if not lead_facet:
            continue
        lead_val = lead_facet['detail_value']
        norm_lead = _norm_ws(lead_val)
        other_facets = [f for f in selected_facets if f is not lead_facet]
        is_redundant_prefix = norm_lead and any(
            _norm_ws(f['detail_value']) == norm_lead
            or _norm_ws(f['detail_value']).startswith(norm_lead + ' ')
            for f in other_facets
        )
        if is_redundant_prefix:
            # Drop entirely: the more specific facet already carries the lead value.
            selected_facets = [f for f in selected_facets if f is not lead_facet]
            # Also strip the bare lead_val occurrence from api_h1 (case-insensitive,
            # word-boundary) so the AI doesn't echo it back alongside the longer
            # facet value. The model facet still feeds the AI the full string, so
            # nothing is lost.
            api_h1 = re.sub(
                r'\b' + re.escape(lead_val) + r'\b\s*',
                '',
                api_h1,
                count=1,
                flags=re.IGNORECASE,
            ).strip()
            api_h1 = re.sub(r'\s+', ' ', api_h1)
            continue
        lead_values.append(lead_val)
        # Strip from H1 so AI doesn't see it
        if lead_val in api_h1:
            api_h1 = api_h1.replace(lead_val, '').strip()
            while '  ' in api_h1:
                api_h1 = api_h1.replace('  ', ' ')
        # Remove from selected_facets so AI doesn't get it as facet either
        selected_facets = [f for f in selected_facets if f is not lead_facet]

    # Drop base color (Kleur) when a more specific shade (Kleurtint) or combination (Kleurcombinaties) is present
    # e.g., Kleur="Zwarte" + Kleurcombinaties="Zwart/goud" → drop "Zwarte"
    kleur_facet = next((f for f in selected_facets if f['facet_name'].lower() == 'kleur'), None)
    kleurtint_facet = next((f for f in selected_facets if f['facet_name'].lower().startswith('kleurtint') or f['facet_name'].lower().startswith('kleurcombi')), None)
    if kleur_facet and kleurtint_facet:
        selected_facets = [f for f in selected_facets if f is not kleur_facet]
        # Strip base color from H1
        kleur_val = kleur_facet['detail_value']
        if kleur_val in api_h1:
            api_h1 = api_h1.replace(kleur_val + ' ', '', 1).strip()

    # Drop general audience (Kinder/Baby) when a more specific one (Meisjes/Jongens) is present
    # Value-based: any facet with a general value is dropped when any facet has a specific child value
    general_audiences = {'kinder', 'kinderen', 'baby'}
    specific_child_values = {'meisjes', 'jongens'}
    has_specific_child = any(f['detail_value'].lower() in specific_child_values for f in selected_facets)
    if has_specific_child:
        general_facets = [f for f in selected_facets if f['detail_value'].lower() in general_audiences]
        for gf in general_facets:
            selected_facets = [f for f in selected_facets if f is not gf]
            gf_val = gf['detail_value']
            if gf_val in api_h1:
                api_h1 = api_h1.replace(gf_val + ' ', '', 1).strip()
        # Also strip "Kinder"/"Kinderen" from H1 when embedded in category name (e.g., "Kinderfietsen")
        if api_h1.lower().startswith('kinder') and not any(f['detail_value'].lower().startswith('kinder') for f in selected_facets):
            api_h1 = api_h1[6:]  # Strip "Kinder" prefix

    # Strip redundant category name when a "Soort" facet already contains the product type
    # e.g., Soort="Parka jassen" + category_name="Jacks" → H1 "Parka jassen jacks" → strip "jacks"
    soort_facet = next((f for f in selected_facets if f['facet_name'].lower() == 'soort'), None)
    if soort_facet and category_name:
        soort_val = soort_facet['detail_value']
        # Check if the soort value ends with a product type word
        soort_last_word = soort_val.rsplit(None, 1)[-1].lower() if soort_val else ''
        is_product_type = soort_last_word.endswith(_PRODUCT_TYPE_SUFFIXES)
        if is_product_type:
            # Strip trailing category name from H1 (case-insensitive)
            cat_pattern = re.compile(r'\s+' + re.escape(category_name) + r'\s*$', re.IGNORECASE)
            api_h1 = cat_pattern.sub('', api_h1).strip()

    # Sizes: appended after AI runs (to prevent "met Maat" errors).
    # Met-features: passed to AI with a hint to wrap them in "met X" clause.
    # Regular: passed to AI as-is.
    size_values = []       # Display values to append at end (e.g., "Maat 57")
    size_originals = []    # Original values to strip from H1 (e.g., "57")
    suffix_values = []     # Values appended after title but before size (e.g., "Zwart/goud")
    suffix_originals = []  # Original values to strip from H1
    voor_values = []       # "voor" target group values (e.g., "voor mannen") - appended after title
    voor_originals = []    # Original values to strip from H1
    met_values = []
    non_size_facets = []
    for f in selected_facets:
        val = f['detail_value']
        fname = f['facet_name'].lower()
        if is_spec_value(val, fname):
            size_originals.append(val)
            # Prepend "Maat" to bare numbers from maat facets (e.g., "57" → "Maat 57")
            if fname.startswith('maat') and not val.lower().startswith('maat') and val.replace('.', '').replace(',', '').replace('-', '').strip().isdigit():
                val = f"Maat {val}"
            # Strip trailing inflected adjective for end-placement
            # "60 cm brede" → "60 cm breed" (uninflect Dutch adjective at end of title)
            last_word = val.rsplit(None, 1)[-1].lower() if ' ' in val else ''
            if last_word in _ADJ_UNINFLECT:
                val = val[:-(len(last_word))] + _ADJ_UNINFLECT[last_word]
            size_values.append(val)
        elif fname == 'doelgroep_drogisterij':
            voor_originals.append(val)
            voor_values.append(f"voor {val.lower()}")
        elif fname.startswith('kleurcombi'):
            suffix_originals.append(val)
            suffix_values.append(val)
        elif val.lower() == 'volwassenen' or val.lower().startswith('vanaf '):
            suffix_originals.append(val)
            suffix_values.append(val)
        else:
            non_size_facets.append(f)
            # Values already starting with "met"/"zonder" (from API detail_value)
            if val.lower().startswith('met ') or val.lower().startswith('zonder '):
                met_values.append(val)
            # Values ending with "print" (e.g., "Panterprint", "Dierenprint")
            elif val.lower().endswith('print'):
                met_values.append(val)
            # Known feature values that need "met" added
            elif val.lower() in _MET_FEATURE_VALUES:
                met_values.append(val)
            # Facet names that should always be met-features
            elif fname == 'materiaal band':
                met_values.append(val)

    # Strip size, suffix, voor, and met-feature values from the API H1 so the AI doesn't see them
    # (met-features are re-added by AI as "met ..." clause, so strip to avoid duplication)
    ai_h1 = api_h1
    for sv in size_originals + suffix_originals + voor_originals:
        ai_h1 = ai_h1.replace(sv, '').strip()
    for mv in met_values:
        # Strip the full value with its preposition first (e.g., "Zonder WiFi"),
        # otherwise stripping only the bare "WiFi" leaves an orphan "Zonder"
        # that the AI later treats as part of the title (→ "Zonder televisies
        # zonder WiFi").
        full_pat = re.compile(re.escape(mv), re.IGNORECASE)
        ai_h1 = full_pat.sub('', ai_h1).strip()
        # Then strip the bare value (e.g., "Korte mouwen") in case the API H1
        # contains it without the "met "/"zonder " prefix.
        clean_mv = mv
        if clean_mv.lower().startswith('met '):
            clean_mv = clean_mv[4:]
        elif clean_mv.lower().startswith('zonder '):
            clean_mv = clean_mv[7:]
        pattern = re.compile(re.escape(clean_mv), re.IGNORECASE)
        ai_h1 = pattern.sub('', ai_h1).strip()
    # Clean up double spaces
    while '  ' in ai_h1:
        ai_h1 = ai_h1.replace('  ', ' ')

    # Build facet values list - only non-size facets
    facet_values = [f['detail_value'] for f in non_size_facets]
    facet_values_str = ", ".join([f'"{v}"' for v in facet_values])

    # Build facet info for context - only non-size facets
    facet_info = ", ".join([f"{f['facet_name']}: \"{f['detail_value']}\"" for f in non_size_facets])

    # Build met-features rule (only include if there are met values)
    met_section = ""
    if met_values:
        # Strip "Met "/"Zonder " prefixes so AI can bundle into one clause
        clean_met = []
        zonder_values = []
        for mv in met_values:
            if mv.lower().startswith('met '):
                clean_met.append(mv[4:])  # strip "Met "
            elif mv.lower().startswith('zonder '):
                zonder_values.append(mv[7:])  # strip "Zonder "
            else:
                clean_met.append(mv)

        met_parts = []
        if clean_met:
            met_parts.append(f"met {', '.join(clean_met[:-1]) + ' en ' + clean_met[-1] if len(clean_met) > 1 else clean_met[0]}")
        if zonder_values:
            met_parts.append(f"zonder {', '.join(zonder_values[:-1]) + ' en ' + zonder_values[-1] if len(zonder_values) > 1 else zonder_values[0]}")
        example_clause = " ".join(met_parts)

        met_section = f"""
PRODUCTEIGENSCHAPPEN — verplichte clause: "{example_clause}" — MOET na de productnaam staan, NOOIT ervoor.
"""
        met_rule = f"""7. PRODUCTEIGENSCHAPPEN — KRITIEKE PLAATSINGSREGEL: De clause "{example_clause}" MOET direct NA de productnaam staan. Gebruik precies deze formulering, en zet hem NOOIT vooraan in de titel of vóór de doelgroep/merk/productnaam.

   Volgorde in de titel is altijd: <merk> <doelgroep/kleur> <productnaam> <{example_clause}> <maat>
   - FOUT: "Zonder beugel Kinder bh's 70A"  (zonder-clause vóór doelgroep en productnaam)
   - GOED: "Kinder bh's zonder beugel 70A"
   - FOUT: "Met capuchon Heren jassen"
   - GOED: "Heren jassen met capuchon"
   - FOUT: "met lange mouwen Dames poloshirts"
   - GOED: "Dames poloshirts met lange mouwen"

   Als de clause uit de input vooraan staat, VERPLAATS hem zelf naar achter de productnaam.
"""
    else:
        met_rule = """7. Voeg NOOIT het woord "met" toe aan de titel.
"""

    prompt = _build_polish_prompt(
        ai_h1=ai_h1,
        facet_info=facet_info,
        facet_values_str=facet_values_str,
        met_section=met_section,
        met_rule=met_rule,
        mode=prompt_mode,
    )

    try:
        _t_polish_start = time.time()
        response = client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100,
            temperature=0.3
        )
        _t_polish_ms = (time.time() - _t_polish_start) * 1000

        improved_h1 = response.choices[0].message.content.strip().strip('"')

        # Strip trailing "met" if AI left it dangling (happens when no features)
        if improved_h1.endswith(' met'):
            improved_h1 = improved_h1[:-4]

        # Remove hallucinated words that aren't in the input
        improved_h1 = _apply_hallucination_guard(
            improved_h1, ai_h1, non_size_facets, mode=halluc_mode
        )

        # Prepend brand/productlijn (stripped before AI, prepended in code).
        # When a populaire_serie+type_productlijn combo was detected, the order is
        # <colour> <merk> <populaire_serie> <type_productlijn> <rest>.
        prefix_chunks = list(pre_chunk_modifiers) + list(lead_values)
        if series_combined_chunk:
            prefix_chunks.append(series_combined_chunk)
        if prefix_chunks:
            improved_h1 = ' '.join(prefix_chunks) + ' ' + improved_h1

        # Append suffix values (e.g., color combos), voor values, then size values at the end
        if suffix_values:
            improved_h1 = improved_h1.rstrip() + " " + " ".join(suffix_values)
        if voor_values:
            improved_h1 = improved_h1.rstrip() + " " + " ".join(voor_values)
        if size_values:
            improved_h1 = improved_h1.rstrip() + " " + " ".join(size_values)

        # Re-append the category name if the AI dropped it. Only fires when the
        # type-facet classifier did NOT strip it earlier (in that branch
        # category_name was set to ''), so this won't undo legitimate type-facet
        # stripping. Size values were appended above; insert the productnaam
        # before them so the order stays "<...> <productnaam> <maat>". Use a
        # word-boundary check so e.g. "Voer" isn't mistaken as present in
        # "voeding" (a hallucinated word).
        if category_name:
            cat_present = re.search(
                r'\b' + re.escape(category_name) + r'\b', improved_h1, re.IGNORECASE
            )
            if not cat_present:
                if size_values:
                    size_suffix = " " + " ".join(size_values)
                    if improved_h1.endswith(size_suffix):
                        head = improved_h1[: -len(size_suffix)].rstrip()
                        improved_h1 = f"{head} {category_name}{size_suffix}"
                    else:
                        improved_h1 = improved_h1.rstrip() + " " + category_name
                else:
                    improved_h1 = improved_h1.rstrip() + " " + category_name

        # Capitalize first letter (unless it's a brand that starts lowercase, e.g. "iPhone")
        if improved_h1 and improved_h1[0].islower():
            first_word = improved_h1.split()[0]
            # Check if the first word is a lead value (brand/productlijn) with intentional lowercase
            is_lowercase_brand = first_word in lead_values
            if not is_lowercase_brand:
                improved_h1 = improved_h1[0].upper() + improved_h1[1:]

        print(f"[AI_TITLES] timings url={url} fetch={_t_fetch_ms:.0f}ms polish={_t_polish_ms:.0f}ms")
        return {
            "h1_title": _dedupe_facet_values(
                _dedupe_internal_compounds(
                    _dedupe_prefix_overlap(
                        _dedupe_compound_category(_strip_pre_clause_duplicates(improved_h1), canonical_category)
                    )
                ),
                selected_facets,
            ),
            "original_h1": api_h1,
        }

    except Exception as e:
        print(f"[AI_TITLES] OpenAI improvement error for {url}: {e}")
        # Return API H1 as fallback
        return {
            "h1_title": _dedupe_facet_values(
                _dedupe_internal_compounds(
                    _dedupe_prefix_overlap(
                        _dedupe_compound_category(_strip_pre_clause_duplicates(api_h1), canonical_category)
                    )
                ),
                selected_facets,
            ),
            "original_h1": api_h1,
        }


# ---------------------------------------------------------------------------
# Pipeline v3 — deterministic builder + AI polish (EXPERIMENTAL — IN FRIDGE)
# ---------------------------------------------------------------------------
#
# STATUS: NOT default. Opt-in via AI_TITLES_PIPELINE=v3 env var.
# Originally shelved 2026-05-06 at ~76% acceptable. Thawed 2026-05-08 for an
# update pass — still in the fridge but with several regressions addressed:
#   - Category-override reused from v1 (batch_classify_facets +
#     _NEVER_/_ALWAYS_TYPE_URL_SLUGS): a t-facet whose value carries the
#     product noun ("wandplaten" in Wanddecoratie) suppresses the canonical
#     category_name, fixing the "Wanten Handschoenen" redundancy class.
#   - generate_title_v3(polish=False) codepath added — A/B showed polish
#     changed output in only 12-17/100 cases. User signal 2026-05-08:
#     "looks fine without polishing" → polish=False is the favored path.
#   - Standalone "Met"/"Zonder" lowercased mid-title.
#   - Conditie facet detected (fname=='conditie' or 'conditie' in slug) and
#     placed at the END of the H1 (Nieuw/Gebruikt/Refurbished after size).
#   - Color precedence: kleurtint and kleur*combi* are more specific than
#     generic kleur. When either is present, generic kleur is suppressed.
#     kleurtint takes the front color slot; kleurcombi keeps post-category.
# Open regressions still blocking promotion:
#   1. Non-brand agglutination errors when polish=True ("damedeodorant",
#      "herenspolshorloges"). Doesn't apply on the polish=False path.
#   2. Brand acronym lowercasing in builder ("HEMA" → "Hema").
#   3. Brand mangling on "&" ("Heckett & Lane" → "Bruine & Lane …").
# See cc1/LEARNINGS.md for the full A/B journey and design rationale.
#
# Replaces the v1 pipeline's strip-and-prepend dance + full AI rewrite with:
#   1. Compose H1 deterministically from facets (no api_h1).
#   2. (Optional) hand to AI for polish only (inflection, agglutination).
#   3. Content-preservation guard (token-set, allows agglutination/inflection).
#   4. Brand-preservation guard (no brand may be swallowed into a compound).
#   5. Casing restoration from composed_h1 (overrides AI's case decisions).
#   6. Same 5 dedup safety nets (cheap insurance).
#
# Public entry point: generate_title_v3(url, polish=True). Same return shape as
# generate_title_from_api. polish=False skips OpenAI entirely.

_POLISH_PROMPT_V3_TEMPLATE = """Je krijgt een Nederlandse SEO-titel waarvan de woorden en hun volgorde correct zijn. Polijst alleen de Nederlandse grammatica.

Regels:
1. Pas adjectiefverbuigingen toe waar nodig (Klein → Kleine, Diep → Diepe, Dimbaar → Dimbare, Modern → Moderne, Rond → Ronde, Waterdicht → Waterdichte).
2. Maak Nederlandse samenstellingen waar dat de standaard is (Kinder Jurken → Kinderjurken, Heren Schoenen → Herenschoenen, Dames Tassen → Damestassen).
3. Zet niet-eigennamen NÁ het eerste woord in kleine letters; eigennamen, merken en afkortingen behouden hoofdletters (LED, RVS, USB, Apple, Samsung).
4. Voeg GEEN woorden toe en verwijder GEEN woorden — ook geen "in", "van", "voor", "met".
5. Verander de woordvolgorde NIET.

Titel: "{composed_h1}"

Geef ALLEEN de gepolijste titel terug, geen uitleg."""


def _build_v3_h1(selected_facets: list, category_name: str) -> str:
    """Compose an H1 from the facets without using Beslist's api_h1 or the AI.

    Slot order:
        <colour> <merk> <populaire_serie> <type_productlijn> <productlijn>
        <materials> <other adjectives> <doelgroep> <category>
        <met-clauses> <voor-clauses> <color-combos> <size>

    Uses detail_value (SOD) — Beslist's prefix-friendly form. Same dedup
    safety nets the v1 pipeline runs are applied at the end.
    """
    # Empty category_name is allowed (caller passes '' when category-override
    # is active — a type-facet's value carries the product noun). Only bail
    # if we have nothing to compose at all.
    if not category_name and not selected_facets:
        return ''
    brand = ''; populaire_serie = ''; type_productlijn = ''; productlijn = ''
    colors: List[str] = []
    kleurtint: List[str] = []  # specific hue facet — supersedes generic kleur
    color_combos: List[str] = []
    materials: List[str] = []
    other_adj: List[str] = []
    doelgroep: List[str] = []
    met_clauses: List[str] = []
    voor_values: List[str] = []
    sizes: List[str] = []
    conditions: List[str] = []
    for f in selected_facets or []:
        sod = (f.get('detail_value') or '').strip()
        if not sod:
            continue
        url_slug = (f.get('url_name') or '').lower()
        fname = (f.get('facet_name') or '').lower()
        if is_spec_value(sod, fname):
            sizes.append(sod); continue
        # Condition facet (Dutch: 'conditie' — values like Nieuw / Gebruikt /
        # Refurbished). Goes at the END of the H1.
        if fname == 'conditie' or 'conditie' in url_slug or 'condition' in url_slug:
            conditions.append(sod); continue
        if fname == 'merk':
            brand = sod; continue
        if url_slug == 'populaire_serie':
            populaire_serie = sod; continue
        if url_slug == 'type_productlijn':
            type_productlijn = sod; continue
        if fname == 'productlijn':
            productlijn = sod; continue
        # Color precedence: kleurtint (specific hue) and kleur*combi*
        # (combination) are more specific than generic kleur. Detect on either
        # url_slug or facet_name. Both checked BEFORE the generic kleur match
        # below; if a specific bucket fires, the generic colors[] is wiped
        # after the loop.
        if url_slug.startswith('kleurtint') or 'kleurtint' in fname:
            kleurtint.append(sod); continue
        if ('kleur' in url_slug and 'combi' in url_slug) or ('kleur' in fname and 'combi' in fname):
            color_combos.append(sod); continue
        if url_slug.startswith('kleur') or fname.startswith('kleur'):
            colors.append(sod); continue
        if fname == 'materiaal' or url_slug == 'materials':
            materials.append(sod); continue
        if fname.startswith('doelgroep'):
            doelgroep.append(sod); continue
        low = sod.lower()
        if low.startswith('met ') or low.startswith('zonder '):
            met_clauses.append(sod); continue
        if low.startswith('voor ') or low.startswith('vanaf '):
            voor_values.append(sod); continue
        other_adj.append(sod)

    # If a more specific color facet (kleurtint or kleur*combi*) is present,
    # drop the generic kleur bucket — the user-facing rule is "use the more
    # specific one only". kleurtint takes the front color slot in place of
    # kleur; kleurcombi keeps its post-category slot below.
    if kleurtint or color_combos:
        colors = []
    front_colors = kleurtint if kleurtint else colors

    parts: List[str] = []
    parts.extend(front_colors)
    if brand:
        parts.append(brand)
    if populaire_serie:
        parts.append(populaire_serie)
    if type_productlijn:
        parts.append(type_productlijn)
    if productlijn and productlijn.lower() != brand.lower():
        parts.append(productlijn)
    parts.extend(materials)
    parts.extend(other_adj)
    parts.extend(doelgroep)
    parts.append(category_name)
    parts.extend(met_clauses)
    parts.extend(voor_values)
    parts.extend(color_combos)
    parts.extend(sizes)
    parts.extend(conditions)

    h1 = ' '.join(p for p in parts if p)
    h1 = _strip_pre_clause_duplicates(h1)
    h1 = _dedupe_compound_category(h1, category_name)
    h1 = _dedupe_prefix_overlap(h1)
    h1 = _dedupe_internal_compounds(h1)
    h1 = _dedupe_facet_values(h1, selected_facets or [])
    # Lowercase standalone "Met"/"Zonder" when not the first word — Dutch
    # connector words inside an H1 read better lowercase.
    h1 = re.sub(r'(?<=\S)\s+(Met|Zonder)\b', lambda m: ' ' + m.group(1).lower(), h1)
    if h1 and h1[0].islower():
        h1 = h1[0].upper() + h1[1:]
    return h1


_V3_STOPWORDS = {
    'en', 'of', 'met', 'voor', 'in', 'op', 'aan', 'bij', 'tot', 'van',
    'om', 'door', 'over', 'onder', 'naar', 'tussen', 'uit', 'tegen',
    'a', 'the', 'de', 'het', 'een', 'zonder',
}


def _v3_preserves_brands(composed: str, polished: str, selected_facets: list) -> bool:
    """Verify every brand-class token from selected_facets appears as a
    standalone token in `polished` (case-insensitive). Catches the AI
    swallowing brands into agglutinated compounds — e.g. "Ara Pumps" →
    "arapumps" (Ara lost). Common-noun agglutinations like "Heren Schoenen"
    → "herenschoenen" pass through because Heren is a doelgroep facet,
    not a brand-class facet.
    """
    brand_tokens: set = set()
    for f in selected_facets or []:
        url_slug = (f.get('url_name') or '').lower()
        fname = (f.get('facet_name') or '').lower()
        if fname == 'merk' or url_slug in ('merk', 'populaire_serie',
                                            'type_productlijn', 'productlijn'):
            for t in re.findall(r"[\w\-]+", f.get('detail_value') or ''):
                if len(t) >= 2:
                    brand_tokens.add(t.lower())
    if not brand_tokens:
        return True
    polished_lower = {t.lower() for t in re.findall(r"[\w\-]+", polished)}
    for bt in brand_tokens:
        if bt not in polished_lower:
            return False
    return True


def _v3_restore_casing(composed: str, polished: str) -> str:
    """Replace each polished token with its original casing from `composed`
    (case-insensitive token match). Skips tokens that aren't in `composed`
    (e.g. agglutinated forms, AI-applied inflections) — those keep the
    polish output's casing.

    This sidesteps the polish AI's tendency to lowercase brands and acronyms
    (`Mercedes` → `mercedes`, `RVS` → `rvs`, `LCD-scherm` → `lcd-scherm`).
    Original casing comes deterministically from the composed builder
    (which uses Beslist's facet detail_value casing).
    """
    if not composed or not polished:
        return polished
    case_map: Dict[str, str] = {}
    for tok in re.findall(r"[\w\-]+", composed):
        case_map[tok.lower()] = tok
    def _swap(m):
        tok = m.group(0)
        return case_map.get(tok.lower(), tok)
    return re.sub(r"[\w\-]+", _swap, polished)


def _v3_preserves_content(composed: str, polished: str) -> bool:
    """Return True iff every meaningful token from `composed` still appears
    in `polished` (possibly agglutinated, case-insensitive). Used as a
    deterministic guard so the polish AI cannot silently drop brand names,
    model numbers, or facet values. If False the caller falls back to the
    composed h1 unmodified.

    "Meaningful" = ≥2 chars, not a stopword. Substring containment handles
    Dutch agglutination ("kinder" + "jurken" → "kinderjurken" passes since
    both substrings remain in the polished output).
    """
    if not composed:
        return True
    polished_lower = polished.lower()
    for tok in re.findall(r"[\w\-]+", composed):
        tl = tok.lower()
        if tl in _V3_STOPWORDS:
            continue
        if len(tl) < 2:
            continue
        if tl in polished_lower:
            continue
        # Allow plural-strip variations: -s, -en, -e (Dutch inflection)
        if len(tl) > 4 and tl[:-1] in polished_lower:
            continue
        if len(tl) > 5 and tl[:-2] in polished_lower:
            continue
        # Token genuinely missing from polished output.
        return False
    return True


def generate_title_v3(url: str, polish: bool = True) -> Optional[Dict]:
    """v3 pipeline: deterministic compose + (optional) AI polish.

    Same return shape as generate_title_from_api so callers can swap.
    Adds `composed_h1` to the result for diagnostics.

    When polish=False, skips the OpenAI polish call entirely and returns the
    deterministic composed_h1 as h1_title. Used for A/B testing whether the
    polish step is worth its cost.
    """
    page_data = fetch_products_api(url, include_related=False)
    if not page_data:
        print(f"[AI_TITLES_V3] API fetch failed for {url}")
        return None
    if page_data.get("error"):
        print(f"[AI_TITLES_V3] API error for {url}: {page_data.get('error')}")
        return None

    selected_facets = page_data.get("selected_facets") or []
    category_name = page_data.get("category_name") or ""
    api_h1 = page_data.get("h1_title") or ""

    # Reuse v1's category-override mechanism (lines ~938-968): if any selected
    # facet is a "type-facet" (its values inherently carry the product noun,
    # e.g. t_wanddeco→"wandplaten" in Wanddecoratie), suppress category_name so
    # the composed H1 doesn't redundantly append it ("Wandplaten Wanddecoratie",
    # "Wanten Handschoenen", "Ventilatieventielen Ventilatiematerialen").
    from backend.facet_classifier import batch_classify_facets
    type_class = batch_classify_facets(selected_facets, category_name)
    _NEVER_URL_SLUGS = {'type_productlijn', 'personage', 'seizoen_schoenen'}
    _ALWAYS_TYPE_URL_SLUGS = {'t_stoel'}
    has_category_override = any(
        (f.get('url_name') or '').lower() in _ALWAYS_TYPE_URL_SLUGS
        or type_class.get((f.get('facet_name') or '').lower().strip(), False)
        for f in selected_facets
        if (f.get('url_name') or '').lower() not in _NEVER_URL_SLUGS
    )
    effective_category = '' if has_category_override else category_name

    composed_h1 = _build_v3_h1(selected_facets, effective_category)
    if not composed_h1:
        print(f"[AI_TITLES_V3] empty composed h1 for {url} — falling back to api_h1")
        return {"h1_title": api_h1, "original_h1": api_h1, "composed_h1": ""}

    if not polish:
        # Explicit no-polish path: deterministic builder output is final.
        return {"h1_title": composed_h1, "original_h1": api_h1, "composed_h1": composed_h1}

    client = get_openai_client()
    if not client:
        # No OpenAI configured — return the composed H1 as-is.
        return {"h1_title": composed_h1, "original_h1": api_h1, "composed_h1": composed_h1}

    prompt = _POLISH_PROMPT_V3_TEMPLATE.format(composed_h1=composed_h1)
    polished = composed_h1
    try:
        response = client.chat.completions.create(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": "Je bent een Nederlandse taalexpert. Polijst alleen grammatica zonder woorden toe te voegen, te verwijderen of te herordenen."},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
            max_tokens=200,
        )
        polished = (response.choices[0].message.content or '').strip().strip('"').strip("'")
        if not polished:
            polished = composed_h1
    except Exception as e:
        print(f"[AI_TITLES_V3] polish failed for {url}: {e} — using composed h1")
        polished = composed_h1

    # Content-preservation guard: if the polish dropped a meaningful token
    # (brand, model number, etc.) fall back to the composed h1 unmodified.
    # Catches cases like "Sony WH-1000XM3 Koptelefoons" → "Sony koptelefoons"
    # where the AI silently rewrites instead of polishing.
    if not _v3_preserves_content(composed_h1, polished):
        print(f"[AI_TITLES_V3] polish dropped content for {url}; falling back to composed_h1")
        polished = composed_h1
    elif not _v3_preserves_brands(composed_h1, polished, selected_facets):
        print(f"[AI_TITLES_V3] polish swallowed a brand into a compound for {url}; falling back to composed_h1")
        polished = composed_h1

    # NOTE: v3 deliberately does NOT run _apply_hallucination_guard. The
    # hallucination guard's prefix-match (length-diff ≤3) wrongly rejects
    # legitimate Dutch agglutination — e.g. polished "koraaltops" (10 chars)
    # would not match input "koraal" (6 chars) or "tops" (4 chars) closely
    # enough and gets stripped. The content-preservation guard above already
    # verifies that no meaningful token was dropped, which is what the
    # hallucination guard was protecting against in v1.

    # Restore original casing from composed_h1 — overrides any case the
    # polish AI applied. Agglutinated tokens / inflected forms not in the
    # composed map keep the polish casing.
    polished = _v3_restore_casing(composed_h1, polished)

    # Cheap insurance — run the same dedup passes the v1 pipeline runs.
    polished = _strip_pre_clause_duplicates(polished)
    polished = _dedupe_compound_category(polished, category_name)
    polished = _dedupe_prefix_overlap(polished)
    polished = _dedupe_internal_compounds(polished)
    polished = _dedupe_facet_values(polished, selected_facets)

    # Ensure first character is uppercase (composed builder already does
    # this; case-restoration may revert it if first token was lowercase
    # in the composed source — uncommon).
    if polished and polished[0].islower():
        polished = polished[0].upper() + polished[1:]

    return {
        "h1_title": polished,
        "original_h1": api_h1,
        "composed_h1": composed_h1,
    }


def update_title_record(url: str, h1_title: str, title: str, description: str, original_h1: str = None, error: str = None):
    """Persist an AI title-generation outcome for `url`.

    On error: bumps unique_titles_jobs.status='failed', records last_error.
    On success: writes/updates unique_titles_content + sets job status='success'.
    """
    from backend.url_catalog import get_url_id  # local import to avoid cycles
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        url_id = get_url_id(cur, url)
        if url_id is None:
            print(f"[AI_TITLES] Cannot canonicalize URL: {url!r}")
            return False
        if error:
            cur.execute("""
                INSERT INTO pa.unique_titles_jobs (url_id, status, last_error, created_at, updated_at)
                VALUES (%s, 'failed', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (url_id) DO UPDATE SET
                    status = 'failed',
                    last_error = EXCLUDED.last_error,
                    updated_at = CURRENT_TIMESTAMP
            """, (url_id, error))
        else:
            cur.execute("""
                INSERT INTO pa.unique_titles_content
                    (url_id, h1_title, title, description, original_h1, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (url_id) DO UPDATE SET
                    h1_title = EXCLUDED.h1_title,
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    original_h1 = EXCLUDED.original_h1,
                    updated_at = CURRENT_TIMESTAMP
            """, (url_id, h1_title, title, description, original_h1))
            cur.execute("""
                INSERT INTO pa.unique_titles_jobs (url_id, status, last_error, created_at, updated_at)
                VALUES (%s, 'success', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (url_id) DO UPDATE SET
                    status = 'success',
                    last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
            """, (url_id,))
        conn.commit()
        return True
    except Exception as e:
        print(f"[AI_TITLES] DB update error for {url}: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        return_db_connection(conn)


def process_single_url(url: str, use_api: bool = True) -> Dict:
    """Process a single URL for AI title generation.

    Args:
        url: The URL to process
        use_api: If True, use productsearch API + OpenAI for faceted URLs.
                 If False, always use scraping + OpenAI method.
    """
    result = {"url": url, "status": "pending"}

    try:
        # Check if URL has facets (contains "~~" or "/c/")
        has_facets = "~~" in url or "/c/" in url

        # Pipeline switch: AI_TITLES_PIPELINE=v3 enables the deterministic
        # builder + AI polish path. Default 'v1' = current full-rewrite pipeline.
        # Set to 'v3' via env var or per-batch override to A/B compare.
        _pipeline = os.getenv("AI_TITLES_PIPELINE", "v1").lower()
        if _pipeline == "v3":
            ai_result = generate_title_v3(url)
        else:
            ai_result = generate_title_from_api(url)

        if not ai_result:
            result["status"] = "failed"
            result["reason"] = "API could not fetch data for URL"
            update_title_record(url, None, None, None, error="api_failed")
            print(f"[AI_TITLES] API failed for {url}")
            return result

        new_h1 = ai_result["h1_title"]
        original_h1 = ai_result.get("original_h1", new_h1)

        # Step 3: Apply text formatting
        # Format dimensions (e.g., "31 cm 115 cm" -> "31 cm x 115 cm")
        new_h1 = format_dimensions(new_h1)
        # Normalize preposition case (e.g., "Met glitter" -> "met glitter" unless at start)
        new_h1 = normalize_preposition_case(new_h1)
        # Strip "met met X" / "met zonder X" -> "met X" / "zonder X"
        new_h1 = fix_redundant_met(new_h1)

        # Step 4: Create SEO title
        # Format: "{h1} kopen? ✔️ Tot !!DISCOUNT!! korting! | beslist.nl"
        seo_title = f"{new_h1} kopen? ✔️ Tot !!DISCOUNT!! korting! | beslist.nl"

        # Step 5: Create SEO description
        # Format: "Zoek je {h1}? &#10062; Vergelijk !!NR!! aanbiedingen en bespaar op je aankoop &#10062; Shop {h1} met !!DISCOUNT!! korting online! &#10062; beslist.nl"
        seo_description = f"Zoek je {new_h1}? &#10062; Vergelijk !!NR!! aanbiedingen en bespaar op je aankoop &#10062; Shop {new_h1} met !!DISCOUNT!! korting online! &#10062; beslist.nl"

        # Step 6: Update database
        if update_title_record(url, new_h1, seo_title, seo_description, original_h1):
            result["status"] = "success"
            result["h1_title"] = new_h1
            result["title"] = seo_title
            result["description"] = seo_description
        else:
            result["status"] = "failed"
            result["reason"] = "Database update failed"

        return result

    except Exception as e:
        result["status"] = "failed"
        result["reason"] = str(e)
        return result


def _process_url_with_delay(url: str, use_api: bool = True) -> Dict:
    """Process a single URL with rate limiting delay."""
    # Check stop flag before processing
    with _state_lock:
        if _processing_state["should_stop"]:
            return {"url": url, "status": "skipped", "reason": "stopped"}

    result = process_single_url(url, use_api=use_api)

    # Rate limit: 0.5s delay = max 2 URLs per worker per second
    time.sleep(0.5)

    return result


def _run_processing(max_urls: int = 100, num_workers: int = 50, use_api: bool = True):
    """Background thread for processing URLs with multiple workers.

    Args:
        max_urls: Maximum number of URLs to process in this batch. If 0, process all pending.
        num_workers: Number of parallel workers (default 50).
        use_api: If True, use productsearch API for faceted URLs. If False, use scraping.
    """
    global _processing_state

    with _state_lock:
        _processing_state["is_running"] = True
        _processing_state["should_stop"] = False
        _processing_state["processed"] = 0
        _processing_state["successful"] = 0
        _processing_state["failed"] = 0
        _processing_state["skipped"] = 0
        _processing_state["started_at"] = datetime.now()
        _processing_state["last_error"] = None

    try:
        # Get URLs to process (max_urls=0 means all pending)
        urls = get_unprocessed_urls(max_urls)
        total = len(urls)

        with _state_lock:
            _processing_state["total_urls"] = total

        if total == 0:
            print("[AI_TITLES] No URLs to process")
            return

        batch_msg = "all pending" if max_urls == 0 else f"batch of {max_urls}"
        method_msg = "API+OpenAI" if use_api else "Scraping+OpenAI"
        print(f"[AI_TITLES] Starting processing of {total} URLs ({batch_msg}) with {num_workers} workers using {method_msg}")

        # Process URLs using thread pool - submit in small chunks to allow stopping
        chunk_size = num_workers * 2
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            url_index = 0
            while url_index < total:
                # Check stop flag before submitting next chunk
                with _state_lock:
                    if _processing_state["should_stop"]:
                        print("[AI_TITLES] Processing stopped by user")
                        break

                # Submit a chunk of URLs
                chunk_end = min(url_index + chunk_size, total)
                future_to_url = {
                    executor.submit(_process_url_with_delay, urls[i]["url"], use_api): urls[i]["url"]
                    for i in range(url_index, chunk_end)
                }

                # Process results as they complete
                stopped = False
                for future in as_completed(future_to_url):
                    with _state_lock:
                        if _processing_state["should_stop"]:
                            print("[AI_TITLES] Processing stopped by user")
                            stopped = True
                            break

                    url = future_to_url[future]
                    try:
                        result = future.result()

                        with _state_lock:
                            _processing_state["processed"] += 1
                            _processing_state["current_url"] = url
                            if result["status"] == "success":
                                _processing_state["successful"] += 1
                            elif result["status"] == "failed":
                                _processing_state["failed"] += 1
                                _processing_state["last_error"] = f"{result.get('reason', 'Unknown error')} ({url})"
                            else:
                                _processing_state["skipped"] += 1

                    except Exception as e:
                        with _state_lock:
                            _processing_state["processed"] += 1
                            _processing_state["failed"] += 1
                            _processing_state["last_error"] = str(e)

                if stopped:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                url_index = chunk_end

    except Exception as e:
        print(f"[AI_TITLES] Processing error: {e}")
        with _state_lock:
            _processing_state["last_error"] = str(e)

    finally:
        with _state_lock:
            _processing_state["is_running"] = False
            _processing_state["current_url"] = None
        print("[AI_TITLES] Processing complete")


def start_processing(batch_size: int = 100, num_workers: int = 50, use_api: bool = True) -> Dict:
    """Start AI title processing in background.

    Args:
        batch_size: Number of URLs to process in this batch. If 0, process all pending.
        num_workers: Number of parallel workers (default 50).
        use_api: If True, use productsearch API for faceted URLs. If False, use scraping.
    """
    with _state_lock:
        if _processing_state["is_running"]:
            return {"status": "error", "message": "Processing already running"}

    thread = threading.Thread(target=_run_processing, args=(batch_size, num_workers, use_api), daemon=True)
    thread.start()

    batch_msg = "all pending URLs" if batch_size == 0 else f"batch of {batch_size}"
    method_msg = "API+OpenAI" if use_api else "Scraping+OpenAI"
    return {"status": "started", "message": f"AI title processing started ({batch_msg}, {num_workers} workers, {method_msg})"}


def stop_processing() -> Dict:
    """Stop AI title processing."""
    with _state_lock:
        if not _processing_state["is_running"]:
            return {"status": "error", "message": "No processing running"}

        _processing_state["should_stop"] = True

    return {"status": "stopping", "message": "Stop signal sent"}


def get_ai_titles_stats() -> Dict:
    """Get statistics about AI title processing."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        stats = {}

        cur.execute("SELECT COUNT(*) AS count FROM pa.unique_titles_jobs")
        stats["total_urls"] = cur.fetchone()["count"]

        cur.execute("SELECT COUNT(*) AS count FROM pa.unique_titles_jobs WHERE status IN ('success', 'failed')")
        stats["ai_processed"] = cur.fetchone()["count"]

        cur.execute("""
            SELECT COUNT(*) AS count
            FROM pa.unique_titles_jobs j
            LEFT JOIN pa.unique_titles_content c ON c.url_id = j.url_id
            WHERE j.status = 'pending'
              AND (c.title IS NULL OR c.title = '' OR c.h1_title IS NULL OR c.h1_title = '')
        """)
        stats["pending"] = cur.fetchone()["count"]

        cur.execute("SELECT COUNT(*) AS count FROM pa.unique_titles_jobs WHERE last_error IS NOT NULL")
        stats["with_errors"] = cur.fetchone()["count"]

        return stats
    finally:
        cur.close()
        return_db_connection(conn)


def get_recent_results(limit: int = 20) -> List[Dict]:
    """Get recently processed AI titles."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # LIMIT before JOIN — sort the jobs table first, then look up
        # urls + content via primary keys.
        cur.execute("""
            SELECT u.url,
                   c.title,
                   c.h1_title,
                   c.original_h1,
                   j.updated_at AS ai_processed_at,
                   j.last_error AS ai_error
            FROM (
                SELECT url_id, updated_at, last_error
                FROM pa.unique_titles_jobs
                WHERE status IN ('success', 'failed')
                ORDER BY updated_at DESC
                LIMIT %s
            ) j
            JOIN pa.urls u ON j.url_id = u.url_id
            LEFT JOIN pa.unique_titles_content c ON c.url_id = j.url_id
            ORDER BY j.updated_at DESC
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    finally:
        cur.close()
        return_db_connection(conn)


def analyze_and_flag_failures(dry_run: bool = True, min_fail_rate: float = 80, min_failures: int = 5) -> Dict:
    """
    Analyze api_failed URLs for patterns and flag pending URLs that are likely to fail.

    Checks two pattern types:
    1. Structural: malformed URLs (empty facets, triple tildes, wrong prefixes)
    2. Subcategory paths: paths with high historical fail rates

    Args:
        dry_run: If True, only report counts without updating the database
        min_fail_rate: Minimum fail rate % for subcategory paths (default 80)
        min_failures: Minimum number of failures for a subcategory to be considered (default 5)

    Returns:
        Summary dict with flagged counts and breakdown
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        results = {"structural": [], "subcategory": [], "total_flagged": 0}

        # --- 1. Structural patterns ---
        structural_patterns = [
            ("empty_start_facet", "url LIKE '%/c/~~%'"),
            ("triple_tilde", "url LIKE '%~~~%'"),
            ("trailing_tilde", "url LIKE '%~~'"),
            ("facet_without_c_prefix", "url LIKE '/products/%' AND url NOT LIKE '%/c/%' AND url LIKE '%~%'"),
            ("brand_url", "url LIKE '/brand/%'"),
            ("filter_url", "url LIKE '/filters/%'"),
            ("non_product_url", "url NOT LIKE '/products/%' AND url NOT LIKE '/brand/%' AND url NOT LIKE '/filters/%'"),
        ]

        for pattern_name, where_clause in structural_patterns:
            # Adjust the URL filter to reference pa.urls.url
            url_filter = where_clause.replace("url ", "u.url ")
            cur.execute(f"""
                SELECT COUNT(*) AS cnt
                FROM pa.unique_titles_jobs j
                JOIN pa.urls u ON j.url_id = u.url_id
                WHERE j.status = 'pending' AND ({url_filter})
            """)
            count = cur.fetchone()["cnt"]

            if count > 0:
                if not dry_run:
                    cur.execute(f"""
                        UPDATE pa.unique_titles_jobs j
                           SET status = 'failed',
                               last_error = 'predicted_fail:structural:{pattern_name}',
                               updated_at = CURRENT_TIMESTAMP
                          FROM pa.urls u
                         WHERE j.url_id = u.url_id
                           AND j.status = 'pending'
                           AND ({url_filter})
                    """)

                results["structural"].append({
                    "pattern": pattern_name,
                    "pending_flagged": count,
                })
                results["total_flagged"] += count

        # --- 2. Subcategory path patterns ---
        cur.execute("""
            WITH subcat_stats AS (
                SELECT
                    SUBSTRING(u.url FROM '^(/products/[^/]+/[^/]+)') AS subcat_path,
                    SUM(CASE WHEN j.last_error = 'api_failed' THEN 1 ELSE 0 END) AS failed,
                    SUM(CASE WHEN j.status = 'success' AND j.last_error IS NULL THEN 1 ELSE 0 END) AS succeeded
                FROM pa.unique_titles_jobs j
                JOIN pa.urls u ON j.url_id = u.url_id
                WHERE u.url LIKE '/products/%%/c/%%'
                GROUP BY 1
            )
            SELECT subcat_path, failed, succeeded,
                ROUND(100.0 * failed / NULLIF(failed + succeeded, 0), 1) AS fail_rate
            FROM subcat_stats
            WHERE failed >= %s
              AND 100.0 * failed / NULLIF(failed + succeeded, 0) >= %s
            ORDER BY fail_rate DESC, failed DESC
        """, (min_failures, min_fail_rate))

        high_risk_paths = cur.fetchall()

        for row in high_risk_paths:
            subcat_path = row["subcat_path"]
            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM pa.unique_titles_jobs j
                JOIN pa.urls u ON j.url_id = u.url_id
                WHERE j.status = 'pending'
                  AND u.url LIKE %s
            """, (subcat_path + '%',))
            pending_count = cur.fetchone()["cnt"]

            if pending_count > 0:
                if not dry_run:
                    cur.execute("""
                        UPDATE pa.unique_titles_jobs j
                           SET status = 'failed',
                               last_error = %s,
                               updated_at = CURRENT_TIMESTAMP
                          FROM pa.urls u
                         WHERE j.url_id = u.url_id
                           AND j.status = 'pending'
                           AND u.url LIKE %s
                    """, (f"predicted_fail:subcat:{subcat_path}:{row['fail_rate']}%", subcat_path + '%'))

                results["subcategory"].append({
                    "subcat_path": subcat_path,
                    "historical_failed": row["failed"],
                    "historical_succeeded": row["succeeded"],
                    "fail_rate": float(row["fail_rate"]),
                    "pending_flagged": pending_count,
                })
                results["total_flagged"] += pending_count

        if not dry_run:
            conn.commit()

        results["dry_run"] = dry_run
        results["min_fail_rate"] = min_fail_rate
        results["min_failures"] = min_failures

        print(f"[AI_TITLES] Failure analysis complete: {results['total_flagged']} URLs {'would be' if dry_run else ''} flagged "
              f"({len(results['structural'])} structural patterns, {len(results['subcategory'])} subcategory patterns)")

        return results

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        return_db_connection(conn)
