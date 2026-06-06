"""
AI Title Generation Service v2

Slot-based redesign of generate_title_from_api. The v1 pipeline strips values
from the API h1 and re-adds them after the AI call, which led to a series of
"orphaned preposition", "duplicated category", "brand-prepended-twice" bugs.

v2 builds the H1 from a structured slot dict, calls the AI only to polish
Dutch grammar, then runs the same final dedupe safety nets v1 has. Side
effects (DB writes) are kept out of the generation function so it is easy to
test alongside v1 without modifying state.

Differences vs. v1:
- One batched DB query for facet-type classifications instead of N per URL
  (previous code: classify_facet() opens a DB connection per facet).
- Deterministic slot composition instead of strip-and-replace string surgery.
- Shorter polish prompt (~250 chars) — slot ordering is already correct, so
  the AI is asked to inflect/clean grammar only, not to rebuild the title.
- Single entry point. v1's generate_title_from_api was 480 lines.

Helpers shared with v1 (imported, not duplicated):
- _dedupe_compound_category, _strip_pre_clause_duplicates: final safety nets
- format_dimensions, normalize_preposition_case: cosmetic post-processing
- get_openai_client: shared OpenAI client
- AI_MODEL, USER_AGENT: constants
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from backend.ai_titles_service import (
    AI_MODEL,
    _dedupe_compound_category,
    _strip_pre_clause_duplicates,
    format_dimensions,
    get_openai_client,
    normalize_preposition_case,
    normalize_tv_category_caps,
)
from backend.database import get_db_connection, return_db_connection
from backend.faq_service import fetch_products_api


_MEM_CACHE: dict = {}


# ---------- Facet categorization (extracted from v1, no longer per-URL local) ----------

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
_MET_FEATURE_VALUES = {
    'korte mouwen', 'lange mouwen', 'driekwart mouwen',
    'capuchon',
    'ronde hals', 'v-hals', 'col', 'opstaande kraag',
    'rits', 'knopen', 'drukknopen', 'veters',
    'draaiplateau', 'grill',
    'strepen',
}
_PRODUCT_TYPE_SUFFIXES = (
    'jassen', 'jacks', 'broeken', 'shirts', 'hemden', 'tops', 'blouses',
    'schoenen', 'laarzen', 'sandalen', 'sneakers', 'boots', 'pumps', 'instappers',
    'jurken', 'rokken', 'truien', 'vesten', 'pakken',
    'tassen', 'horloges', 'brillen', 'sieraden',
    'pannen', 'ovens', 'magnetrons', 'koelkasten', 'wasmachines',
    'banken', 'stoelen', 'tafels', 'kasten', 'bedden',
)


def is_spec_value(val: str, fname: str) -> bool:
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


def normalize_size_value(val: str, fname: str) -> str:
    """Render a spec value for the title (e.g., bare "57" → "Maat 57")."""
    is_bare_num = val.replace('.', '').replace(',', '').replace('-', '').strip().isdigit()
    if fname.startswith('maat') and not val.lower().startswith('maat') and is_bare_num:
        val = f"Maat {val}"
    last_word = val.rsplit(None, 1)[-1].lower() if ' ' in val else ''
    if last_word in _ADJ_UNINFLECT:
        val = val[:-len(last_word)] + _ADJ_UNINFLECT[last_word]
    return val


# ---------- Batched facet-type classification ----------

def batch_classify_facets(facets: List[Dict], category_name: str) -> Dict[str, bool]:
    """Look up is_type_facet for every facet in a single DB round-trip.

    v1's classify_facet() opens a connection per facet. Here we hit
    pa.facet_type_classifications once per URL.

    Returns: dict mapping facet_name (lowercased) → is_type_facet (default False
    when no row exists).
    """
    if not facets:
        return {}
    cat = (category_name or '').lower().strip()
    fnames = sorted({(f.get('facet_name') or '').lower().strip() for f in facets if f.get('facet_name')})
    if not fnames:
        return {}
    cache_keys = [(fn, cat) for fn in fnames]
    result: Dict[str, bool] = {}
    missing: List[str] = []
    for fn in fnames:
        ck = (fn, cat)
        if ck in _MEM_CACHE:
            result[fn] = _MEM_CACHE[ck]
        else:
            missing.append(fn)
    if not missing:
        return result
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT facet_name, is_type_facet FROM pa.facet_type_classifications "
            "WHERE sample_category = %s AND facet_name = ANY(%s)",
            (cat, missing),
        )
        rows = cur.fetchall()
        for row in rows:
            fn = row['facet_name']
            v = bool(row['is_type_facet'])
            result[fn] = v
            _MEM_CACHE[(fn, cat)] = v
        for fn in missing:
            if fn not in result:
                result[fn] = False  # default to non-type-facet
                _MEM_CACHE[(fn, cat)] = False
        return result
    finally:
        cur.close()
        return_db_connection(conn)


# ---------- Slot data structure ----------

@dataclass
class TitleSlots:
    lead: List[str] = field(default_factory=list)        # brand / productlijn (prepended)
    modifiers: List[str] = field(default_factory=list)   # color, material, style
    audience: List[str] = field(default_factory=list)    # Heren, Dames, Kinderen, ...
    productname: List[str] = field(default_factory=list) # serie, modelnaam, type
    other: List[str] = field(default_factory=list)       # everything else
    met_clause: List[str] = field(default_factory=list)  # bundled "met X" features
    zonder_clause: List[str] = field(default_factory=list)
    suffix: List[str] = field(default_factory=list)      # voor X / kleurcombi / volwassenen
    size: List[str] = field(default_factory=list)        # appended at end
    category: str = ""                                   # appended before size
    type_facet_overrides_category: bool = False


def _norm_ws(s: str) -> str:
    return ' '.join(s.lower().split())


def categorize_into_slots(
    facets: List[Dict],
    category_name: str,
    type_class: Dict[str, bool],
) -> TitleSlots:
    """Classify each facet into its slot. Pure function: no string surgery."""
    slots = TitleSlots()

    # Whether any selected facet is a type-facet for this category — if so the
    # category name is redundant in the H1.
    slots.type_facet_overrides_category = any(
        type_class.get((f.get('facet_name') or '').lower().strip(), False)
        for f in facets
    )
    slots.category = "" if slots.type_facet_overrides_category else (category_name or "")

    # Pre-pass: detect drop candidates so we don't include them.
    facet_lower = [(f, (f.get('facet_name') or '').lower(), (f.get('detail_value') or '')) for f in facets]
    drop_ids = set()  # python id() of facet dicts to drop

    # 1) Drop merk when another facet's value contains the merk name (case-insensitive).
    merk = next((f for f, fn, _ in facet_lower if fn == 'merk'), None)
    if merk:
        merk_low = merk['detail_value'].lower()
        if any(merk_low in dv.lower() for f, fn, dv in facet_lower if f is not merk):
            drop_ids.add(id(merk))

    # 2) Drop a lead facet (merk, productlijn) whose value is a prefix of any
    #    other remaining facet's value — i.e., the more specific facet already
    #    carries the lead. Avoids "Lenovo IdeaPad Lenovo Ideapad 5".
    for lead_fname in ('merk', 'productlijn'):
        lead = next((f for f, fn, _ in facet_lower if fn == lead_fname and id(f) not in drop_ids), None)
        if not lead:
            continue
        norm_lead = _norm_ws(lead['detail_value'])
        if not norm_lead:
            continue
        for f, fn, dv in facet_lower:
            if f is lead or id(f) in drop_ids:
                continue
            nv = _norm_ws(dv)
            if nv == norm_lead or nv.startswith(norm_lead + ' '):
                drop_ids.add(id(lead))
                break

    # 3) Drop kleur when kleurtint/kleurcombi is present.
    has_kleurtint = any(fn.startswith('kleurtint') or fn.startswith('kleurcombi') for _, fn, _ in facet_lower)
    if has_kleurtint:
        for f, fn, _ in facet_lower:
            if fn == 'kleur':
                drop_ids.add(id(f))

    # 4) Drop kinder/kinderen/baby when meisjes/jongens is present.
    specific_child_present = any(dv.lower() in ('meisjes', 'jongens') for _, _, dv in facet_lower)
    if specific_child_present:
        for f, _, dv in facet_lower:
            if dv.lower() in ('kinder', 'kinderen', 'baby'):
                drop_ids.add(id(f))

    # 5) When a Soort facet contains a product type ending (e.g., "Parka jassen"),
    #    drop the category from slots so it isn't appended (the soort value covers it).
    soort = next((f for f, fn, _ in facet_lower if fn == 'soort'), None)
    if soort and slots.category:
        soort_val = soort['detail_value']
        last_word = soort_val.rsplit(None, 1)[-1].lower() if soort_val else ''
        if last_word.endswith(_PRODUCT_TYPE_SUFFIXES):
            slots.category = ""

    # Now slot the remaining facets.
    color_facet_keys = ('kleur',)  # kleur dropped above when overridden
    material_keys = ('materiaal',)
    style_facet_values = {  # adjectival style keywords usually exposed via various facet names
        'industriële', 'industrieel', 'klassiek', 'klassieke', 'modern', 'moderne',
        'scandinavisch', 'scandinavische', 'vintage', 'rustiek', 'rustieke',
    }
    audience_values = {'heren', 'dames', 'kinderen', 'kinder', 'jongens', 'meisjes',
                       'baby', "baby's", 'unisex'}
    productname_keys = ('serie', 'modelnaam', 'modelnaam_mob', 'model',
                        'type', 'type_koffiezetter', 't_klimplantrek')

    for f, fn, dv in facet_lower:
        if id(f) in drop_ids:
            continue
        if not dv:
            continue

        # Lead values
        if fn == 'merk' or fn == 'productlijn':
            slots.lead.append(dv)
            continue

        # Spec values → size slot (unchanged from v1)
        if is_spec_value(dv, fn):
            slots.size.append(normalize_size_value(dv, fn))
            continue

        # Suffix slots
        if fn == 'doelgroep_drogisterij':
            slots.suffix.append(f"voor {dv.lower()}")
            continue
        if fn.startswith('kleurcombi'):
            slots.suffix.append(dv)
            continue
        if dv.lower() == 'volwassenen' or dv.lower().startswith('vanaf '):
            slots.suffix.append(dv)
            continue

        # Met / zonder clauses
        if dv.lower().startswith('met '):
            slots.met_clause.append(dv[4:])
            continue
        if dv.lower().startswith('zonder '):
            slots.zonder_clause.append(dv[7:])
            continue
        if dv.lower() in _MET_FEATURE_VALUES or dv.lower().endswith('print'):
            slots.met_clause.append(dv)
            continue
        if fn == 'materiaal band':
            slots.met_clause.append(dv)
            continue

        # Audience
        if dv.lower() in audience_values:
            slots.audience.append(dv)
            continue

        # Color
        if fn in color_facet_keys or fn.startswith('kleurtint'):
            slots.modifiers.append(dv)
            continue

        # Material
        if fn in material_keys:
            slots.modifiers.append(dv)
            continue

        # Style adjectives — fall through unless detected
        if dv.lower() in style_facet_values:
            slots.modifiers.append(dv)
            continue

        # Productname (specific product identifiers)
        if any(p in fn for p in productname_keys):
            slots.productname.append(dv)
            continue

        # Default → other (typically used as adjectives before the productnaam)
        slots.other.append(dv)

    return slots


# ---------- Draft rendering ----------

def render_draft(slots: TitleSlots) -> str:
    """Compose the slots into a draft title string. Order:
       <lead> <modifiers> <other> <audience> <productname> <category> <met-clause> <suffix> <size>
    """
    parts: List[str] = []
    parts += slots.lead
    parts += slots.modifiers
    parts += slots.other
    parts += slots.audience
    parts += slots.productname
    if slots.category:
        parts.append(slots.category.lower())
    if slots.met_clause:
        clean = [v for v in slots.met_clause]
        if len(clean) == 1:
            parts.append("met " + clean[0])
        else:
            parts.append("met " + ", ".join(clean[:-1]) + " en " + clean[-1])
    if slots.zonder_clause:
        clean = slots.zonder_clause
        if len(clean) == 1:
            parts.append("zonder " + clean[0])
        else:
            parts.append("zonder " + ", ".join(clean[:-1]) + " en " + clean[-1])
    if slots.suffix:
        parts.append(" ".join(slots.suffix))
    if slots.size:
        parts.append(" ".join(slots.size))
    return " ".join(p for p in parts if p)


# ---------- AI polish ----------

POLISH_PROMPT_TEMPLATE = (
    "Je krijgt een draft van een Nederlandse SEO-titel die al de juiste woorden in "
    "ongeveer de juiste volgorde heeft. Lever ALLEEN de gepolijste titel terug "
    "(geen uitleg, geen JSON).\n\n"
    "Draft: \"{draft}\"\n\n"
    "Regels:\n"
    "1. Voeg geen woorden toe en verwijder geen woorden behalve dubbele.\n"
    "2. Mag wel woordvolgorde aanpassen voor natuurlijk Nederlands.\n"
    "3. Verbuig bijvoeglijke naamwoorden ('Nieuw'→'Nieuwe', 'Vrijstaand'→'Vrijstaande', "
    "'Klein'→'Kleine') waar grammaticaal nodig.\n"
    "4. Eerste woord met hoofdletter; vervolgens kleine letters behalve eigennamen/merken "
    "(behoud bestaande hoofdletters van merken zoals 'Apple', 'Samsung', 'iPhone').\n"
    "5. Geen aanhalingstekens, geen streepjes, geen punctuatie aan het eind.\n"
    "6. Behoud 'met X, Y en Z' clauses precies zoals ze in de draft staan.\n"
    "7. Behoud maten zoals 'Maat L', '40 cm', '128 GB' aan het einde."
)


def polish_with_ai(draft: str, allowed_words: set, client) -> Optional[str]:
    """Send the draft to OpenAI for grammar polish. Reject hallucinated output."""
    if not draft or not client:
        return None
    prompt = POLISH_PROMPT_TEMPLATE.format(draft=draft)
    try:
        resp = client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=120,
            temperature=0.2,
        )
        polished = resp.choices[0].message.content.strip().strip('"').rstrip('.')
    except Exception as e:
        print(f"[AI_TITLES_V2] polish error: {e}")
        return None

    # Hallucination guard: every alphabetic word in the polished output must be
    # derivable from the draft / allowed_words. Allows Dutch inflection by
    # accepting any polished word that shares its leading 5+ characters with an
    # allowed word and differs only in the trailing 1-3 characters (covers
    # zilvere↔zilveren, inklapbaar↔inklapbare, nieuw↔nieuwe, kind↔kinderen).
    # Also: completeness guard — every multi-char allowed word must still appear
    # somewhere in the polish, otherwise a real value got dropped.
    polished_words = re.findall(r"[A-Za-zÀ-ÿ]+", polished)
    polished_lower = polished.lower()

    def _word_allowed(wl: str) -> bool:
        if wl in allowed_words:
            return True
        if len(wl) < 4:
            return False
        for aw in allowed_words:
            if len(aw) < 4:
                continue
            common = 0
            for a, b in zip(wl, aw):
                if a == b:
                    common += 1
                else:
                    break
            if common >= 5 and abs(len(wl) - len(aw)) <= 3 and common >= max(len(wl), len(aw)) - 3:
                return True
        return False

    for w in polished_words:
        if not _word_allowed(w.lower()):
            return None  # hallucinated word

    return polished


def _allowed_words(slots: TitleSlots) -> set:
    out: set = set()
    for bag in (slots.lead, slots.modifiers, slots.other, slots.audience,
                slots.productname, slots.met_clause, slots.zonder_clause,
                slots.suffix, slots.size):
        for s in bag:
            for w in re.findall(r"[A-Za-zÀ-ÿ]+", s):
                out.add(w.lower())
    if slots.category:
        for w in re.findall(r"[A-Za-zÀ-ÿ]+", slots.category):
            out.add(w.lower())
    # Connectors that the polish step is allowed to use.
    out.update(['met', 'zonder', 'en', 'voor', 'de', 'het', 'een', 'van'])
    return out


# ---------- Capitalization ----------

def capitalize_first(s: str, lead_lowercase_brands: List[str]) -> str:
    if not s:
        return s
    if s[0].islower():
        first_word = s.split()[0] if s else ''
        if first_word not in lead_lowercase_brands:
            s = s[0].upper() + s[1:]
    return s


# ---------- Top-level entry point ----------

def generate_title_v2(url: str) -> Optional[Dict]:
    """v2 of generate_title_from_api. Returns a dict with intermediate values
    so the caller can compare to v1 output. Does NOT write to the database.

    Result keys: api_h1, draft, polished, h1_title, original_h1, slots (dataclass).
    """
    page_data = fetch_products_api(url)
    if not page_data or page_data.get('error'):
        return None
    api_h1 = page_data.get('h1_title', '') or ''
    facets = page_data.get('selected_facets', []) or []
    category_name = page_data.get('category_name', '') or ''
    if not api_h1:
        return None

    type_class = batch_classify_facets(facets, category_name)
    slots = categorize_into_slots(facets, category_name, type_class)

    draft = render_draft(slots)

    client = get_openai_client()
    polished = polish_with_ai(draft, _allowed_words(slots), client) if client else None
    chosen = polished or draft

    chosen = capitalize_first(chosen, lead_lowercase_brands=slots.lead)
    chosen = format_dimensions(chosen)
    chosen = normalize_preposition_case(chosen)
    chosen = normalize_tv_category_caps(chosen)
    chosen = _strip_pre_clause_duplicates(chosen)
    chosen = _dedupe_compound_category(chosen, category_name)

    return {
        "url": url,
        "api_h1": api_h1,
        "draft": draft,
        "polished": polished,
        "h1_title": chosen,
        "original_h1": api_h1,
        "slots": slots,
        "type_class": type_class,
    }
