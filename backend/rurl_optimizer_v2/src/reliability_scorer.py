"""
Reliability Scorer for R-URL Redirects

Calculates a reliability score (0-100) indicating how trustworthy a redirect is.
Higher scores = more reliable, can be used for production redirects.

Score Components (V21 - Met Coverage Penalty):
1. Base Score: match_score × 0.6 (0-60 punten) - match kwaliteit is primair
2. Same Category Bonus: +20 punten - veiliger dan cross-category
3. Exact Match Bonus: +10 punten bij score=100
4. Cross-Category Penalty: -15 punten
5. Category Fallback Penalty: -25 punten
6. Generic Match Penalty: -10 tot -20 punten (Action, Hema, Ikea, etc.)
7. Bad Pattern Penalty: -30 tot -50 punten (bekende foute matches)
8. V21 Coverage Penalty: -35 tot 0 punten afhankelijk van match_coverage

V21: Match coverage wordt nu meegenomen in de reliability score.
- Coverage < 25%: -35 punten (zeer onvolledig, bijv. "hoog" uit "hoog luchtbed")
- Coverage 25-50%: -20 punten
- Coverage 50-75%: -10 punten
- Coverage >= 75%: 0 punten (goede dekking)

Extra: Bij cross-category match EN coverage < 33%: score wordt 0 (blokkeer match)
"""

import re
from typing import Optional

# V38: hard-reject a match where ONLY generic descriptor/form tokens matched
# while a specific product term went unmatched (see _v27_reject_reason). Module
# flag so an A/B can toggle it off to compare against the prior behaviour.
V38_GENERIC_ONLY_REJECT = True

# Generic brand/store names that often cause false matches
GENERIC_FACET_VALUES = {
    'action', 'action.com', 'hema', 'ikea', 'kruidvat', 'kruidvat.nl',
    'blokker', 'gamma', 'praxis', 'karwei', 'bol.com', 'amazon',
    'mediamarkt', 'coolblue', 'wehkamp', 'leen bakker'
}

# Known bad cross-category patterns (keyword contains X, facet is Y)
BAD_CROSS_CATEGORY_PATTERNS = [
    # "zonder boren" matched to "Appelboren" (kitchen tools)
    (r'\bboren\b', r'appelboren', -40),
    # "zwembad" matched to "Zwembadfonteinen" (garden ponds)
    (r'\bzwembad\b', r'zwembadfonteinen', -30),
    # "pomp" matched to "Pompons" (craft supplies)
    (r'\bpomp\b', r'pompons', -40),
    # "fontein" matched to "Vijverfonteinen" when looking for toilet
    (r'(?:\btoilet\b.*fontein|fontein.*toilet|fontein.*wc|wc.*fontein)\b', r'vijverfonteinen', -40),
    # "opblaasbaar" matched to adult products
    (r'\bopblaasba', r'buttplugs|dildo|vibrator', -50),
    # "magnetron" matched to baby sterilizers
    (r'\bmagnetron\b', r'magnetronsterilisators', -30),
    # "auto" matched to "Autopeds" (scooters)
    (r'\bauto\b', r'autopeds', -30),
    # "bank" (furniture) matched to "Bankhoezen"
    (r'\bbank\b.*kopen|kopen.*bank\b', r'bankhoezen', -20),
    # Generic kitchen tools mismatch
    (r'\bkeuken\b', r'keukenpincetten', -30),
]

# Pre-compiled once at import — previously each (keyword, facet) pattern was
# re.search'd from its string form on every scored row.
_BAD_CROSS_COMPILED = [
    (re.compile(kp), re.compile(fp), pen)
    for kp, fp, pen in BAD_CROSS_CATEGORY_PATTERNS
]


def compute_h1_similarity(
    keyword: str,
    original_cat_name: Optional[str],
    redirect_cat_name: Optional[str],
    facet_value_names: Optional[str],
) -> int:
    """
    V26: Synthetic H1 similarity (0-100) used as a trust signal.

    We don't crawl the live pages — instead we build the H1 from the URL
    components we already have:

    - R-URL H1   ≈ keyword (+ original deepest_cat label if any). This mirrors
      what a Beslist search-result page shows for /<maincat>/<subcat>/r/<keyword>.
    - Redirect H1 ≈ redirect deepest_cat label + facet_value_names. Beslist
      facet pages render as "<deepest_cat> <facet value(s)>".

    Compares with token_set_ratio so word order and small fragments don't
    matter. Returns 0 when either side is empty.
    """
    try:
        from fuzzywuzzy import fuzz
    except ImportError:
        return 0

    rurl_parts = [p for p in [keyword, original_cat_name] if p]
    redirect_parts = [p for p in [redirect_cat_name, facet_value_names] if p]
    rurl_h1 = " ".join(rurl_parts).strip()
    redirect_h1 = " ".join(redirect_parts).replace(",", " ").strip()
    if not rurl_h1 or not redirect_h1:
        return 0
    return int(fuzz.token_set_ratio(rurl_h1.lower(), redirect_h1.lower()))


def _v27_reject_reason(
    matched_keywords: Optional[list],
    unmatched_keywords: Optional[list],
    long_token_threshold: int = 8,
    match_type: Optional[str] = None,
) -> Optional[str]:
    """V27: Decide whether a match should be hard-rejected (score → 0).

    Returns a short human-readable reason string, or None when the match
    survives. Centralised so the scorer and the export pipeline agree on
    why a row got dropped.

    Rules:
    1. Generic-only: every matched token is a generic size/color/shape
       adjective (e.g. "Creme" carrying a "tretinoine creme" query).
    1b. (V31) Cross-category match where every matched token is a generic
        adjective OR a generic noun (e.g. "meubel" matching
        "Kapstokmeubels" for keyword "tv-meubel set"). In-subcat matches
        on generic nouns are fine; the bug is only when those weak signals
        justify a jump to a different category.
    2. Long unmatched token: any unmatched non-stopword token of length
       >= long_token_threshold. Long tokens are almost always brands,
       ingredients or product types — losing them in the match means the
       redirect is missing the user's actual intent.
    """
    from src.validation_rules import (GENERIC_ADJECTIVES, GENERIC_NOUNS,
                                       GENERIC_FORM_WORDS)

    matched = [w.lower().strip() for w in (matched_keywords or []) if w and w.strip()]
    unmatched = [w.lower().strip() for w in (unmatched_keywords or []) if w and w.strip()]

    if matched and all(w in GENERIC_ADJECTIVES for w in matched):
        return f"V27: only generic adjective(s) matched: {', '.join(matched)}"

    # V31: cross-category jumps justified by generic words only are
    # almost always wrong — "meubel" alone shouldn't move you from
    # Sfeerhaarden to Kapstokken just because "Kapstokmeubels" exists.
    if (match_type == 'cross_category_type'
            and matched
            and all(w in GENERIC_ADJECTIVES or w in GENERIC_NOUNS for w in matched)):
        return f"V31: cross-category match on generic word(s) only: {', '.join(matched)}"

    # V38: ANY match type — every matched token is a generic descriptor
    # (adjective / generic noun / form word) AND a SPECIFIC product term went
    # unmatched. Matching only "poeder" while "borax" is unmatched routes the
    # query by its physical form, not its identity (→ Allesreinigers 'Poeder').
    # Gated on an unmatched specific token so a legitimately-generic query
    # ("kast" → Kasten, nothing unmatched) is NOT rejected, and on the form/
    # noun being the ONLY thing matched so "losse lamellen" (lamellen is
    # specific → matched) survives. Flag lets the A/B compare old vs new.
    if V38_GENERIC_ONLY_REJECT and matched:
        _generic = GENERIC_ADJECTIVES | GENERIC_NOUNS | GENERIC_FORM_WORDS
        unmatched_specific = [w for w in unmatched
                              if len(w) >= 4 and w.isalpha() and w not in _generic]
        if unmatched_specific and all(w in _generic for w in matched):
            return (f"V38: only generic/form token(s) matched ({', '.join(matched)}); "
                    f"specific term(s) unmatched ({', '.join(unmatched_specific)})")

    long_unmatched = [w for w in unmatched if len(w) >= long_token_threshold]
    if long_unmatched:
        return f"V27: long unmatched token(s): {', '.join(long_unmatched)}"

    return None


def _bridge_stem(w: str) -> str:
    """Normalize a single token to a comparable Dutch stem for bridging.

    Strips one plural suffix, then undoes the two spelling changes Dutch makes
    when pluralizing so the singular and plural forms collapse to the same stem:
      * final-consonant voicing:  dief -> dieven (f->v),  doos -> dozen (s->z),
        huis -> huizen — map a trailing v/z back to f/s.
      * open/closed-syllable double vowel:  doos/dozen, boot/boten — collapse
        'aa','ee','oo','uu' to a single vowel.
    So kruimeldief == kruimeldieven and aftakdoos == aftakdozen after stemming,
    which the old `rstrip('s').rstrip('e')` could not reach (f/v, s/z, oo/o)."""
    for suf in ('eren', 'en', 's'):
        if w.endswith(suf) and len(w) - len(suf) >= 3:
            w = w[:-len(suf)]
            break
    if w.endswith('v'):
        w = w[:-1] + 'f'
    elif w.endswith('z'):
        w = w[:-1] + 's'
    return re.sub(r'([aeou])\1+', r'\1', w)


def _keyword_bridges_value(keyword: Optional[str], value_names: Optional[str]) -> bool:
    """Issue #3: True when at least one content token of the keyword lexically
    overlaps the facet value name(s) (exact, or a >=4-char stem appearing in
    either direction). Deliberately loose — it only gates a hard-floor, so a
    false 'bridge' just leaves the normal score in place. "vogelgeluiden" vs
    "Keuken" has no bridge; "kunststof tuinstoel" vs "Kunststof" does.

    The stem comparison normalizes Dutch plural spelling changes (f/v, s/z,
    double-vowel) so "kruimeldief"~"Kruimeldieven" and "aftakdoos"~"Aftakdozen"
    bridge — without this the head noun fails to match its own category name and
    an otherwise-good facet gets dropped from the redirect."""
    kt = [w for w in re.findall(r'[a-z0-9]+', (keyword or '').lower()) if len(w) >= 3]
    vt = re.findall(r'[a-z0-9]+', (value_names or '').lower())
    if not kt or not vt:
        return False
    for k in kt:
        ks = k.rstrip('s').rstrip('e')          # original loose stem
        kstem = _bridge_stem(k)                 # voicing + double-vowel stem
        for v in vt:
            if k == v:
                return True
            # original raw-stem containment (kept verbatim so nothing that
            # bridged before stops bridging — the stem branch is additive only)
            if len(ks) >= 4 and (ks in v or (len(v) >= 4 and v.rstrip('s').rstrip('e') in k)):
                return True
            # additive: Dutch plural voicing / double-vowel (dief~dieven,
            # doos~dozen). Guarded on ORIGINAL token length so an aggressive
            # stem can't drop a match below the length floor.
            if len(k) >= 4 and len(v) >= 4:
                vstem = _bridge_stem(v)
                # V62: the >= 4 floor has to hold for the STEMS as well, not just
                # the tokens it started from. _bridge_stem('boren') is 'bor', and
                # 'bor' sits inside 'bordeauxrod' - which is how the leftover
                # tokens "zonder boren" got kleurtint 'Bordeauxrood' appended to
                # a Gordijnroedes redirect. Three letters is not a bridge.
                if (len(kstem) >= 4 and len(vstem) >= 4
                        and (kstem in vstem or vstem in kstem)):
                    return True
    return False


def _value_equals_query(keyword: Optional[str], value_names: Optional[str]) -> bool:
    """RC5: True when the facet value name essentially IS the query — every
    query content token appears in the value name (modulo a trailing plural -s),
    and the value adds at most one extra token. Distinguishes a true "value ==
    query" match ("watertafel" vs "Watertafels", "toiletmeubel" vs
    "Toiletmeubels") from a fragment match of a long query ("…13 polig naar 7
    polig" vs "7-polige stekkers") or a head-noun-dropped match
    ("kunststof-hoekprofielen" vs "Kunststof", where 'hoekprofielen' is missing).
    """
    def _norm(s):
        toks = re.findall(r'[a-z0-9]+', (s or '').lower())
        return {t[:-1] if (t.endswith('s') and len(t) > 3) else t for t in toks}
    kw = _norm(keyword)
    vn = _norm(value_names)
    if not kw or not vn:
        return False
    # every query token must be represented in the value (value may be a superset
    # by at most one descriptor token, e.g. "Watertafels" vs "watertafel").
    return kw <= vn and (len(vn) - len(kw)) <= 1


def calculate_reliability_score(
    match_score: int,
    facet_count: int,
    match_type: str,
    is_cross_category: bool,
    facet_value_names: Optional[str],
    keyword: str,
    reason: str,
    match_coverage: float = 100.0,  # V21: match_coverage als percentage (0-100)
    h1_similarity: Optional[int] = None,  # V26: synthetic H1 similarity (0-100)
    matched_keywords: Optional[list] = None,  # V27: tokens that actually matched
    unmatched_keywords: Optional[list] = None,  # V27: tokens that did NOT match
) -> int:
    """
    Calculate reliability score for a redirect.

    Args:
        match_score: Score van de match (0-100)
        facet_count: Aantal gematchte facetten
        match_type: Type match (exact, fuzzy, subcategory_name, etc.)
        is_cross_category: Of de redirect naar een andere categorie gaat
        facet_value_names: Namen van de gematchte facetwaarden
        keyword: Het originele keyword uit de R-URL
        reason: Reden/beschrijving van de match
        match_coverage: V21 - Percentage van keyword dat gematcht is (0-100)

    Returns:
        int: Score from 0-100 where:
            - 90-100: Very reliable, safe for production
            - 75-89: Reliable, likely correct
            - 50-74: Moderate, needs review
            - 0-49: Unreliable, should not be used
    """
    # ==========================================================================
    # V14.1: SUBCATEGORIE NAAM MATCHING SCORING
    # ==========================================================================
    # Subcategorie naam matches krijgen speciale scoring:
    # - Exact match (score 100): Tier A (90+) - perfecte redirect
    # - Hoge score (95-99): Tier B (75-89) - zeer betrouwbaar
    # - Lagere score (80-94): Tier C (50-74) - matig betrouwbaar
    #
    # Voorbeeld:
    # - "deurscharnieren" -> "Deurscharnieren" (score 100) -> Tier A
    # - "scharnieren" -> "Deurscharnieren" (score ~95) -> Tier B
    # ==========================================================================

    if match_type == 'subcategory_name':
        # V32: subcategory-name matches now get the SAME unmatched-token
        # treatment as facet matches. A long unmatched qualifier (e.g.
        # "waterdicht" in "tuinkasten waterdicht" -> bare "Tuinkasten") or a
        # generic-only match hard-rejects to 0, exactly as it would on the
        # facet path below. Previously this branch returned early and skipped
        # the rule entirely, so a subcategory redirect could silently drop a
        # long product qualifier and still score 95. Requires the caller to
        # compute real matched/unmatched_keywords for subcategory matches
        # (see main_parallel_v2: subcategory_name removed from TRUSTED).
        if _v27_reject_reason(matched_keywords, unmatched_keywords, match_type=match_type) is not None:
            return 0

        # Subcategorie naam match - speciale scoring
        if match_score == 100:
            # Exact match met subcategorie naam = Tier A
            base = 95
        elif match_score >= 95:
            # Hoge score (keyword is deel van subcategorie naam) = Tier B
            # Score 95 -> 80, Score 99 -> 88
            base = 75 + ((match_score - 95) * 2.5)
        else:
            # Lagere score = Tier C
            # Score 80 -> 55, Score 94 -> 69
            base = 50 + ((match_score - 80) * 1.4)

        # V24: Coverage penalty toepassen
        # Bij lage coverage is de match minder betrouwbaar
        # Voorbeeld: "ketoconazol shampoo" -> "Shampoo" = 50% coverage
        if match_coverage < 50.0:
            base -= 30  # Minder dan helft gematcht = grote penalty
        elif match_coverage < 75.0:
            base -= 15  # Matige coverage
        elif match_coverage < 100.0:
            base -= 5   # Kleine penalty voor niet-complete match

        return max(0, min(100, int(base)))

    # ==========================================================================
    # V15: VEREENVOUDIGDE FACET MATCHING SCORING
    # ==========================================================================
    # Match kwaliteit is primair - facet count is NIET relevant
    # Één goede match is net zo betrouwbaar als meerdere goede matches
    # ==========================================================================

    # Base score: match_score × 0.6 (0-60 punten)
    # match_score 100 -> 60 punten
    # match_score 90  -> 54 punten
    # match_score 80  -> 48 punten
    base_score = (match_score / 100) * 60

    # === POSITIVE FACTORS ===

    # Same category bonus (+20 punten)
    # Blijven in dezelfde categorie is veiliger
    if not is_cross_category:
        base_score += 20

    # Exact match bonus (+10 punten bij score=100)
    # Perfecte match = extra vertrouwen
    if match_score == 100:
        base_score += 10

    # === NEGATIVE FACTORS ===

    # Category fallback penalty (-25 punten)
    # Geen facet match = minder betrouwbaar
    if match_type == 'category_fallback':
        base_score -= 25

    # Cross-category type penalty (-15 punten)
    # Matches in andere categorieën zijn riskanter
    if match_type == 'cross_category_type':
        base_score -= 15

    # Generic facet value penalty (-10 tot -20 punten)
    # Generieke winkel/merknamen geven vaak foute matches
    if facet_value_names:
        facet_lower = facet_value_names.lower()
        for generic in GENERIC_FACET_VALUES:
            if generic in facet_lower:
                base_score -= 15  # Standaard penalty voor generic facet
                break

    # Cross-category + generic = extra onbetrouwbaar (-10 extra)
    if is_cross_category and facet_value_names:
        facet_lower = facet_value_names.lower()
        for generic in GENERIC_FACET_VALUES:
            if generic in facet_lower:
                base_score -= 10  # Additional penalty
                break

    # Bad pattern detection (bekende foute matches)
    keyword_lower = keyword.lower() if keyword else ''
    facet_lower = (facet_value_names or '').lower()

    # Apply only the single WORST matching bad-pattern penalty (was summing all
    # matching penalties, which double-docked rows that tripped two patterns).
    _worst_penalty = 0
    for kw_re, fv_re, penalty in _BAD_CROSS_COMPILED:
        if kw_re.search(keyword_lower) and fv_re.search(facet_lower):
            _worst_penalty = min(_worst_penalty, penalty)
    base_score += _worst_penalty  # penalty is negative

    # Maincat/parent_subcat fallback with cross-category = less reliable
    if is_cross_category and '[maincat]' in reason:
        base_score -= 10
    if is_cross_category and '[parent_subcat]' in reason:
        base_score -= 5

    # ==========================================================================
    # V21: MATCH COVERAGE PENALTY
    # ==========================================================================
    # Lage coverage = onbetrouwbare match
    # Voorbeeld: "hoog" uit "extra hoog luchtbed voor 2 personen" = 25% coverage
    # Dit soort matches zijn vaak fout en moeten bestraft worden.
    #
    # Extra streng voor cross-category: coverage < 33% = blokkeer volledig
    # ==========================================================================

    # V21: Cross-category + very low coverage = block completely
    if is_cross_category and match_coverage < 33.0:
        return 0  # Blokkeer deze match volledig

    # V21: Coverage penalty
    if match_coverage < 25.0:
        base_score -= 35  # Zeer onvolledig
    elif match_coverage < 50.0:
        base_score -= 20  # Matig
    elif match_coverage < 75.0:
        base_score -= 10  # Redelijk

    # V26 / V27: H1 similarity tiers (tightened in V27).
    # Synthetic R-URL H1 vs redirect H1 — see compute_h1_similarity. The
    # earlier V26 thresholds (≥85 / <60 / <40) let mediocre matches through;
    # V27 tightens the band so genuine semantic mismatches (e.g. "Mini" vs
    # "mini gps tracker", token_set_ratio ≈ 50) drop into a real penalty.
    if h1_similarity is not None:
        if h1_similarity >= 85:
            base_score += 5
        elif h1_similarity >= 70:
            base_score += 0  # neutral — clearly related but not a near-twin
        elif h1_similarity >= 50:
            base_score -= 10  # was -5 in V26
        else:
            base_score -= 20  # was -10 in V26

    # Issue #3: a facet_probe_fallback promotes a facet purely on result-set
    # coverage (match_score == coverage%). When NONE of the keyword's content
    # tokens lexically bridges the promoted facet value, the facet doesn't
    # represent the query — e.g. "vogelgeluiden" → ruimte_woonaccessoires
    # "Keuken" (the destination subcat is vogel-related, but the FACET is not).
    # Hard-floor to 0, the same outcome as a generic-only lexical match (borax →
    # 'Poeder'). Synonym-bridged probes ride a different match_type
    # (search_derived_subcat_with_probe_facet), so they are untouched.
    if match_type == 'facet_probe_fallback' and not _keyword_bridges_value(keyword, facet_value_names):
        return 0

    # V27: Hard-rejection rules — generic-only matches and long unmatched
    # tokens. Reasons are computed by _v27_reject_reason so the export
    # pipeline can surface them in the same wording.
    if _v27_reject_reason(matched_keywords, unmatched_keywords, match_type=match_type) is not None:
        return 0

    # V28: When the URL builder dropped a duplicate facet name (Beslist
    # URLs only allow one value per facet name), the resulting redirect is
    # essentially "back to the original URL minus the keyword" — devalue
    # heavily so it lands in tier D and reviewers spot it.
    if match_type == 'duplicate_facet_dropped':
        base_score = min(base_score, 25)

    # RC5 (2026-06-19): lift a genuinely perfect single-facet match. When the
    # facet value name IS the query (modulo plural — "watertafel"→"Watertafels")
    # or the query is a single-token synonym ("transparant"→"Doorzichtig"), and
    # the whole query is covered with nothing left over, the match is as good as
    # an exact one — it shouldn't sit at tier C (67) just because the fuzzy/
    # synonym/plural path capped match_score at 95 or a [maincat] rescue docked
    # it. A plural-equal value match is treated like an exact match (A); a
    # synonym match lands at B. The value≡query test fails for long gamed-coverage
    # queries ("verloop stekker 13 polig…" vs "7-polige stekkers") and for
    # head-noun-dropped matches ("kunststof-hoekprofielen" vs "Kunststof"), so
    # those are NOT lifted.
    if (facet_count <= 1 and match_coverage >= 100.0 and not unmatched_keywords):
        _val_eq = _value_equals_query(keyword, facet_value_names)
        if _val_eq:
            base_score = max(base_score, 90)
        elif match_type == 'synonym':
            base_score = max(base_score, 80)

    # Clamp to 0-100
    return max(0, min(100, int(base_score)))


# ==========================================================================
# V45: SEARCH-DERIVED CONFIDENCE SCORING (2026-06-30)
# ==========================================================================
# The search-derived branches in main_parallel_v2 historically each shipped a
# FLAT constant (samecat=65, faceted=70, subcat-rescue=75, fallback=65/45),
# blind to the two signals that actually carry confidence:
#   1. how much of the query is covered by the redirect (match_coverage)
#   2. how dominant the chosen category is in product count (dom_cat_share),
#      qualified by how many products that dominance rests on (dom_cat_count) —
#      a 100% share over 80 products is noise, not signal.
# score_search_derived() adjusts the per-branch base by those signals. Per the
# 2026-06-30 product decision the count guard is PENALTY-ONLY (it never lifts a
# thin-evidence row) and nothing here auto-suppresses a redirect — a weak row
# just sinks toward tier D for the reviewer to catch.
# Bands are module constants so the regression harness can sweep them.

# Coverage adjustment (two-sided). match_coverage is 0-100. Coverage is the
# PRIMARY confidence driver: a half-covered query (head noun matched, qualifier
# dropped — e.g. "aftakdoos waterdicht" → all Aftakdozen) must not reach tier B
# on dominance alone, so the low-coverage penalties outweigh the dominance bonus.
COVERAGE_BANDS = (
    (90.0, +8),    # query almost fully represented
    (75.0, +3),
    (60.0, 0),     # neutral
    (40.0, -8),    # ~half the query dropped
    (0.0, -18),    # almost nothing of the query is covered
)
# Dominance adjustment (two-sided). dom_share is 0-1. Secondary to coverage:
# the bonus is deliberately small so it can't rescue a poorly-covered query.
DOMINANCE_BANDS = (
    (0.85, +6),
    (0.65, +2),
    (0.45, 0),
    (0.30, -8),
    (0.0, -15),
)
# Absolute-count guard (PENALTY ONLY). dom_cat_count = products under the chosen
# category in the AND-match set. A high share over a tiny set is noise
# (motorhelm → Videocamera's, share 1.0 over ~116 products), so thin sets are
# penalised regardless of share.
COUNT_PENALTY_BANDS = (
    (1000, 0),     # enough products to trust the share
    (500, -3),
    (200, -8),
    (100, -12),
    (0, -15),      # a handful of products — share is meaningless
)
# V56 (2026-08-18, Joeps besluit): RETIRED — a faceted destination is no longer
# docked for holding few products. The bands stayed in place for two versions on
# the reasoning that a near-empty page is a churn/quality risk, but that judges
# the CATALOGUE, not the redirect: a query whose intent the page names exactly is
# answered correctly even when the taxonomy has yet to fill it, and filling it is
# another team's work. The old note conceded the point itself — "count alone can't
# tell a legit thin brand page (ici paris, 4) from a churny one" — so the bands
# were penalising both to flag one.
#
# Kept as an empty table rather than deleted so the retirement is legible next to
# COUNT_PENALTY_BANDS, which is NOT the same judgement and stays: there the count
# measures how much a dom_share can be TRUSTED (share 1.0 over 116 products is
# noise that routes motorhelm -> bare Videocamera's), not how full a page is.
FACETED_COUNT_PENALTY_BANDS = ()
# Below this AND-match count a high dom_share is not trustworthy enough to earn
# its dominance bonus (the bonus is zeroed; the count penalty still applies).
DOMINANCE_MIN_COUNT = 300


def _band(value, bands, default=0):
    """Return the adjustment for the first (threshold, adj) pair whose
    threshold `value` meets (bands are ordered high→low)."""
    if value is None:
        return default
    for threshold, adj in bands:
        if value >= threshold:
            return adj
    return bands[-1][1]


def score_search_derived(
    base: int,
    match_coverage: Optional[float],
    dom_share: Optional[float],
    dom_count: Optional[int],
    match_type: Optional[str] = None,
    include_coverage: bool = True,
    target_is_faceted: bool = False,
) -> int:
    """V45: adjust a score by query coverage and category product-count
    dominance. See the band tables above.

    Args:
        base: the branch's historical flat score (e.g. 65/70/75), or the score
            already produced by calculate_reliability_score.
        match_coverage: % of (non-stopword) query tokens represented (0-100).
        dom_share: fraction of AND-match products in the chosen category (0-1).
        dom_count: absolute product count behind dom_share (for the count guard).
        match_type: reserved for per-branch tuning; currently informational.
        include_coverage: apply the coverage band. Set False for paths whose
            base already folded coverage in (the subcategory_name / facet paths
            of calculate_reliability_score) so coverage isn't double-counted —
            only the new dominance + count signals are added there.
        target_is_faceted: the redirect carries a /c/ facet. The count guard
            (small product set = unreliable dominance) is meant for BARE-category
            redirects, where a thin dominant cat is noise (motorhelm → bare
            Videocamera's, 116 products). A faceted page is INTENTIONALLY narrow
            (ici paris → merk~Ici Paris, 4 products is normal), so the count
            penalty AND the count-based bonus suppression are skipped for it —
            since V56 the faceted count penalty is skipped unconditionally.

    Returns:
        int 0-100. Conservative: the count band only ever subtracts, and this
        function never returns a hard 0 on its own (no auto-suppression) — it
        clamps to [0,100] but weak rows simply fall to tier D.
    """
    score = float(base)
    if include_coverage:
        cov_adj = _band(match_coverage, COVERAGE_BANDS)
        # A BARE-category redirect can't filter a dropped query token, so a
        # partially-covered query is a poorer fit than the same coverage on a
        # faceted page (whose /c/ value captures the extra token). Deepen the
        # penalty for bare targets so a query with a real qualifier dropped falls
        # to tier D. "aftakdoos waterdicht" -> bare Aftakdozen (cov 50%, the
        # 'waterdicht' filter lost) should not sit in tier C on dominance alone.
        if cov_adj < 0 and not target_is_faceted:
            cov_adj -= 8
        score += cov_adj
    # Dominance only earns its full weight when it rests on enough products: a
    # high share over a tiny set is the motorhelm→Videocamera's (116 products,
    # share 1.0) failure mode, so we cap the dominance BONUS when the count is
    # thin while still letting a low share penalise. Skipped for faceted targets
    # (their small counts are by design, not thin evidence).
    dom_adj = _band(dom_share, DOMINANCE_BANDS)
    if (dom_adj > 0 and not target_is_faceted
            and dom_count is not None and dom_count < DOMINANCE_MIN_COUNT):
        dom_adj = 0  # don't reward dominance we can't trust
    # Dominance must not rescue a poorly-covered BARE category: a query missing a
    # real token sent to the whole (dominant) category is a weak fit regardless
    # of how dominant that category is. Mirrors the band comment's stated intent.
    if (dom_adj > 0 and not target_is_faceted
            and match_coverage is not None and match_coverage < 60):
        dom_adj = 0
    score += dom_adj
    # Count penalty for BARE-category redirects only: a dom_share resting on a
    # thin set is noise, so the count limits how far dominance may carry the
    # score. A faceted target is exempt (V56) — there the count would be judging
    # how full the destination is, which is a taxonomy matter and not this
    # score's business.
    if not target_is_faceted:
        score += _band(dom_count, COUNT_PENALTY_BANDS)
    return max(0, min(100, int(round(score))))


def get_reliability_tier(score: int) -> str:
    """
    Get reliability tier label.

    Returns:
        str: 'A' (very reliable), 'B' (reliable), 'C' (moderate), 'D' (unreliable)
    """
    if score >= 90:
        return 'A'  # Very reliable - safe for production
    elif score >= 75:
        return 'B'  # Reliable - likely correct
    elif score >= 50:
        return 'C'  # Moderate - needs review
    else:
        return 'D'  # Unreliable - should not be used


def get_reliability_description(tier: str) -> str:
    """Get human-readable description of reliability tier."""
    descriptions = {
        'A': 'Zeer betrouwbaar - veilig voor productie',
        'B': 'Betrouwbaar - waarschijnlijk correct',
        'C': 'Matig - review nodig',
        'D': 'Onbetrouwbaar - niet gebruiken'
    }
    return descriptions.get(tier, 'Onbekend')


# ==========================================================================
# V55: SYMMETRIC H1 OVERLAP
# ==========================================================================
# compute_h1_similarity (V26) uses fuzz.token_set_ratio, which returns 100
# whenever one side's token set is a SUBSET of the other's. That makes it blind
# to exactly the distinction an H1 comparison is wanted for. For the query
# "ketoconazol shampoo":
#     redirect H1 "Shampoo"             -> token_set_ratio 100
#     redirect H1 "Shampoo Ketoconazol" -> token_set_ratio 100
# The first dropped the qualifier the searcher typed, the second kept it, and
# the metric cannot tell them apart. Every bare-category redirect whose category
# name appears in the query scores a perfect 100, so the signal says nothing
# there — and those are precisely the rows an H1 check should judge.
#
# compute_h1_overlap is symmetric. It penalises BOTH a query token the redirect
# H1 doesn't represent (the dropped 'ketoconazol') AND a redirect-H1 token the
# query never asked for (a facet value or an alien category noun bolted on top
# of the intent). The score is the F1 of the two per-side coverage rates, so 100
# means the two H1s name the same things and nothing else.
# ==========================================================================

# Commercial/quality tail that carries no product intent. Dropped from BOTH
# sides so it can neither earn nor cost overlap. Mirrors _COV_FILLER in
# main_parallel_v2's V45 coverage recompute, plus the query-side buy words.
_H1_FILLER = {
    'kopen', 'koop', 'goedkoop', 'goedkope', 'goedkoopste', 'beste',
    'aanbieding', 'aanbiedingen', 'sale', 'outlet', 'online', 'prijs', 'prijzen',
    'review', 'reviews', 'mooi', 'mooie', 'mooiste', 'leuk', 'leuke', 'handig',
    'handige', 'simpel', 'simpele', 'praktisch', 'praktische',
    # Dutch function words. Kept local rather than importing STOPWORDS: that set
    # is tuned for match rejection and also holds size/colour words we DO want
    # to weigh here ("zwart" in the query vs "Zwart" on the target is signal).
    'de', 'het', 'een', 'en', 'of', 'met', 'zonder', 'voor', 'van', 'in', 'op',
    'aan', 'bij', 'te', 'tot', 'per', 'als', 'om', 'die', 'dat', 'is', 'zijn',
}

_H1_TOKEN_RE = re.compile(r'[a-z0-9]+')


def _h1_str(v) -> str:
    """Coerce a field to text. Category/value names arrive as '' from the
    optimizer but as a float NaN when a row is read back from the output csv.
    """
    if v is None or isinstance(v, float):
        return ''
    return v if isinstance(v, str) else str(v)


def _h1_fold(text) -> str:
    """Strip diacritics so 'geisoleerd' matches 'Geïsoleerd'. Mirrors
    facet_probe._fold. Applied to the whole string BEFORE tokenising — folding
    per token would be too late, since the [a-z0-9]+ tokeniser splits
    'geïsoleerd' into 'ge' + 'soleerd' at the accented letter.
    """
    import unicodedata as _ud
    return "".join(c for c in _ud.normalize("NFKD", _h1_str(text).lower())
                   if not _ud.combining(c))


def _h1_norm(tok: str) -> str:
    """Strip a Dutch plural/-e suffix, then collapse doubled vowels.

    Mirrors facet_probe._stem + KeywordMatcher._collapse_double_vowels, so
    'panelen'/'paneel' and 'grote'/'groot' land on the same key. Kept local
    instead of importing those modules — this scorer is imported by the export
    pipeline too and shouldn't drag pandas/requests along.
    """
    if len(tok) > 3 and tok.endswith('s'):
        tok = tok[:-1]
    if len(tok) > 3 and tok.endswith('e'):
        tok = tok[:-1]
    return re.sub(r'([aeou])\1+', r'\1', tok)


def _h1_tokens(text: str) -> list:
    """Normalised, de-duplicated, filler-free tokens of one H1."""
    out = []
    for raw in _H1_TOKEN_RE.findall(_h1_fold(text)):
        if raw in _H1_FILLER:
            continue
        tok = _h1_norm(raw)
        if not tok or tok in _H1_FILLER or tok in out:
            continue
        out.append(tok)
    return out


def _h1_bridges(a: str, b: str) -> bool:
    """Two normalised tokens name the same thing.

    Equal, or one contains the other with both >= 4 chars ('mand' in 'kastmand',
    'shampoo' in 'antiroosshampoo'). The length floor keeps short fragments
    ('tv', 'led') from bridging into unrelated longer words — the same rule the
    V45 coverage recompute uses.
    """
    if a == b:
        return True
    return len(a) >= 4 and len(b) >= 4 and (a in b or b in a)


def h1_overlap_parts(
    keyword: str,
    redirect_cat_name: Optional[str],
    facet_value_names: Optional[str],
) -> tuple:
    """V55: (overlap, query_coverage, target_coverage), each 0-100.

    R-URL H1    ≈ the keyword (what the searcher typed).
    Redirect H1 ≈ "<deepest category> <facet value(s)>" — how Beslist renders a
    /c/ facet page, and the same construction the xlsx `h1` column shows.

    The ORIGINAL category is deliberately not folded into the query side (unlike
    compute_h1_similarity): a maincat label the searcher never typed
    ('Drogisterij') is not part of the intent and only dilutes the denominator.

    The redirect category name IS counted against target coverage. It damps the
    overlap when the destination's subject noun is alien to the query, which is
    the "right facet values, wrong category" failure ("rubberen tegels" ->
    Zwembadgrondzeilen /c/materiaal~Rubber~~t_zwembadgrondzeil~Tegels: both
    values echo the query, the category does not).

    overlap is the F1 of the two coverages — symmetric, and 100 only when both
    sides name the same things. All three are 0 when either side is empty.

    Known limitation: the >= 4-char containment bridge cannot tell a Dutch
    hyponym compound from an inflection. 'tafel' is inside 'voetbaltafel' just
    as 'kast' is inside 'opbergkast', so "inklapbare tafel" -> Voetbaltafels
    scores 100 (wrong) with the same rule that earns "kasten 30 cm diep" ->
    Opbergkasten its 100 (right). That is semantics, not tokens — the signal
    that knows the difference is search-derived dominance. Both cases are pinned
    in tests/test_v55_h1_overlap.py; the lift stays small and capped precisely
    because of this.
    """
    q = _h1_tokens(keyword)
    t = _h1_tokens(_h1_str(redirect_cat_name) + ' '
                   + _h1_str(facet_value_names).replace(',', ' '))
    if not q or not t:
        return 0, 0, 0
    recall = sum(1 for a in q if any(_h1_bridges(a, b) for b in t)) / len(q)
    precision = sum(1 for b in t if any(_h1_bridges(a, b) for a in q)) / len(t)
    if recall + precision == 0:
        return 0, 0, 0
    f1 = 100 * 2 * recall * precision / (recall + precision)
    return int(round(f1)), int(round(100 * recall)), int(round(100 * precision))


def compute_h1_overlap(
    keyword: str,
    redirect_cat_name: Optional[str],
    facet_value_names: Optional[str],
) -> int:
    """V55: symmetric H1 overlap (0-100). See h1_overlap_parts."""
    return h1_overlap_parts(keyword, redirect_cat_name, facet_value_names)[0]


# V55 lift thresholds. ONE band, set high: with 1-3 token queries the F1 is
# heavily quantised (100 / 80 / 67 / 40 / 0), so a floor of 90 means "the two
# H1s are twins" and nothing softer earns anything. A second band at 75 was
# tried and dropped — it lifted "rubberen tegels" -> Zwembadgrondzeilen
# (overlap 80: both facet values echo the query, the category doesn't), which is
# exactly the row an H1 check must NOT vouch for.
H1_OVERLAP_LIFT_FLOOR = 90
H1_OVERLAP_RECALL_FLOOR = 90   # the query must be near-fully represented
H1_OVERLAP_LIFT = 10
H1_OVERLAP_LIFT_CEILING = 89   # H1 alone never manufactures a Tier A
# V56 (2026-08-18, Joeps besluit): how many products sit behind the destination
# no longer bears on the redirect's score. A sparse facet page is a taxonomy
# problem for another team; it does not make the redirect the wrong answer for
# the query. "ketoconazol shampoo" -> Shampoo /c/ingr_shamp~23982436 is the right
# destination whether that page holds one product or six hundred. The earlier
# H1_OVERLAP_MIN_DEST_COUNT gate (and the dest_count argument it read) is gone.


def apply_h1_overlap_lift(score: int, overlap: Optional[int],
                          query_coverage: Optional[int]) -> int:
    """V55: raise a score whose redirect H1 genuinely echoes the R-URL H1.

    A near-identical H1 pair means the destination page names the same product
    the searcher named — the strongest end-to-end evidence a redirect is right,
    because it judges the ANSWER rather than the branch that found it. That
    matters here: the cascade reaches one target down several routes, and the
    flat per-branch constants (RC4's 70, samecat's 65, subcat-rescue's 75, ...)
    disagree about the same URL. Applied once, after the cascade settles, so the
    route stops deciding the score on its own.

    Deliberately conservative:
      * lift-only — never subtracts, so it cannot suppress a redirect that
        scores fine today;
      * gated on query_coverage as well as overlap, so a redirect that dropped
        a query token can't buy the bonus with a tidy target side;
      * capped at H1_OVERLAP_LIFT_CEILING, so H1 alone can't promote a row into
        the Tier A production set;
      * a score of 0 (hard-rejected by V27/V38/V39 or a probe guard) stays 0.

    It is deliberately blind to how many products the destination holds — see
    the V56 note above H1_OVERLAP_LIFT_FLOOR.
    """
    if not score or overlap is None:
        return score
    if overlap < H1_OVERLAP_LIFT_FLOOR:
        return score
    if (query_coverage or 0) < H1_OVERLAP_RECALL_FLOOR:
        return score
    return max(score, min(score + H1_OVERLAP_LIFT, H1_OVERLAP_LIFT_CEILING))
