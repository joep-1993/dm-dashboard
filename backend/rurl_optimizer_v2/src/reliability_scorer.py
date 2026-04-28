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
    (r'\btoilet\b.*fontein|fontein.*toilet|fontein.*wc|wc.*fontein\b', r'vijverfonteinen', -40),
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


def calculate_reliability_score(
    match_score: int,
    facet_count: int,
    match_type: str,
    is_cross_category: bool,
    facet_value_names: Optional[str],
    keyword: str,
    reason: str,
    match_coverage: float = 100.0  # V21: match_coverage als percentage (0-100)
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

    for keyword_pattern, facet_pattern, penalty in BAD_CROSS_CATEGORY_PATTERNS:
        if re.search(keyword_pattern, keyword_lower) and re.search(facet_pattern, facet_lower):
            base_score += penalty  # penalty is negative

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

    # Clamp to 0-100
    return max(0, min(100, int(base_score)))


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
