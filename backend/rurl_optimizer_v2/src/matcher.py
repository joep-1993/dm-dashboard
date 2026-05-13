"""
Keyword matching module.
Performs exact and fuzzy matching of keywords against facet values.

All validation rules are imported from validation_rules.py.
See that file for detailed documentation of each rule and its version history.
"""

from dataclasses import dataclass
from typing import Optional
from fuzzywuzzy import fuzz, process
import sys
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import config
from src.facet_filter import FacetValue
from src.synonyms import expand_keyword, get_synonyms

# Import all validation rules from central location
from src.validation_rules import (
    STRICT_FACETS,
    STRICT_FACET_THRESHOLD,
    STRICT_FACET_EXACT_THRESHOLD,
    MIN_KEYWORD_LENGTH_FOR_FUZZY,
    MIN_FACET_LENGTH_FOR_FUZZY,
    MIN_LENGTH_RATIO,
    PRIORITY_FACET_PREFIXES,
    CROSS_CATEGORY_MIN_SCORE,
    SAME_CATEGORY_MIN_SCORE,
    PRODUCT_TYPE_FACETS,
    STOPWORDS,
    SHOP_NAMES,
    DUTCH_SUFFIXES,
)

# V30: Dutch prepositions that indicate keyword is a qualifier, not the primary product
# "Dildo's met zuignap" → "met" before "zuignap" means zuignap is a feature, not the product
PREPOSITION_QUALIFIERS = {'met', 'voor', 'van', 'zonder', 'op', 'bij', 'als', 'om', 'uit', 'aan'}


def extract_category_path_from_url(facet_url: str) -> Optional[str]:
    """
    v5: Extract the category path from a facet URL.

    Example:
        Input: "/products/huis_tuin/huis_tuin_505313_505230/c/type_opberger~23807768"
        Output: "/products/huis_tuin/huis_tuin_505313_505230"
    """
    if not facet_url:
        return None
    # Remove /c/... part
    if '/c/' in facet_url:
        path = facet_url.split('/c/')[0]
        return path
    return None


@dataclass
class MatchResult:
    """Result of a keyword match attempt."""
    keyword: str
    facet_value: Optional[FacetValue]
    match_type: str  # 'exact', 'fuzzy', 'none'
    score: int       # 0-100 confidence score
    matched_text: str  # The facet value name that was matched
    # v5: For cross-category matches, store the target category path from facet URL
    cross_category_path: Optional[str] = None

    @property
    def is_match(self) -> bool:
        return self.match_type != 'none' and self.score >= config.FUZZY_THRESHOLD

    @property
    def is_cross_category(self) -> bool:
        """Check if this match requires redirect to a different category."""
        return self.cross_category_path is not None

    @property
    def is_winkel_facet(self) -> bool:
        """Check if this match is a winkel/shop facet."""
        if self.facet_value:
            return self.facet_value.facet_name.lower() == 'winkel'
        return False

    @property
    def is_strict_facet(self) -> bool:
        """Check if this match is a strict facet (winkel or merk)."""
        if self.facet_value:
            return self.facet_value.facet_name.lower() in STRICT_FACETS
        return False

    @property
    def is_priority_facet(self) -> bool:
        """Check if this is a priority facet (type_, kleur, etc.)."""
        if self.facet_value:
            facet_name = self.facet_value.facet_name.lower()
            return facet_name.startswith(PRIORITY_FACET_PREFIXES)
        return False


class KeywordMatcher:
    """Matches keywords against facet values."""

    def __init__(self, fuzzy_threshold: int = None, strict_winkel: bool = True,
                 use_token_coverage: bool = True):
        """
        Initialize the matcher.

        Args:
            fuzzy_threshold: Minimum score for fuzzy match (0-100).
                           Defaults to config.FUZZY_THRESHOLD
            strict_winkel: If True, apply stricter matching for winkel facets
            use_token_coverage: V29 — when True (default), multi-word keywords
                are scored facet-value-centric (count how many keyword tokens
                appear in each facet value, normalised by facet length, with
                a positional adjacency bonus) instead of the per-token
                cascade. Removes the count-tiebreak crutch that made
                "vaste telefoons kpn senioren" land on "Draadloze telefoon"
                simply because it had the most products. Pass False to
                fall back to the legacy path.
        """
        self.fuzzy_threshold = fuzzy_threshold or config.FUZZY_THRESHOLD
        self.strict_winkel = strict_winkel
        self.use_token_coverage = use_token_coverage

    def _get_threshold_for_facet(self, facet_name: str) -> int:
        """Get the matching threshold for a specific facet type."""
        if self.strict_winkel and facet_name.lower() in STRICT_FACETS:
            return STRICT_FACET_THRESHOLD
        return self.fuzzy_threshold

    def _is_valid_fuzzy_match(self, keyword: str, facet_value: str) -> bool:
        """
        v5: Validate if fuzzy match makes sense based on string lengths.
        Prevents false matches like "12v" -> "E".
        """
        kw_len = len(keyword)
        fv_len = len(facet_value)

        # Check minimum lengths
        if kw_len < MIN_KEYWORD_LENGTH_FOR_FUZZY:
            return False
        if fv_len < MIN_FACET_LENGTH_FOR_FUZZY:
            return False

        # Check length ratio - strings should be somewhat similar in length
        if kw_len > 0 and fv_len > 0:
            ratio = min(kw_len, fv_len) / max(kw_len, fv_len)
            if ratio < MIN_LENGTH_RATIO:
                return False

        return True

    def match(self, keyword: str, facet_values: list[FacetValue], exclude_winkel: bool = False) -> MatchResult:
        """
        Find the best matching facet value for a keyword.

        Strategy:
        1. Try exact match (case-insensitive)
        2. Try synonym match
        3. Try fuzzy match with threshold
        4. Return best match or no match

        Args:
            keyword: The search keyword to match
            facet_values: List of FacetValue objects to match against
            exclude_winkel: If True, exclude winkel facets from matching

        Returns:
            MatchResult with best match or no match
        """
        if not facet_values:
            return MatchResult(
                keyword=keyword,
                facet_value=None,
                match_type='none',
                score=0,
                matched_text=''
            )

        keyword_normalized = self._normalize(keyword)

        # Filter out winkel facets if requested
        if exclude_winkel:
            facet_values = [fv for fv in facet_values if fv.facet_name.lower() not in STRICT_FACETS]

        # Build lookup dict: normalized_name -> FacetValue
        facet_lookup = {}
        # V23.2: Also build measurement-normalized lookup for O(1) matching
        facet_lookup_measurement = {}
        for fv in facet_values:
            normalized = self._normalize(fv.facet_value_name)
            facet_lookup[normalized] = fv
            # V23.2: Pre-compute measurement-normalized version
            if re.search(r'\d', normalized):
                measurement_normalized = self._normalize_measurement_in_text(normalized)
                if measurement_normalized != normalized:
                    facet_lookup_measurement[measurement_normalized] = fv

        if not facet_lookup:
            return MatchResult(
                keyword=keyword,
                facet_value=None,
                match_type='none',
                score=0,
                matched_text=''
            )

        # 1. Try exact match
        if keyword_normalized in facet_lookup:
            fv = facet_lookup[keyword_normalized]
            return MatchResult(
                keyword=keyword,
                facet_value=fv,
                match_type='exact',
                score=100,
                matched_text=fv.facet_value_name
            )

        # V23.2: Try measurement-normalized match
        # '200cm' should match facet '200 cm'
        keyword_measurement_normalized = self._normalize_measurement_in_text(keyword_normalized)
        if keyword_measurement_normalized != keyword_normalized:
            # Keyword contained a measurement that was normalized
            if keyword_measurement_normalized in facet_lookup:
                fv = facet_lookup[keyword_measurement_normalized]
                return MatchResult(
                    keyword=keyword,
                    facet_value=fv,
                    match_type='exact',
                    score=100,
                    matched_text=fv.facet_value_name
                )

        # V23.2: O(1) lookup in pre-built measurement-normalized dict
        if keyword_normalized in facet_lookup_measurement:
            fv = facet_lookup_measurement[keyword_normalized]
            return MatchResult(
                keyword=keyword,
                facet_value=fv,
                match_type='exact',
                score=100,
                matched_text=fv.facet_value_name
            )
        if keyword_measurement_normalized in facet_lookup_measurement:
            fv = facet_lookup_measurement[keyword_measurement_normalized]
            return MatchResult(
                keyword=keyword,
                facet_value=fv,
                match_type='exact',
                score=100,
                matched_text=fv.facet_value_name
            )

        # 2. Try synonym match - check if keyword synonyms match any facet
        synonyms = get_synonyms(keyword_normalized)
        for syn in synonyms:
            syn_normalized = self._normalize(syn)
            if syn_normalized in facet_lookup:
                fv = facet_lookup[syn_normalized]
                return MatchResult(
                    keyword=keyword,
                    facet_value=fv,
                    match_type='synonym',
                    score=95,  # High score for synonym match
                    matched_text=fv.facet_value_name
                )

        # 3. Try fuzzy match
        choices = list(facet_lookup.keys())
        best_match = process.extractOne(
            keyword_normalized,
            choices,
            scorer=fuzz.ratio
        )

        if best_match:
            matched_name = best_match[0]
            score = best_match[1]
            fv = facet_lookup[matched_name]

            # v5: Validate the fuzzy match makes sense (length checks)
            if not self._is_valid_fuzzy_match(keyword_normalized, matched_name):
                # Invalid fuzzy match due to length mismatch
                return MatchResult(
                    keyword=keyword,
                    facet_value=None,
                    match_type='none',
                    score=0,
                    matched_text=''
                )

            # v12: Validate semantic match (prevents pyjama -> pyjamabroeken)
            if not self._is_semantic_match(keyword_normalized, matched_name):
                # Invalid fuzzy match - keyword embedded incorrectly
                return MatchResult(
                    keyword=keyword,
                    facet_value=None,
                    match_type='none',
                    score=0,
                    matched_text=''
                )

            # Apply stricter threshold for winkel/merk facets
            threshold = self._get_threshold_for_facet(fv.facet_name)

            if score >= threshold:
                return MatchResult(
                    keyword=keyword,
                    facet_value=fv,
                    match_type='fuzzy',
                    score=score,
                    matched_text=fv.facet_value_name
                )

        # 4. No match found
        return MatchResult(
            keyword=keyword,
            facet_value=None,
            match_type='none',
            score=best_match[1] if best_match else 0,
            matched_text=''
        )

    def _coverage_tokens(self, text: str) -> list[str]:
        """V29: tokenize for token-coverage matching. Lowercase, strip
        punctuation, drop stopwords + shops, drop tokens shorter than 3.
        """
        if not text:
            return []
        import re as _re
        toks = _re.findall(r"[a-zÀ-ž]+", text.lower())
        return [t for t in toks if len(t) >= 3
                and t not in STOPWORDS
                and t not in SHOP_NAMES]

    def _tokens_equal_modulo_morphology(self, t1: str, t2: str) -> bool:
        """V29: two tokens count as 'the same' under the coverage scorer
        if they're exactly equal, equal after stripping a common Dutch
        plural/diminutive suffix, equal after collapsing double vowels
        (paneel↔panelen), or near-equal under a tight fuzz.ratio.
        """
        if not t1 or not t2:
            return False
        if t1 == t2:
            return True
        for suffix in ("en", "es", "er", "s"):
            if len(t1) > len(suffix) + 2 and t1.endswith(suffix) and t1[:-len(suffix)] == t2:
                return True
            if len(t2) > len(suffix) + 2 and t2.endswith(suffix) and t2[:-len(suffix)] == t1:
                return True
        if self._collapse_double_vowels(t1) == self._collapse_double_vowels(t2):
            return True
        if abs(len(t1) - len(t2)) <= 2 and min(len(t1), len(t2)) >= 4:
            if fuzz.ratio(t1, t2) >= 88:
                return True
        return False

    def match_by_token_coverage(self, keyword: str,
                                facet_values: list[FacetValue],
                                exclude_winkel: bool = False) -> MatchResult:
        """V29 EXPERIMENTAL: facet-value-centric scorer.

        For each facet value, count how many of the keyword's content
        tokens appear inside it (after morphology). Pick the value with
        the highest (matched_count, score), where
            score = 50 * keyword_coverage + 50 * facet_specificity
        i.e. it rewards both "covers the keyword well" AND "doesn't have
        a lot of other noise tokens".

        Replaces the per-token cascade in match_multi_word for the
        common multi-word keyword case where two facet values look
        equally good under the per-token scorer but actually differ in
        how many of the keyword's tokens they collectively explain.

        Sample: keyword "senioren huistelefoon" → "senioren telefoon"
        (after V28 compound decomposition).
          - Senioren telefoon: matched=2 / cov=100% / spec=100% → 100
          - Vaste telefoon:    matched=1 / cov= 50% / spec= 50% →  50
        ⇒ Senioren telefoon wins, as expected.
        """
        kw_tokens = self._coverage_tokens(keyword)
        if not kw_tokens:
            return MatchResult(keyword=keyword, facet_value=None,
                               match_type='none', score=0, matched_text='')

        if exclude_winkel:
            facet_values = [fv for fv in facet_values
                            if fv.facet_name.lower() not in STRICT_FACETS]

        best = None  # (matched_kw, score, count, fv)
        for fv in facet_values:
            fv_tokens = self._coverage_tokens(fv.facet_value_name)
            if not fv_tokens:
                continue

            # Track WHICH keyword positions matched, so we can reward
            # contiguous matches over spread-out ones. For
            # "vaste senioren telefoons":
            #   Senioren telefoon → positions {1, 2}  (adjacent)
            #   Vaste telefoon    → positions {0, 2}  (gap at 1)
            # The keyword's natural reading favours adjacent matches —
            # they're the "phrase inside the phrase" while the unmatched
            # token between them just modifies the rest.
            matched_positions: list = []
            for i, kt in enumerate(kw_tokens):
                if any(self._tokens_equal_modulo_morphology(kt, ft)
                       for ft in fv_tokens):
                    matched_positions.append(i)
            matched_kw = len(matched_positions)
            if matched_kw == 0:
                continue

            coverage = matched_kw / len(kw_tokens)
            specificity = matched_kw / len(fv_tokens)
            if matched_kw == 1:
                adjacency = 1.0
            else:
                span = matched_positions[-1] - matched_positions[0] + 1
                adjacency = matched_kw / span  # 1.0 when fully contiguous
            score = int(50 * coverage + 30 * specificity + 20 * adjacency)
            if score < 50:
                continue

            count = getattr(fv, "count", 0) or 0
            cand = (matched_kw, score, count, fv)
            if best is None or cand[:3] > best[:3]:
                best = cand

        if best is None:
            return MatchResult(keyword=keyword, facet_value=None,
                               match_type='none', score=0, matched_text='')
        matched_kw, score, _, fv = best
        return MatchResult(
            keyword=keyword, facet_value=fv,
            match_type='token_coverage',
            score=score,
            matched_text=fv.facet_value_name,
        )

    def match_with_partial(self, keyword: str, facet_values: list[FacetValue], exclude_winkel: bool = False) -> MatchResult:
        """
        Enhanced matching that also tries partial ratio for compound keywords.

        V29: when use_token_coverage is on AND the keyword has 2+ content
        tokens, route through match_by_token_coverage first. Single-token
        keywords stay on the legacy path (the token-coverage scorer
        degenerates to a simple "this token in facet" check there, where
        the existing partial-ratio logic is still better).
        """
        if (self.use_token_coverage
                and len(self._coverage_tokens(keyword)) >= 2):
            tc = self.match_by_token_coverage(keyword, facet_values,
                                              exclude_winkel=exclude_winkel)
            if tc.is_match:
                return tc

        """
        Existing implementation continues below.

        Useful for keywords like "zweefparasol 3m" matching "Zweefparasols".

        Args:
            keyword: The search keyword
            facet_values: List of FacetValue objects
            exclude_winkel: If True, exclude winkel facets

        Returns:
            MatchResult with best match
        """
        # First try standard match
        result = self.match(keyword, facet_values, exclude_winkel)
        if result.is_match:
            return result

        # Try partial ratio for longer keywords
        if len(keyword) < 4:
            return result

        # Filter out winkel facets if requested
        if exclude_winkel:
            facet_values = [fv for fv in facet_values if fv.facet_name.lower() not in STRICT_FACETS]

        keyword_normalized = self._normalize(keyword)
        facet_lookup = {self._normalize(fv.facet_value_name): fv for fv in facet_values}

        if not facet_lookup:
            return result

        # v23: Iterate the top candidates instead of bailing after the first one
        # fails validation — with substring-heavy type facets (e.g. "ontstopper"
        # matches 5 values equally), extractOne often returns an invalid one first
        # and we miss the semantically-valid candidate. Pick best by (score, count).
        candidates = process.extract(
            keyword_normalized,
            list(facet_lookup.keys()),
            scorer=fuzz.partial_ratio,
            limit=10,
        )

        best = None  # (score_after_penalty, count, fv)
        for matched_name, raw_score in candidates:
            fv = facet_lookup[matched_name]
            score = raw_score - 5  # penalty preserved from v14

            if not self._is_valid_fuzzy_match(keyword_normalized, matched_name):
                continue
            if not self._is_semantic_match(keyword_normalized, matched_name):
                continue

            threshold = self._get_threshold_for_facet(fv.facet_name)
            if score < threshold:
                continue

            cand = (score, getattr(fv, "count", 0) or 0, fv)
            # Only tiebreak on (score, count); FacetValue isn't orderable so
            # comparing the full tuple raises TypeError when both ties match.
            if best is None or (cand[0], cand[1]) > (best[0], best[1]):
                best = cand

        if best:
            score, _, fv = best
            return MatchResult(
                keyword=keyword,
                facet_value=fv,
                match_type='fuzzy',
                score=score,
                matched_text=fv.facet_value_name,
            )

        return result

    def match_multi_word(
        self,
        keyword: str,
        facet_values: list[FacetValue],
        all_type_facets: list[FacetValue] = None,
        require_type_for_merk: bool = True,
        current_main_category: str = None,
        category_name: str = None
    ) -> list[MatchResult]:
        """
        Try matching each word in a multi-word keyword.
        v5: First tries full keyword match, then individual words.
        Prioritizes type_ facets over merk/winkel facets.
        Supports cross-category type matching with redirect to correct subcategory.
        v13: Skip words that are already covered by category name.

        Useful for "zweefparasol grijs" -> [type_parasol match, kleur match]
        Also handles "balkon bloembakken" -> "Balkon bloembakken" (full match)

        Args:
            keyword: Multi-word search keyword
            facet_values: List of FacetValue objects from current subcategory
            all_type_facets: All type facets for cross-category lookup
            require_type_for_merk: If True, only allow merk matches if there's also a type match
            current_main_category: Current main category (e.g., "huis_tuin") for prioritizing same-category matches
            category_name: Category display name (e.g., "Tuintafels") - words matching this are skipped

        Returns:
            List of MatchResults for each matching word
        """
        # V29: try the facet-value-centric token-coverage scorer on the FULL
        # keyword before falling into the per-word cascade. The per-word
        # path scores tokens individually and uses a count tiebreak, which
        # picks "Draadloze telefoon" for "vaste senioren telefoons" simply
        # because Draadloze has the most products. Token-coverage compares
        # how many keyword tokens each candidate facet value covers and
        # rewards positional adjacency — picks "Senioren telefoon" instead.
        if self.use_token_coverage:
            tc = self.match_by_token_coverage(keyword, facet_values)
            if tc.is_match:
                return [tc]

        words = keyword.split()
        results = []
        has_type_match = False

        # v13: Identify words that are already covered by the category name
        # e.g., "tafel" in keyword when category is "Tuintafels" - no need to match on facets
        words_in_category = set()
        if category_name:
            cat_lower = category_name.lower()
            cat_stem = cat_lower.rstrip('s').rstrip('en')
            for word in words:
                word_lower = word.lower()
                word_stem = word_lower.rstrip('s').rstrip('en')
                # Check if word is contained in category name (or vice versa)
                if (word_lower in cat_lower or cat_lower in word_lower or
                    word_stem in cat_lower or cat_stem in word_lower or
                    word_stem in cat_stem or cat_stem in word_stem):
                    words_in_category.add(word_lower)

        # v5: First try to match the FULL keyword (for compound facets like "Balkon bloembakken")
        full_match = self.match_full_keyword(keyword, facet_values)
        if full_match.is_match:
            # If full keyword matches, add it as primary result
            results.append(full_match)
            has_type_match = full_match.is_priority_facet
            # For EXACT matches on priority facets, we can skip individual word matching
            # for OTHER priority facets (to prevent "bloembakken" overriding "Balkon bloembakken")
            # But we still want to try winkel/merk matches for other words
            if full_match.match_type == 'exact' and full_match.is_priority_facet:
                # Skip to winkel/merk matching (don't try more priority facets)
                pass  # Continue to winkel/merk passes below
            elif full_match.match_type in ('exact', 'synonym'):
                # For exact/synonym, skip individual priority matching but allow winkel/merk
                pass

        # v6: Try word pair synonyms BEFORE individual word matching
        # This catches "extra groot" -> "XXL" before "extra" matches something else
        # Track which words were consumed by synonym pairs
        words_used_in_pairs = set()
        if len(words) >= 2:
            for i in range(len(words) - 1):
                word_pair = f"{words[i]} {words[i+1]}"
                pair_result = self._match_with_synonyms(word_pair, facet_values)
                if pair_result and pair_result.is_match:
                    results.append(pair_result)
                    has_type_match = has_type_match or pair_result.is_priority_facet
                    # Mark both words as used so they don't match individually
                    words_used_in_pairs.add(words[i].lower())
                    words_used_in_pairs.add(words[i+1].lower())

        # v5: Filter facets into priority (type_, kleur etc) and non-priority (merk, winkel)
        priority_facets = [fv for fv in facet_values if fv.facet_name.lower().startswith(PRIORITY_FACET_PREFIXES)]
        non_priority_facets = [fv for fv in facet_values if not fv.facet_name.lower().startswith(PRIORITY_FACET_PREFIXES)]

        # First pass: try to match priority facets (type_, kleur, materiaal, etc.)
        # v6: Skip stopwords and words already used in synonym pairs (e.g., "extra" from "extra groot")
        # v13: Also skip words that are already covered by category name
        for word in words:
            if (len(word) >= 3 and
                word.lower() not in STOPWORDS and
                word.lower() not in words_used_in_pairs and
                word.lower() not in words_in_category):
                result = self.match_with_partial(word, priority_facets, exclude_winkel=True)
                if result.is_match:
                    results.append(result)
                    has_type_match = True

        # v5: Cross-category type lookup if no local type match found
        # Search order: 1) same main_category, 2) all other categories
        # Only match non-stopword terms to prevent "action" -> "Action camera"
        # v6: Also skip words already used in synonym pairs
        # v13: Also skip words covered by category name
        if not has_type_match and all_type_facets:
            # Filter out stopwords, words used in pairs, and words in category from cross-category matching
            meaningful_words = [w for w in words if w.lower() not in STOPWORDS and w.lower() not in words_used_in_pairs and w.lower() not in words_in_category]
            meaningful_keyword = ' '.join(meaningful_words) if meaningful_words else ''

            if meaningful_keyword:  # Only try if we have meaningful words left
                cross_cat_result = self._find_cross_category_type_match(
                    meaningful_keyword, meaningful_words, all_type_facets, current_main_category
                )
                if cross_cat_result:
                    results.append(cross_cat_result)
                    has_type_match = True

        # Second pass: try non-priority, non-strict facets for unmatched words
        # v6: Also skip stopwords and words used in synonym pairs
        # v13: Also skip words covered by category name
        matched_words = {r.keyword.lower() for r in results}
        non_strict_facets = [fv for fv in non_priority_facets if fv.facet_name.lower() not in STRICT_FACETS]
        for word in words:
            if (len(word) >= 3 and
                word.lower() not in matched_words and
                word.lower() not in STOPWORDS and
                word.lower() not in words_used_in_pairs and
                word.lower() not in words_in_category):
                result = self.match_with_partial(word, non_strict_facets, exclude_winkel=True)
                if result.is_match:
                    results.append(result)

        # Third pass: try winkel facet matches
        # v10: Winkel matches MUST be exact (score = 100) to prevent false positives
        # This prevents generic shop names from matching unrelated searches
        # v6: Also skip words used in synonym pairs
        # v13: Also skip words covered by category name
        matched_words = {r.keyword.lower() for r in results}
        winkel_facets = [fv for fv in facet_values if fv.facet_name.lower() == 'winkel']

        for word in words:
            if (len(word) >= 3 and
                word.lower() not in matched_words and
                word.lower() not in STOPWORDS and
                word.lower() not in words_used_in_pairs and
                word.lower() not in words_in_category):
                result = self.match_with_partial(word, winkel_facets, exclude_winkel=False)
                if result.is_match:
                    # v10: Winkel match alleen toevoegen als exact match (score = 100)
                    if result.score >= STRICT_FACET_EXACT_THRESHOLD:
                        results.append(result)

        # Fourth pass: try merk facet matches
        # v10: Merk matches MUST be exact (score = 100) to prevent false positives
        # e.g., "Combisteel" verkoopt veel producten - alleen matchen bij exact zoeken
        # v6: Also skip words used in synonym pairs
        # v13: Also skip words covered by category name
        matched_words = {r.keyword.lower() for r in results}
        merk_facets = [fv for fv in facet_values if fv.facet_name.lower() == 'merk']

        for word in words:
            if (len(word) >= 3 and
                word.lower() not in matched_words and
                word.lower() not in STOPWORDS and
                word.lower() not in words_used_in_pairs and
                word.lower() not in words_in_category):
                result = self.match_with_partial(word, merk_facets, exclude_winkel=False)
                if result.is_match:
                    # v10: Merk match alleen toevoegen als exact match (score = 100)
                    if result.score >= STRICT_FACET_EXACT_THRESHOLD:
                        results.append(result)

        # Deduplicate by facet name (keep best score per facet)
        seen_facets = {}
        for r in results:
            if r.facet_value:
                facet_name = r.facet_value.facet_name
                if facet_name not in seen_facets or r.score > seen_facets[facet_name].score:
                    seen_facets[facet_name] = r

        # V30: Als kleurtint gematcht is, verwijder kleur (kleurtint is specifieker)
        # Voorbeeld: "donkerblauw" -> kleurtint~Donkerblauw EN kleur~Blauw → verwijder kleur
        if 'kleurtint' in seen_facets and 'kleur' in seen_facets:
            del seen_facets['kleur']

        # v5: Sort results: priority facets first, then non-strict, then strict, by score
        sorted_results = sorted(
            seen_facets.values(),
            key=lambda r: (not r.is_priority_facet, r.is_strict_facet, -r.score)
        )

        return sorted_results

    @staticmethod
    def _collapse_double_vowels(s: str) -> str:
        """
        Dutch plural/closed-vs-open-syllable normalizer.

        Many Dutch nouns keep a single vowel in the plural (open syllable) but
        use a doubled vowel in the singular (closed syllable):
          paneel / panelen    verhaal / verhalen    boot / boten
        Collapsing 'aa','ee','oo','uu' -> single letter makes the two forms
        comparable after '-en' is stripped.
        """
        import re as _re
        return _re.sub(r'([aeouAEOU])\1+', r'\1', s)

    def _is_semantic_match(self, keyword: str, facet_value_name: str) -> bool:
        """
        v12: Check if the match is semantically valid.

        Prevents false matches like:
        - "wasmachine" -> "bellenblaasmachine" (keyword embedded in middle)
        - "meubels" -> "Badmeubelsets" (different product category!)

        The key insight: Dutch compound words are built by PREFIX + CORE.
        - "wasmachine" should match "Wasmachines" (plural suffix only)
        - "wasmachine" should NOT match "bellenblaasmachine" (different prefix)
        - "parasol" should match "Zweefparasol" (keyword is the core)
        - "meubels" should NOT match "meubelsets" (sets is a different product!)

        Rules:
        1. Exact match (incl. after suffix removal): VALID
        2. Keyword at END of facet (compound word): VALID
        3. Keyword at START of facet: only VALID if remainder is just a suffix
           - "wasmachine" -> "wasmachines" (s is suffix) = VALID
           - "meubel" -> "meubelsets" (sets is NOT just suffix) = INVALID

        Args:
            keyword: The search keyword (normalized)
            facet_value_name: The facet value name (normalized)

        Returns:
            True if semantic match is valid, False otherwise
        """
        kw = keyword.lower().strip()
        fv = facet_value_name.lower().strip()

        # Exact match is always valid
        if kw == fv:
            return True

        # Remove common Dutch plural/diminutive suffixes for comparison
        kw_base = kw
        fv_base = fv
        for suffix in DUTCH_SUFFIXES:
            if kw.endswith(suffix) and len(kw) > len(suffix) + 2:
                kw_base = kw[:-len(suffix)]
            if fv.endswith(suffix) and len(fv) > len(suffix) + 2:
                fv_base = fv[:-len(suffix)]

        # Check if base forms match exactly
        if kw_base == fv_base:
            return True

        # V29: Dutch double-vowel normalization (paneel <-> panelen, boot <-> boten).
        if self._collapse_double_vowels(kw_base) == self._collapse_double_vowels(fv_base):
            return True

        # V12 STRICTER RULE: Keyword at START of facet
        # Only valid if the REMAINDER after keyword is just a plural/diminutive suffix
        # This prevents "meubel" -> "meubelsets" (sets is not just a suffix)
        if fv.startswith(kw):
            remainder = fv[len(kw):]
            # Check if remainder is empty or just a known suffix
            if remainder == '' or remainder in DUTCH_SUFFIXES:
                return True
            # Otherwise: INVALID - the facet has more than just a suffix
            # e.g., "meubel" + "sets" -> "sets" is not in DUTCH_SUFFIXES

        if fv_base.startswith(kw_base):
            remainder = fv_base[len(kw_base):]
            # After suffix removal, check if stems match or remainder is suffix
            if remainder == '' or remainder in DUTCH_SUFFIXES:
                return True
            # INVALID: stems don't match
            # e.g., kw_base="meubel", fv_base="meubelset" -> remainder="set" not a suffix

        # Rule 2: Keyword at END of facet (e.g., "parasol" -> "Zweefparasol")
        # This is valid because Dutch compounds: PREFIX + CORE_WORD
        if fv.endswith(kw) or fv_base.endswith(kw_base):
            return True

        # Rule 3: Facet at START of keyword (reverse - user types longer form)
        # Same stricter logic applies
        if kw.startswith(fv):
            remainder = kw[len(fv):]
            if remainder == '' or remainder in DUTCH_SUFFIXES:
                return True

        if kw_base.startswith(fv_base):
            remainder = kw_base[len(fv_base):]
            if remainder == '' or remainder in DUTCH_SUFFIXES:
                return True

        # Rule 4: Facet at END of keyword
        if kw.endswith(fv) or kw_base.endswith(fv_base):
            return True

        # REJECT: Any other substring relationship is embedded in middle
        # "wasmachine" is in "bellenblaasmachine" but NOT at start or end
        # -> This is a FALSE POSITIVE we want to reject
        if kw in fv or kw_base in fv_base:
            return False

        # No substring relationship - let fuzzy matching handle it
        return False

    def _find_cross_category_type_match(
        self,
        keyword: str,
        words: list[str],
        all_type_facets: list[FacetValue],
        current_main_category: str = None
    ) -> Optional[MatchResult]:
        """
        v5: Find a type match in other categories when current subcategory has none.
        v7: Only uses type facets from PRODUCT_TYPE_FACETS whitelist.
        v9: Added semantic validation to prevent false matches like "wasmachine" -> "bellenblaasmachine"
        Returns a MatchResult with cross_category_path set to the correct redirect path.

        Search order:
        1. Same main_category first (e.g., other huis_tuin subcategories)
        2. Then other main_categories

        Args:
            keyword: Full keyword
            words: Individual words from keyword
            all_type_facets: All type facets across all categories
            current_main_category: Current main category for prioritization

        Returns:
            MatchResult with cross_category_path if found, None otherwise
        """
        if not all_type_facets:
            return None

        # v7: Filter to only product category type facets (not option facets)
        # This prevents false matches on options like "Met matras", "Opvouwbaar", etc.
        product_type_facets = [
            fv for fv in all_type_facets
            if fv.facet_name.lower() in PRODUCT_TYPE_FACETS
        ]

        if not product_type_facets:
            return None

        # Split type facets by main category
        same_main_cat_facets = []
        other_main_cat_facets = []

        for fv in product_type_facets:
            # Extract main category from URL (e.g., "/products/huis_tuin/..." -> "huis_tuin")
            if current_main_category and f'/{current_main_category}/' in fv.url:
                same_main_cat_facets.append(fv)
            else:
                other_main_cat_facets.append(fv)

        # Try same main_category first
        for facets_to_try in [same_main_cat_facets, other_main_cat_facets]:
            if not facets_to_try:
                continue

            # Try full keyword first
            result = self.match_with_partial(keyword, facets_to_try, exclude_winkel=True)
            if result.is_match and result.score >= CROSS_CATEGORY_MIN_SCORE:
                # v9: Validate semantic match
                if self._is_semantic_match(keyword, result.matched_text):
                    category_path = extract_category_path_from_url(result.facet_value.url)
                    return MatchResult(
                        keyword=result.keyword,
                        facet_value=result.facet_value,
                        match_type='cross_category_type',
                        score=result.score,
                        matched_text=result.matched_text,
                        cross_category_path=category_path
                    )

            # Try individual words
            for word in words:
                if len(word) >= 3:
                    result = self.match_with_partial(word, facets_to_try, exclude_winkel=True)
                    if result.is_match and result.score >= CROSS_CATEGORY_MIN_SCORE:
                        # v9: Validate semantic match
                        if self._is_semantic_match(word, result.matched_text):
                            category_path = extract_category_path_from_url(result.facet_value.url)
                            return MatchResult(
                                keyword=word,
                                facet_value=result.facet_value,
                                match_type='cross_category_type',
                                score=result.score,
                                matched_text=result.matched_text,
                                cross_category_path=category_path
                            )

        return None

    def _match_with_synonyms(self, keyword: str, facet_values: list[FacetValue]) -> Optional[MatchResult]:
        """
        v6: Try to match a keyword via its synonyms.
        Useful for compound terms like "extra groot" -> "XXL".

        Args:
            keyword: The keyword to match (e.g., "extra groot")
            facet_values: List of FacetValue objects

        Returns:
            MatchResult if synonym match found, None otherwise
        """
        keyword_normalized = self._normalize(keyword)
        synonyms = get_synonyms(keyword_normalized)

        if not synonyms:
            return None

        # Build lookup dict for facet values
        facet_lookup = {}
        for fv in facet_values:
            normalized = self._normalize(fv.facet_value_name)
            facet_lookup[normalized] = fv

        # Try each synonym
        for syn in synonyms:
            syn_normalized = self._normalize(syn)
            if syn_normalized in facet_lookup:
                fv = facet_lookup[syn_normalized]
                return MatchResult(
                    keyword=keyword,
                    facet_value=fv,
                    match_type='synonym',
                    score=95,  # High score for synonym match
                    matched_text=fv.facet_value_name
                )

        return None

    def match_full_keyword(self, keyword: str, facet_values: list[FacetValue]) -> MatchResult:
        """
        Try to match the full keyword first (for compound terms like "extra groot").

        Args:
            keyword: Full search keyword
            facet_values: List of FacetValue objects

        Returns:
            MatchResult if full keyword matches, else no match
        """
        # Try full keyword match first (excluding winkel)
        result = self.match_with_partial(keyword, facet_values, exclude_winkel=True)
        if result.is_match:
            return result

        # Try with winkel if no other match
        return self.match_with_partial(keyword, facet_values, exclude_winkel=False)

    def _normalize(self, text: str) -> str:
        """Normalize text for comparison."""
        text = text.lower()
        text = text.replace('-', ' ').replace('_', ' ')
        text = ' '.join(text.split())
        return text

    def _normalize_measurement(self, text: str) -> tuple:
        """
        V23.2: Normalize measurements for comparison.
        Handles: '120cm' -> ('120', 'cm'), '12 cm' -> ('12', 'cm'), '120 CM' -> ('120', 'cm')
        Returns (number, unit) tuple or None if not a measurement.
        """
        text = text.lower().strip()
        # Pattern: number followed by optional space and unit
        match = re.match(r'^(\d+(?:[.,]\d+)?)\s*(cm|mm|m|kg|g|l|ml|w|v|a|inch|")?$', text)
        if match:
            number = match.group(1).replace(',', '.')
            unit = match.group(2) or ''
            return (number, unit)
        return None

    def _measurements_match(self, word1: str, word2: str) -> bool:
        """
        V23.2: Check if two words represent the same measurement.
        '120cm' matches '120 cm' but NOT '12 cm'.
        """
        m1 = self._normalize_measurement(word1)
        m2 = self._normalize_measurement(word2)
        if m1 and m2:
            # Both are measurements - compare number and unit
            return m1[0] == m2[0] and m1[1] == m2[1]
        return False

    def _normalize_measurement_in_text(self, text: str) -> str:
        """
        V23.2: Normalize measurements in text by adding space between number and unit.
        '200cm' -> '200 cm', '120x80' stays as is (handled separately)
        """
        # Pattern: number directly followed by unit (no space)
        result = re.sub(r'(\d+)(cm|mm|m|kg|g|l|ml|w|v|a)\b', r'\1 \2', text.lower())
        return result

    def match_subcategory_name(
        self,
        keyword: str,
        categories_df,
        main_category: str = None
    ) -> Optional[dict]:
        """
        V14: Match keyword against subcategory display names.

        This is a fallback when no facet match is found. If the keyword matches
        a subcategory name, redirect to that subcategory (without facet filter).

        Example:
            keyword: "scharnieren"
            -> matches subcategory "Deurscharnieren" (klussen_486170_6356938)
            -> redirect to /products/klussen/klussen_486170_6356938/

        Args:
            keyword: Search keyword to match
            categories_df: DataFrame with categories (must have 'display_name', 'url_name' columns)
            main_category: Optional main category to filter on (e.g., "klussen")

        Returns:
            Dict with match info if found:
            {
                'matched_category': 'Deurscharnieren',
                'url_name': 'klussen_486170_6356938',
                'category_path': '/products/klussen/klussen_486170_6356938',
                'score': 85,
                'match_type': 'subcategory_name'
            }
            None if no match found.
        """
        from src.validation_rules import SUBCATEGORY_MATCH_THRESHOLD, SUBCATEGORY_MATCH_ENABLED

        if not SUBCATEGORY_MATCH_ENABLED:
            return None

        if not keyword or len(keyword) < 3:
            return None

        keyword_lower = keyword.lower().strip()
        keyword_normalized = self._normalize(keyword)

        # Filter categories by main_category if provided
        if main_category:
            # url_name starts with main_category (e.g., "klussen_486170")
            filtered_cats = categories_df[
                categories_df['url_name'].str.startswith(main_category + '_', na=False)
            ]
        else:
            filtered_cats = categories_df

        best_match = None
        best_score = 0
        best_is_exact = False  # Track if best match is exact (full word match)

        for _, row in filtered_cats.iterrows():
            display_name = row.get('display_name', '')
            url_name = row.get('url_name', '')

            if not display_name or not url_name:
                continue

            display_lower = display_name.lower()
            display_normalized = self._normalize(display_name)

            is_exact = False
            score = 0

            # Check exact match first (highest priority)
            if keyword_lower == display_lower or keyword_normalized == display_normalized:
                score = 100
                is_exact = True
            else:
                # Check if keyword matches a complete word in category name
                # e.g., "stofzuigers" should match "Stofzuigers" but not "Speelgoed stofzuigers"
                display_words = display_normalized.split()
                if keyword_normalized in display_words:
                    keyword_idx = display_words.index(keyword_normalized)
                    # V30: Check if preposition directly precedes keyword → qualifier pattern
                    # "Dildo's met zuignap" → "met" before "zuignap" → keyword is a feature, not the product
                    has_preposition_before = (
                        keyword_idx > 0 and
                        display_words[keyword_idx - 1] in PREPOSITION_QUALIFIERS
                    )
                    if has_preposition_before:
                        # Skip: keyword describes a feature of another product, not the primary product
                        score = 0  # Below any threshold
                    else:
                        # Keyword is a complete word in category - prioritize shorter category names
                        # "Stofzuigers" (1 word) > "Speelgoed stofzuigers" (2 words)
                        score = 100
                        is_exact = (len(display_words) == 1)  # Exact if category is single word
                else:
                    # Fuzzy match for compound words like "scharnieren" -> "Deurscharnieren"
                    # V14.1: Use fuzz.ratio for scoring (NOT partial_ratio which gives 100 for substrings)
                    # This ensures "scharnieren" -> "Deurscharnieren" gets score 85, not 100
                    score = fuzz.ratio(keyword_normalized, display_normalized)

                    # Bonus for keyword at START or END of category name (semantic position)
                    # But don't use partial_ratio as it inflates the score
                    if keyword_lower in display_lower:
                        if display_lower.startswith(keyword_lower) or display_lower.endswith(keyword_lower):
                            # Small bonus for good semantic position (max +10, capped at 99)
                            # Score 100 is reserved for exact matches only
                            score = min(99, score + 10)

            # Apply semantic validation (keyword at start or end) for non-exact matches
            if score >= SUBCATEGORY_MATCH_THRESHOLD and not is_exact:
                if not self._is_semantic_match(keyword_lower, display_lower):
                    continue

            # Update best match: prefer exact matches, then highest score
            should_update = False
            if score >= SUBCATEGORY_MATCH_THRESHOLD:
                if is_exact and not best_is_exact:
                    # Exact match beats non-exact
                    should_update = True
                elif is_exact == best_is_exact and score > best_score:
                    # Same type, higher score wins
                    should_update = True
                elif is_exact and best_is_exact and score == best_score:
                    # Both exact, same score - prefer shorter category name
                    if best_match and len(display_name) < len(best_match['matched_category']):
                        should_update = True

            if should_update:
                best_score = score
                best_is_exact = is_exact
                # Build category path. url_name is like "klussen_486170_6356938"
                # or "sport_outdoor_vrije-tijd_484428". The maincat is every
                # underscore-segment before the first numeric segment — split
                # rather than regex so hyphenated maincats (sport_outdoor_vrije-tijd,
                # films-series, boeken-19395973) survive.
                main_cat_parts = []
                for part in url_name.split('_'):
                    if part.isdigit():
                        break
                    main_cat_parts.append(part)
                if main_cat_parts and len(main_cat_parts) < len(url_name.split('_')):
                    main_cat = '_'.join(main_cat_parts)
                    category_path = f"/products/{main_cat}/{url_name}"
                else:
                    category_path = f"/products/{url_name}"

                best_match = {
                    'matched_category': display_name,
                    'url_name': url_name,
                    'category_path': category_path,
                    'score': score,
                    'match_type': 'subcategory_name'
                }

        return best_match


if __name__ == "__main__":
    from src.db_loader import DataLoader
    from src.facet_filter import FacetFilter

    # Load facets
    loader = DataLoader(use_cache=True)
    facets_df = loader.load_facets()

    # Filter for Parasols category
    facet_filter = FacetFilter(facets_df)
    filtered = facet_filter.filter_by_subcategory("504063")
    facet_values = facet_filter.get_facet_values(filtered)

    # Create matcher
    matcher = KeywordMatcher(fuzzy_threshold=80, strict_winkel=True)

    print("Keyword Matcher Test (v2 - strict winkel)")
    print("=" * 60)

    # Test keywords
    test_keywords = [
        "zweefparasol",      # Should fuzzy match "Zweefparasols"
        "zweefparasols",     # Should exact match "Zweefparasols"
        "stokparasol",       # Should fuzzy match "Stokparasols"
        "grijs",             # Should exact match "Grijs"
        "madison",           # Should exact match "Madison"
        "laptop",            # Should NOT match anything
        "zweefparasol grijs", # Multi-word: should match type + kleur
        "action",            # Should NOT match winkel (strict)
    ]

    for kw in test_keywords:
        print(f"\nKeyword: '{kw}'")

        # Single match
        result = matcher.match_with_partial(kw, facet_values)
        if result.is_match:
            print(f"  Match: {result.matched_text}")
            print(f"  Type:  {result.match_type}")
            print(f"  Score: {result.score}")
            print(f"  Facet: {result.facet_value.url_fragment}")
            print(f"  Is winkel: {result.is_winkel_facet}")
        else:
            print(f"  No match (best score: {result.score})")

        # Multi-word match for compound keywords
        if ' ' in kw:
            print("  Multi-word matches:")
            multi_results = matcher.match_multi_word(kw, facet_values)
            for mr in multi_results:
                print(f"    - {mr.matched_text} ({mr.facet_value.facet_name}, score: {mr.score}, winkel: {mr.is_winkel_facet})")
