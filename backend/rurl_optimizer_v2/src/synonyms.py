"""
Synonym mappings for keyword matching.
Maps common search terms to facet values.

V27: Uitgebreid van 16 naar 80+ entries met 9 categorieën.
"""

from typing import Optional

# Synonyms: search term -> facet value(s) that should match
# V27: Uitgebreid van 16 naar 80+ entries
SYNONYMS = {
    # ==========================================================================
    # MAAT & AFMETING
    # ==========================================================================
    "extra groot": ["xxl", "groot", "extra large", "oversized", "gigantisch"],
    "extra large": ["xxl", "xl", "extra groot"],
    "heel groot": ["xxl", "enorm", "gigantisch"],
    "groot": ["xxl", "xl", "l", "large", "ruim"],
    "medium": ["m", "middel", "middelgroot", "normaal", "standaard"],
    "klein": ["s", "xs", "small", "compact", "mini"],
    "extra klein": ["xs", "mini", "miniatuur"],
    "lang": ["lengte", "verlengd", "lange"],
    "breed": ["breedte", "wijd", "brede", "extra breed"],
    "smal": ["slank", "dun", "smalle"],
    "hoog": ["hoogte", "hoge"],
    "laag": ["lage", "diep", "diepte"],

    # ==========================================================================
    # VORMEN (V27)
    # ==========================================================================
    "rond": ["ronde", "cirkel", "bol", "bolvormig"],
    "vierkant": ["vierkante", "kwadraat", "blokvormig"],
    "rechthoek": ["rechthoekig", "rechthoekige", "langwerpig"],
    "ovaal": ["ovale", "ellips", "eivormig"],
    "driehoek": ["driehoekig", "driehoekige"],

    # ==========================================================================
    # KLEUREN & TINTEN (V27 uitgebreid)
    # ==========================================================================
    "antraciet": ["anthraciet", "donkergrijs", "charcoal", "grafiet"],
    "donkergrijs": ["antraciet", "anthraciet", "charcoal"],
    "lichtgrijs": ["zilvergrijs", "muisgrijs", "platinagrijs"],
    "wit": ["sneeuwwit", "helderwit", "spierwit"],
    "gebroken wit": ["ecru", "creme", "crème", "off-white", "ivoor", "ivoorwit", "beige"],
    "zwart": ["gitzwart", "matzwart", "pikzwart", "noir", "black"],
    "bruin": ["chocoladebruin", "kastanjebruin", "houtkleur"],
    "taupe": ["grijsbruin", "leverkleur", "greige"],
    "beige": ["zand", "zandkleur", "zandkleurig", "kaki", "khaki"],
    "donkerblauw": ["navy", "marine", "marineblauw", "nachtblauw"],
    "lichtblauw": ["babyblauw", "hemelsblauw", "pastelblauw"],
    "rood": ["bordeaux", "bordeauxrood", "wijnrood", "kersrood", "terracotta"],
    "groen": ["olijfgroen", "legergroen", "mintgroen", "pastelgroen", "lime"],
    "geel": ["okergeel", "mosterdgeel", "goudgeel"],
    "zilver": ["zilverkleurig", "silver", "chroom", "rvs-look"],
    "goud": ["goudkleurig", "gold", "messing"],
    "koper": ["koperkleurig", "brons", "bronskleurig"],
    "transparant": ["doorzichtig", "helder", "glashelder", "clear"],
    "meerkleurig": ["multi", "multicolor", "gekleurd", "regenboog", "print", "dessin"],

    # ==========================================================================
    # AFKORTINGEN & EENHEDEN (V27 uitgebreid)
    # ==========================================================================
    "rvs": ["roestvrij staal", "roestvast staal", "roestvrijstaal", "inox"],
    "roestvrij staal": ["rvs", "inox", "roestvast staal"],
    "led": ["led verlichting", "ledlamp", "led-lamp", "smd"],
    "cm": ["centimeter", "centimeters"],
    "mm": ["millimeter", "millimeters"],
    "kg": ["kilo", "kilogram"],
    "gr": ["gram", "grammen"],
    "l": ["liter", "liters", "ltr"],
    "st": ["stuk", "stuks", "stuksverpakking", "exemplaar"],
    "incl": ["inclusief", "inbegrepen"],
    "excl": ["exclusief", "zonder", "niet inbegrepen"],

    # ==========================================================================
    # MATERIALEN (V27)
    # ==========================================================================
    "alu": ["aluminium", "aluminum", "lichtmetaal"],
    "kunststof": ["plastic", "pvc", "pe", "polyethyleen", "polycarbonaat", "pp"],
    "pu": ["polyurethaan", "kunstleer", "nepleer", "imitatieleer", "skai"],
    "mdf": ["spaanplaat", "houtvezelplaat", "geperst hout"],
    "hout": ["houten", "massief hout", "hardhout", "fsc hout", "fsc", "teakhout"],
    "katoen": ["cotton", "textiel", "stoffen", "stof"],
    "wicker": ["vlechtwerk", "rotan", "polyrotan", "kunstrotan", "riet", "rattan"],
    "glas": ["glazen", "veiligheidsglas", "gehard glas"],

    # ==========================================================================
    # PRODUCT SPECIFIEK - Tuin, Meubels & Wonen (V27 uitgebreid)
    # ==========================================================================
    "zweef": ["zweefparasol", "zweefparasols", "hangparasol"],
    "hoes": ["beschermhoes", "afdekhoes", "parasolhoes", "cover"],
    "lounge": ["loungeset", "loungebank", "loungestoel", "hoekbank"],
    "dining": ["diningset", "eethoek", "tuinset", "eettafel", "eetset"],
    "bbq": ["barbecue", "barbeque", "grill", "buitenkeuken", "kamado"],
    "vloerkleed": ["tapijt", "karpet", "kleed"],
    "bijzettafel": ["salontafel", "hoektafel"],

    # ==========================================================================
    # EIGENSCHAPPEN & SPECIFICATIES (V27)
    # ==========================================================================
    "waterdicht": ["waterproof", "regendicht", "waterbestendig", "ip65", "ip68"],
    "spatwaterdicht": ["waterafstotend", "ip44"],
    "uv-bestendig": ["zonwerend", "kleurvast", "uv-werend"],
    "draadloos": ["snoerloos", "wireless", "op accu", "batterij", "oplaadbaar"],
    "smart": ["slim", "slimme", "wifi", "app", "domotica", "bluetooth"],
    "dimbaar": ["instelbaar", "regelbaar"],
    "verstelbaar": ["kantelbaar", "draaibaar", "flexibel", "aanpasbaar", "knikbaar"],
    "inklapbaar": ["opvouwbaar", "vouwbaar", "plooibaar"],
    # V29: antonym-style aliases — users often type "uit..." even though the
    # catalog labels products as "in..." (a sleeper sofa is 'inklapbaar' but
    # people search 'uitklappen').
    "uitklapbaar": ["inklapbaar", "opvouwbaar", "vouwbaar"],
    "uitklappen": ["inklapbaar", "opvouwbaar", "vouwbaar"],
    "uitvouwbaar": ["inklapbaar", "opvouwbaar", "vouwbaar"],
    "duurzaam": ["eco", "ecologisch", "groen", "milieuvriendelijk", "gerecycled"],
    "warm wit": ["warmwit", "2700k", "3000k"],
    "koud wit": ["koel wit", "daglicht", "4000k", "6000k", "6500k"],

    # ==========================================================================
    # VEELVOORKOMENDE SPELFOUTEN (V27)
    # Werkt twee kanten op: spelfout -> correct EN correct -> spelfouten
    # zodat zowel zoektermen als facetwaarden gematcht worden
    # ==========================================================================

    # barbecue varianten
    "barbecue": ["barbeque", "bbq", "barbeceu", "babecue"],
    "barbeque": ["barbecue", "bbq", "barbeceu", "babecue"],
    "barbeceu": ["barbecue", "barbeque", "bbq"],
    "babecue": ["barbecue", "barbeque", "bbq"],

    # accessoires varianten
    "accessoires": ["accesoires", "accessoire", "accesoire"],
    "accesoires": ["accessoires", "accessoire", "accesoire"],
    "accesoire": ["accessoires", "accesoires", "accessoire"],

    # cappuccino varianten
    "cappuccino": ["capuccino", "cappucino", "capucino"],
    "capuccino": ["cappuccino", "cappucino", "capucino"],
    "cappucino": ["cappuccino", "capuccino", "capucino"],
    "capucino": ["cappuccino", "capuccino", "cappucino"],

    # fauteuil varianten
    "fauteuil": ["fotel", "fouteuil", "foteuil"],
    "fotel": ["fauteuil", "fouteuil", "foteuil"],
    "fouteuil": ["fauteuil", "fotel", "foteuil"],
    "foteuil": ["fauteuil", "fotel", "fouteuil"],

    # portemonnee varianten
    "portemonnee": ["portemonee", "portemonnaie", "beurs"],
    "portemonee": ["portemonnee", "portemonnaie", "beurs"],
    "portemonnaie": ["portemonnee", "portemonee", "beurs"],

    # ==========================================================================
    # SANITAIR / BADKAMER (wc <-> toilet)
    # ==========================================================================
    # ==========================================================================
    # TELEFONIE (V28)
    # ==========================================================================
    # Beslist subcat _19934132 (Mobiele telefoons) has facet smart-of-classic
    # value "Senioren mobiel" (id 8381795). The everyday Dutch phrase "senioren
    # telefoon" semantically maps to that, but lexical fuzzy matching can't
    # bridge "telefoon" → "mobiel". An explicit synonym closes the gap.
    "senioren telefoon": ["senioren mobiel"],
    "senioren mobiel": ["senioren telefoon"],
    "ouderen telefoon": ["senioren mobiel", "senioren telefoon"],
    "senioren mobieltje": ["senioren mobiel", "senioren telefoon"],

    "wc": ["toilet"],
    "toilet": ["wc"],
    "wc papier": ["toiletpapier", "toilet papier", "wc-papier", "toiletrol"],
    "wc-papier": ["toiletpapier", "wc papier", "toilet papier", "toiletrol"],
    "toiletpapier": ["wc papier", "wc-papier", "toilet papier", "toiletrol"],
    "toilet papier": ["toiletpapier", "wc papier", "wc-papier", "toiletrol"],
    "wc ontstoppers": ["toilet ontstoppers", "toiletontstoppers", "wc-ontstoppers"],
    "wc-ontstoppers": ["toilet ontstoppers", "toiletontstoppers", "wc ontstoppers"],
    "toilet ontstoppers": ["wc ontstoppers", "wc-ontstoppers", "toiletontstoppers"],
    "toiletontstoppers": ["wc ontstoppers", "wc-ontstoppers", "toilet ontstoppers"],

    # ==========================================================================
    # TV-RESOLUTIE AFKORTINGEN
    # Facetwaarden (televisie_b) spellen de resolutie voluit — "4K Ultra HD",
    # "8K Ultra HD", "Full HD" — terwijl zoekopdrachten de afkorting gebruiken
    # ("4k", "uhd", "fhd"). Zonder mapping matcht "samsung 55 inch 4k uhd tv"
    # niet op "4K Ultra HD": "4k" is te kort (<3 tekens) en "uhd" deelt geen
    # letters met "Ultra HD". Elk synoniem is de EXACTE genormaliseerde
    # facetwaarde-naam, zodat de matcher een synoniem-treffer (score 95) krijgt.
    # ==========================================================================
    "uhd": ["4k ultra hd"],
    "4k": ["4k ultra hd"],
    "4k uhd": ["4k ultra hd"],
    "uhd 4k": ["4k ultra hd"],
    "ultra hd": ["4k ultra hd"],
    "8k": ["8k ultra hd"],
    "8k uhd": ["8k ultra hd"],
    "fhd": ["full hd"],
    "hd ready": ["hd-ready"],
    "hdready": ["hd-ready"],
}

# V28: Compound-noun decomposition. Dutch retail keywords often glue a
# location/specifier onto a base noun: "huistelefoon" = "huis" + "telefoon".
# Indexed facet values usually carry only the base noun ("Senioren telefoon",
# not "Senioren huistelefoon"), so the legacy fuzzy matcher misses them.
# When the full keyword fails to match, we retry with each compound token
# replaced by its base. Keep this dict targeted — over-generalising creates
# false-positive matches.
COMPOUND_DECOMPOSITIONS = {
    # Telefonie
    "huistelefoon": "telefoon",
    "huistelefoons": "telefoon",
    "draadloze telefoon": "telefoon",
    # Single-token concatenations of "senioren X" — Beslist's matcher
    # doesn't decompose Dutch compounds, so the bare concatenated noun
    # never reaches the right facet without an explicit phrase rewrite.
    "seniorentelefoon": "senioren telefoon",
    "seniorentelefoons": "senioren telefoon",
    "seniorenmobiel": "senioren mobiel",
    "seniorenmobiele": "senioren mobiel",
    "seniorenhuistelefoon": "senioren telefoon",
    # Verlichting
    "wandlamp": "lamp",
    "tafellamp": "lamp",
    "bureaulamp": "lamp",
    "vloerlamp": "lamp",
    "staande lamp": "lamp",
    "hanglamp": "lamp",
    "plafondlamp": "lamp",
    # Textiel
    "vloerkleed": "kleed",
    "tafelkleed": "kleed",
    "wandkleed": "kleed",
    # Meubilair
    "tuintafel": "tafel",
    "salontafel": "tafel",
    "eettafel": "tafel",
    "bureautafel": "tafel",
    "kinderstoel": "stoel",
    "bureaustoel": "stoel",
    "tuinstoel": "stoel",
    "kantoorstoel": "stoel",
    "kinderbed": "bed",
    "stapelbed": "bed",
    # Tuin
    "tuinslang": "slang",
    "tuinhuisje": "tuinhuis",
    # Sport / outdoor
    "wandelstok": "stok",
    "kinderfiets": "fiets",
    "elektrische fiets": "fiets",
}


# V31: Dutch compound suffix decomposer. When a keyword token doesn't have
# an explicit entry in COMPOUND_DECOMPOSITIONS but ends with one of these
# common Beslist-category noun suffixes (length >= 4) and has a prefix of
# >= 3 chars, expand_compounds() also yields the split form. Concrete case:
# `wasdroger` is not in COMPOUND_DECOMPOSITIONS, but ends with `droger`,
# prefix `was` (3 chars) → split to `was droger`. The matcher then matches
# the `droger` token to facet value 'Wasmachine en droger kasten'.
#
# Ordered longest-first when iterated (sorting happens at use site) so
# longer overlaps win — e.g. 'wasmachine' is preferred over 'machine' if
# both ever apply to the same token.
DUTCH_COMPOUND_SUFFIXES = (
    # Appliances
    'wasmachine', 'droger', 'machine', 'apparaat', 'ketel', 'oven',
    'koelkast', 'vriezer',
    # Furniture
    'meubels', 'meubel', 'kast', 'stoel', 'tafel', 'bank',
    'fauteuil', 'commode', 'dressoir', 'plank',
    # Outdoor / garden
    'huisje', 'huis', 'schuur', 'tent', 'parasol', 'haard',
    # Bedding / soft furnishing
    'dekbed', 'kussen', 'deken', 'plaid',
    # Lighting
    'lampen', 'lamp', 'spot',
    # Kitchen
    'pannen', 'pan', 'mes',
)

_MIN_COMPOUND_PREFIX_LEN = 3
_MIN_COMPOUND_SUFFIX_LEN = 4


def _suffix_split(token: str) -> Optional[str]:
    """Return 'prefix suffix' if `token` ends with a known Dutch noun
    suffix and the remaining prefix is long enough; otherwise None.

    Skipped when:
      - token is already in COMPOUND_DECOMPOSITIONS (handled there)
      - token contains a hyphen: the hyphen is the publisher-intended
        compound boundary (tv-meubel, e-bike, TP-Link). Further splitting
        produces fragments like 'tv-' that lead to bad matches; the
        cross-maincat subcat-name matcher already handles hyphenated forms.
    """
    t = (token or '').lower()
    if not t or '-' in t or t in COMPOUND_DECOMPOSITIONS:
        return None
    # Sort longest-first so we don't split 'wasmachine' on 'machine'
    # when 'wasmachine' itself is the right suffix.
    for suf in sorted(DUTCH_COMPOUND_SUFFIXES, key=len, reverse=True):
        if len(suf) < _MIN_COMPOUND_SUFFIX_LEN:
            continue
        if t.endswith(suf) and len(t) - len(suf) >= _MIN_COMPOUND_PREFIX_LEN:
            prefix = t[:-len(suf)]
            return f"{prefix} {suf}"
    return None


def expand_compounds(keyword: str) -> list[str]:
    """V28: Generate variants of `keyword` where each compound token is
    replaced by its base noun (per COMPOUND_DECOMPOSITIONS). V31 also tries
    a Dutch noun-suffix split for tokens not in the explicit map
    (e.g. 'wasdroger' → 'was droger'). Returns the original keyword first,
    followed by deduplicated decomposed variants.
    """
    if not keyword:
        return [keyword]
    kw_lower = keyword.lower()
    variants = [keyword]
    seen = {kw_lower}

    # Whole-keyword lookup (handles phrasal compounds like "draadloze telefoon").
    if kw_lower in COMPOUND_DECOMPOSITIONS:
        v = COMPOUND_DECOMPOSITIONS[kw_lower]
        if v not in seen:
            variants.append(v)
            seen.add(v)

    tokens = keyword.split()
    for i, t in enumerate(tokens):
        # 1. explicit COMPOUND_DECOMPOSITIONS entry (e.g. tuinslang → slang)
        base = COMPOUND_DECOMPOSITIONS.get(t.lower())
        if base:
            new_tokens = list(tokens)
            new_tokens[i] = base
            variant = " ".join(new_tokens)
            if variant.lower() not in seen:
                variants.append(variant)
                seen.add(variant.lower())

        # 2. V31: suffix-based split for unknown compounds.
        #    Emit TWO variants:
        #      (a) 'prefix suffix' — full split (e.g. wasdroger → was droger).
        #      (b) suffix-only — drops the prefix (e.g. wasdroger → droger).
        #    The suffix-only variant exists because the token-coverage scorer
        #    drops sharply when extra prefix fragments appear: for
        #    'combi wasmachine wasdroger' the split form
        #    'combi wasmachine was droger' has 2/4 = 50% coverage against
        #    facet 'Wasmachine en droger kasten' (below threshold), but the
        #    suffix-only form 'combi wasmachine droger' has 2/3 = 67% and
        #    matches at ~85.
        suffix_form = _suffix_split(t)
        if suffix_form:
            for replacement in (suffix_form, suffix_form.split(' ', 1)[1]):
                new_tokens = list(tokens)
                new_tokens[i] = replacement
                variant = " ".join(new_tokens)
                if variant.lower() not in seen:
                    variants.append(variant)
                    seen.add(variant.lower())
    return variants


# Reverse mapping: facet value -> search terms that should match it
# Built from SYNONYMS
REVERSE_SYNONYMS = {}
for search_term, facet_values in SYNONYMS.items():
    for fv in facet_values:
        if fv not in REVERSE_SYNONYMS:
            REVERSE_SYNONYMS[fv] = []
        REVERSE_SYNONYMS[fv].append(search_term)


def get_synonyms(keyword: str) -> list[str]:
    """
    Get synonyms for a keyword.

    Args:
        keyword: The search keyword

    Returns:
        List of synonym terms that could match the same facet
    """
    keyword_lower = keyword.lower().strip()
    return SYNONYMS.get(keyword_lower, [])


def get_search_terms_for_facet(facet_value: str) -> list[str]:
    """
    Get search terms that should match a facet value.

    Args:
        facet_value: The facet value

    Returns:
        List of search terms that should match this facet
    """
    fv_lower = facet_value.lower().strip()
    return REVERSE_SYNONYMS.get(fv_lower, [])


def expand_keyword(keyword: str) -> list[str]:
    """
    Expand a keyword to include synonyms.

    Args:
        keyword: The original search keyword

    Returns:
        List containing the original keyword plus any synonyms
    """
    keywords = [keyword]
    synonyms = get_synonyms(keyword)
    keywords.extend(synonyms)
    return keywords
