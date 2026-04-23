"""
Synonym mappings for keyword matching.
Maps common search terms to facet values.

V27: Uitgebreid van 16 naar 80+ entries met 9 categorieën.
"""

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
}

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
