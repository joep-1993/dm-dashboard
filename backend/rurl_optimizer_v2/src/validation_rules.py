"""
Centrale validatieregels voor R-URL Redirect Optimizer.
================================================================================

Alle matching thresholds, stopwords, whitelists en validatieregels op één plek.
Dit bestand is de "single source of truth" voor alle validatielogica.

Versie: V14
Laatste update: Januari 2026

Wijzigingslog:
- V3:  Synoniemen, stricter winkel matching
- V5:  Length validatie, cross-category, priority facets
- V6:  Word-pair synonyms, extended stopwords
- V7:  Product type whitelist
- V8:  Composite word matching
- V9:  Semantische validatie, URL validity
- V10: Exacte match voor merk/winkel
- V11: Diepere subcategorie check, generieke stopwords
- V12: Stricter stem matching (meubels ≠ meubelsets), semantic check op alle fuzzy matches
- V13: Skip woorden die al in categorienaam zitten, measurement normalisatie (120cm = 120 cm)
- V14: Subcategorie naam matching - zoekterm matchen tegen subcategorie display_names
"""

# ==============================================================================
# SCORE THRESHOLDS
# ==============================================================================

# Basis fuzzy matching threshold
FUZZY_THRESHOLD = 80  # Minimum score voor een fuzzy match

# Stricter matching voor bepaalde facet types
STRICT_FACET_THRESHOLD = 95  # Hogere drempel voor winkel/merk (V3)

# V10: Merk/winkel vereisen EXACTE match om false positives te voorkomen
# Voorbeeld: "steel" mag NIET matchen op merk "Combisteel"
STRICT_FACET_EXACT_THRESHOLD = 100

# V5/V9: Cross-category matching vereist hogere score
# Verhoogd van 85 naar 90 in V9 om valse matches te voorkomen
CROSS_CATEGORY_MIN_SCORE = 90

# V9: Same-category matching mag lagere score hebben
SAME_CATEGORY_MIN_SCORE = 80


# ==============================================================================
# LENGTH VALIDATIE (V5)
# ==============================================================================

# Minimum lengtes om false fuzzy matches te voorkomen
# Voorbeeld: "12v" mag NIET matchen op "E" (energielabel)
MIN_KEYWORD_LENGTH_FOR_FUZZY = 3  # Keywords korter dan 3 chars: geen fuzzy
MIN_FACET_LENGTH_FOR_FUZZY = 3    # Facets korter dan 3 chars: geen fuzzy

# Minimum lengte ratio tussen keyword en facet
# Keyword moet minimaal 40% van facet lengte zijn (of vice versa)
# Verhoogd van 0.3 naar 0.4 in V5
MIN_LENGTH_RATIO = 0.4


# ==============================================================================
# FACET CLASSIFICATIE
# ==============================================================================

# Facets die stricter matching vereisen (minder waarschijnlijk intentioneel)
# V3: winkel toegevoegd, V5: merk toegevoegd
STRICT_FACETS = {'winkel', 'merk'}

# Facets die prioriteit krijgen bij matching (product attributen > shop/brand)
# V5: Type facets eerst, dan kleur/materiaal/maat
PRIORITY_FACET_PREFIXES = ('type_', 'kleur', 'materiaal', 'maat', 'vorm')


# ==============================================================================
# STOPWORDS - Woorden die NIET matchen mogen (V5-V11)
# ==============================================================================

STOPWORDS = {
    # --- Marketing termen (V5, V27 uitgebreid) ---
    'actie', 'acties', 'aanbieding', 'aanbiedingen',
    'korting', 'kortingen', 'kortingscode',
    'sale', 'deal', 'deals',
    'beste', 'best', 'top',
    'goedkoop', 'goedkope', 'goedkoopste',
    'budget', 'voordelig', 'voordelige', 'voordeel',
    'premium', 'luxe', 'pro', 'plus',
    'gratis', 'uitverkoop', 'opruiming', 'outlet',
    'cashback', 'exclusief', 'exclusieve',
    'tijdelijk', 'tijdelijke',
    'populair', 'populaire', 'aanrader', 'bestseller',
    'garantie',

    # --- Koop-intentie / onderzoek (V27) ---
    'vergelijken', 'vergelijk',
    'review', 'reviews', 'recensie', 'recensies',
    'test', 'ervaring', 'ervaringen',
    'prijs', 'prijzen',
    'alternatief', 'alternatieven',

    # --- Winkel gerelateerd (V5, V27 uitgebreid) ---
    'kopen', 'bestellen', 'online', 'shop', 'store', 'winkel',
    'webshop', 'webwinkel',
    'bezorgen', 'bezorging', 'levering', 'leveren',
    'verzenden', 'verzending',
    'retour', 'retourneren',
    'voorraad', 'voorradig', 'leverbaar',
    'afhalen', 'ophalen',
    'winkelwagen', 'kassa', 'assortiment', 'magazijn',
    'filiaal', 'klantenservice',

    # --- Algemene zoektermen (V5) ---
    'nieuw', 'nieuwe', '2024', '2025', '2026',

    # --- Nederlandse grammatica (V6, V27 uitgebreid) ---
    # Voorzetsels / lidwoorden / voegwoorden
    'de', 'het', 'een', 'en', 'of', 'met', 'voor', 'van', 'naar', 'aan',
    'op', 'in', 'uit', 'bij', 'tot', 'over', 'onder', 'tegen', 'door',
    'te', 'om', 'als', 'dan', 'dat', 'die', 'deze', 'dit',
    'maar', 'want', 'dus', 'toch',

    # Bijwoorden / partikels (V27)
    'er', 'daar', 'hier', 'ook', 'nog', 'al', 'niet', 'geen', 'wel',

    # Werkwoorden (V27)
    'zijn', 'is', 'was', 'waren',
    'word', 'wordt', 'worden', 'werd', 'werden',
    'heb', 'heeft', 'hebben', 'had', 'hadden',

    # Persoonlijke voornaamwoorden (V27)
    'ik', 'jij', 'hij', 'zij', 'wij', 'jullie',
    'mij', 'me', 'je', 'hem', 'haar', 'ons', 'hun', 'hen',
    'mijn', 'jouw', 'uw',

    # Vraagwoorden (V27)
    'wat', 'waar', 'wanneer', 'hoe', 'waarom', 'wie', 'welke', 'welk',

    # Positie-voorzetsels (V27)
    # NB: 'rond' verwijderd (= vorm-facet), 'binnen'/'buiten' verwijderd (= facetwaarden)
    'via', 'tussen', 'achter', 'naast', 'per',

    # --- Locatie-aanduidingen (V6) ---
    # V27: VERWIJDERD - hier bestaan facets/subcategorieën voor
    # Verwijderd: 'muur', 'wand', 'vloer', 'plafond', 'deur', 'raam'

    # --- Generieke eigenschappen (V11) ---
    # V27: VERWIJDERD - hier bestaan facets voor
    # Verwijderd: 'elektrisch', 'elektrische', 'elektro', 'digitaal', 'digitale',
    #   'draadloos', 'draadloze', 'wireless', 'automatisch', 'automatische',
    #   'handmatig', 'handmatige', 'manueel', 'timer'

    # V26: Bijvoeglijke naamwoorden VERWIJDERD - hier bestaan WEL facets voor
    # Verwijderd: 'groot', 'grote', 'klein', 'kleine', 'lang', 'lange', 'kort', 'korte'
}


# ==============================================================================
# WINKELNAMEN (V23) - Worden APART behandeld, nooit naar facet matchen
# ==============================================================================
#
# Zoektermen die een winkelnaam bevatten worden apart behandeld:
# - De winkelnaam zelf wordt NIET gematcht naar facets
# - De rest van het keyword (bijv. "parasol" uit "action parasol") wordt WEL gematcht
# - Deze groep kan later apart geanalyseerd worden voor winkel-specifieke redirects
#
# Voorbeeld:
#   Keyword: "hema eierkoker"
#   -> "hema" wordt genegeerd (is winkelnaam)
#   -> "eierkoker" wordt gematcht naar facet
#   -> Kolom 'shop_in_keyword' = "hema" (voor latere analyse)

SHOP_NAMES = {
    # --- Grote warenhuizen / algemene winkels ---
    'action', 'action.com',
    'hema', 'hema.nl',
    'blokker', 'blokker.nl',
    'xenos', 'xenos.nl',
    'flying tiger', 'tiger',
    'sostrene grene',

    # --- Bouwmarkten ---
    'gamma', 'gamma.nl',
    'praxis', 'praxis.nl',
    'karwei', 'karwei.nl',
    'hornbach', 'hornbach.nl',
    'hubo', 'hubo.nl',
    'bouwmaat',
    'toolstation',
    'cranenbroek', 'cranenbroek.nl', 'van cranenbroek',

    # --- Supermarkten ---
    'albert heijn', 'ah', 'ah.nl',
    'jumbo', 'jumbo.nl',
    'lidl', 'lidl.nl',
    'aldi', 'aldi.nl',
    'plus', 'plus.nl',
    'dirk', 'dirk.nl',
    'deka', 'dekamarkt',
    'coop',
    'spar',

    # --- Drogisterijen / parfumerie ---
    'kruidvat', 'kruidvat.nl',
    'etos', 'etos.nl',
    'trekpleister', 'trekpleister.nl',
    'douglas', 'douglas.nl',
    'da',

    # --- Mode / kleding ---
    'primark',
    'zeeman', 'zeeman.nl',
    'wibra', 'wibra.nl',
    'c&a', 'c en a',
    'h&m', 'h en m', 'hm',
    'zara',
    'we fashion', 'wefashion',
    'only',
    'vero moda',
    'bristol',

    # --- Elektronica ---
    'mediamarkt', 'media markt', 'mediamarkt.nl',
    'coolblue', 'coolblue.nl',
    'bcc', 'bcc.nl',
    'expert', 'expert.nl',
    'alternate', 'alternate.nl',

    # --- Online retailers ---
    'bol.com', 'bol', 'bolcom',
    'amazon', 'amazon.nl', 'amazon.de',
    'wehkamp', 'wehkamp.nl',
    'otto', 'otto.nl',
    'fonq', 'fonq.nl',
    'vidaxl', 'vidaxl.nl',
    'leen bakker', 'leenbakker', 'leenbakker.nl',
    'jysk', 'jysk.nl',
    'kwantum', 'kwantum.nl',

    # --- Wonen / meubels ---
    'ikea', 'ikea.nl',
    'casa',
    'seats and sofas', 'seats en sofas',
    'goossens', 'goossens.nl',

    # --- Tuincentra ---
    'intratuin', 'intratuin.nl',
    'tuincentrum', 'groenrijk',
    'life and garden',

    # --- Sport ---
    'decathlon', 'decathlon.nl',
    'intersport',
    'perry sport', 'perrysport',
    'aktiesport',

    # --- Speelgoed ---
    'intertoys', 'intertoys.nl',
    'bart smit', 'bartsmit',
    'toychamp',

    # --- Dieren ---
    'pets place', 'petsplace',
    'jumper', 'jumper.nl',
    'ranzijn',
    'discus',

    # --- Automotive ---
    'halfords', 'halfords.nl',

    # --- Overig ---
    'makro', 'makro.nl',
    'sligro',
    'hanos',
    'bigbazar', 'big bazar',
}


# ==============================================================================
# PRODUCT TYPE FACETS WHITELIST (V7)
# ==============================================================================
#
# Alleen deze type_ facets mogen gebruikt worden voor cross-category matching.
# Dit voorkomt false matches op "option" facets zoals:
# - "Met matras", "Opvouwbaar", "Bluetooth" (dit zijn opties, geen producttypes)
#
# Een facet komt in deze lijst als de waarden ECHTE PRODUCTEN zijn:
# - Meervoudsvormen (Wasmachines, Parasols)
# - Product-woorden (Stoomreiniger, Convectorkachel)

PRODUCT_TYPE_FACETS = {
    # --- Originele V7 facets ---
    'type_agenda', 'type_aquarium_onderhoud', 'type_aromatherapie', 'type_asbak',
    'type_badpak', 'type_bakvorm', 'type_bank_onderdelen', 'type_bc', 'type_bekers',
    'type_boiler', 'type_bollen', 'type_boom', 'type_borden', 'type_bouten',
    'type_bouwblok', 'type_breinbreker', 'type_bs', 'type_buis', 'type_buttplug',
    'type_ca', 'type_cap', 'type_cases', 'type_deur', 'type_diepvriezer',
    'type_dierenriemen', 'type_dm', 'type_dressboys', 'type_droger', 'type_dvdspeler',
    'type_effectmachines', 'type_eg', 'type_elek_fiets', 'type_elekttacc',
    'type_epilator_scheren', 'type_erotische_slips', 'type_fauteuil', 'type_fietsstacc',
    'type_fontein', 'type_frezen', 'type_funcooking', 'type_gebak', 'type_gehoorbescherming',
    'type_geiser', 'type_gewichtsmanchetten', 'type_gitaren', 'type_glazen', 'type_gootsteen',
    'type_gordond', 'type_handsch', 'type_heffers', 'type_hoeden', 'type_horren',
    'type_houders', 'type_kamerplaten', 'type_kapstokken', 'type_kerstversiering',
    'type_ketting', 'type_keukensch', 'type_kh', 'type_kinderwagenhoes', 'type_kl',
    'type_km', 'type_knikkerbaan', 'type_knutselmateriaal', 'type_koffieacc',
    'type_koffiezetter', 'type_kommen', 'type_kookplaat', 'type_kookt', 'type_kopjes',
    'type_kr', 'type_kruk', 'type_krulsp', 'type_lampen', 'type_lens', 'type_lucht',
    'type_luier', 'type_modellandschap', 'type_moeren', 'type_netschoen', 'type_nh',
    'type_nooddekens', 'type_oj', 'type_onderdeel_tand', 'type_onesies', 'type_oorreiniger',
    'type_oven', 'type_oven_oven', 'type_parasol', 'type_pedaal', 'type_planten',
    'type_plantenbakken', 'type_pluggen', 'type_pump', 'type_puzzelmat', 'type_r',
    'type_raamaccessoires', 'type_radiators', 'type_rasp', 'type_rijlaars', 'type_sand',
    'type_sapcentrifuge', 'type_schakelaars', 'type_schalen', 'type_schilderij',
    'type_schiller', 'type_schommel', 'type_scooter', 'type_sfeerhaarden',
    'type_sieradendozen', 'type_skateboards', 'type_slof', 'type_slot', 'type_snijder',
    'type_so', 'type_sp', 'type_spatel', 'type_spiegels', 'type_sponzen', 'type_sportbroek',
    'type_sportshirts', 'type_ssok', 'type_st', 'type_stekkers', 'type_steppen',
    'type_stickers', 'type_stmp', 'type_stofzuiger', 'type_stoma', 'type_strijkijzer',
    'type_strooiwagens', 'type_stuur', 'type_tand', 'type_tang', 'type_terrasverwarming',
    'type_theeacc', 'type_topdekmatras', 'type_tosti', 'type_veiligheid', 'type_versn',
    'type_vhdoos', 'type_waterschoen', 'type_wiel', 'type_woongadget', 'type_zakl', 'type_zb',

    # --- V7.1: Toegevoegde product categorie facets ---
    'type_opberger',        # Vacuümzakken, Speelgoedmand, Boodschappenkrat
    'type_airco',           # Mobiele airco, Split airco
    'type_airfryer',        # Airfryer Oven, Dubbele airfryer
    'type_blender',         # Smoothiemakers, Power blender
    'type_c',               # Action camera, Dashcam, Camcorder
    'type_douche',          # Regendouche, Douchesets, Handdouchekop
    'type_folie',           # Vacuümzakken, Aluminiumfolie
    'type_fietsstoeltjes',  # Achterzitjes, Voorzitjes
    'type_haagplanten',     # Buxus, Laurier, Taxus
    'type_hoofdtelefoon',   # Gaming headset, DJ koptelefoon
    'type_pannensets',      # Koekenpannenset, Braadpannenset
    'type_stoomreinig',     # Stoommop, Stoomzuiger
    'type_verwarming',      # Convectorkachel, Oliekachel
    'type_warmhouder',      # Chafing dish, Bordenwarmer

    # --- V9: Huishoudelijke apparatuur facets ---
    'type_kookplaat',       # Gas kookplaten, Inductie kookplaten
    'type_kookplaat_oven',  # Gas, Keramisch, Inductie
    'type_lucht',           # Luchtbevochtigers, Luchtkoelers
    'type_strijkijzer',     # Stoomstrijkijzers, Kledingstomers
    'type_sapcentrifuge',   # Citruspersen, Slowjuicers
    'type_koffiezetter',    # Espressomachines, Filterkoffie
    'type_tosti',           # Contactgrills, Tosti-ijzers
    'type_afz',             # Afzuigkap types
    'type_weegschaal',      # Digitaal, Analoog
    'type_droger',          # Warmtepompdrogers, Condensdrogers
}


# ==============================================================================
# URL VALIDATIE REGELS (V9/V11)
# ==============================================================================
#
# Deze regels bepalen of een facet geldig is voor een bepaalde categorie URL.
#
# Situatie: R-URL is in categorie A, facet komt uit categorie B
#
# Case 1: A == B (exact match)
#         -> GELDIG: facet hoort bij dezelfde categorie
#         Voorbeeld: R-URL in "meubilair_389371", facet in "meubilair_389371"
#
# Case 2: B is PARENT van A (facet_subcat is prefix van rurl_subcat)
#         -> GELDIG: facet van parent geldt ook voor children
#         Voorbeeld: R-URL in "meubilair_389371_395590", facet in "meubilair_389371"
#
# Case 3: B is CHILD van A (rurl_subcat is prefix van facet_subcat) [V11]
#         -> ONGELDIG: facet van child geldt NIET voor parent
#         Voorbeeld: R-URL in "meubilair_389371", facet in "meubilair_389371_395590"
#         Oplossing: Redirect naar de categorie waar de facet WEL geldig is
#
# Case 4: A en B zijn verschillende branches
#         -> ONGELDIG: geen relatie tussen categorieën


# ==============================================================================
# SEMANTISCHE MATCH VALIDATIE (V9/V12)
# ==============================================================================
#
# Voorkomt false matches waar keyword "embedded" is in midden van facet.
#
# GOED: keyword aan BEGIN van facet + alleen SUFFIX remainder
#       "wasmachine" -> "Wasmachines" (meervoud suffix 's')
#
# GOED: keyword aan EINDE van facet
#       "parasol" -> "Zweefparasol" (compound prefix)
#
# FOUT: keyword in MIDDEN van facet
#       "wasmachine" -> "bellenblaasmachine" (GEBLOKKEERD)
#
# FOUT (V12): keyword aan BEGIN maar remainder is GEEN suffix
#       "meubels" -> "Badmeubelsets" (GEBLOKKEERD)
#       Reden: "meubel" + "set" -> "set" is geen suffix, maar nieuw woord!
#
# Nederlandse compound woorden: PREFIX + KERNWOORD
# - "zweefparasol" = "zweef" + "parasol"
# - "wasmachine" = "was" + "machine"
# - "meubelset" = "meubel" + "set" (ANDER product dan "meubel"!)

# Suffixen die verwijderd worden voor base-form vergelijking
# V12: Deze lijst bepaalt ook wat een GELDIGE remainder is bij start-match
# Als remainder NIET in deze lijst zit, is het een nieuw woord = INVALID
DUTCH_SUFFIXES = ['s', 'en', 'jes', 'tjes', 'eren']


def detect_shops_in_keyword(keyword: str) -> list[str]:
    """
    Return every SHOP_NAME (single- or multi-word) that appears in the
    keyword. Word-boundary aware so 'davids' doesn't trigger 'da'.
    """
    if not keyword:
        return []
    import re as _re
    kw = keyword.lower().strip()
    tokens = kw.split()
    token_set = set(tokens)
    hits = []
    for shop in SHOP_NAMES:
        s = shop.lower()
        if ' ' in s:
            # multi-word: whole-phrase substring with word boundaries
            if _re.search(r'(?:^|\s)' + _re.escape(s) + r'(?:\s|$)', kw):
                hits.append(shop)
        else:
            if s in token_set:
                hits.append(shop)
    return hits


# ==============================================================================
# MATCHING VOLGORDE (V5-V6)
# ==============================================================================
#
# De volgorde waarin matching geprobeerd wordt:
#
# 1. FULL KEYWORD MATCH
#    Probeer eerst het hele keyword te matchen
#    Voorbeeld: "balkon bloembakken" -> "Balkon bloembakken"
#
# 2. WORD PAIR SYNONYMS (V6)
#    Probeer 2-woord combinaties als synoniemen
#    Voorbeeld: "extra groot" -> "XXL"
#    Woorden die matchen worden gemarkeerd en niet nogmaals geprobeerd
#
# 3. PRIORITY FACETS
#    Match individuele woorden tegen type_, kleur, materiaal, etc.
#    Skip stopwords en al-gematchte woorden
#
# 4. CROSS-CATEGORY LOOKUP
#    Als geen local type match: zoek in andere categorieën
#    Alleen voor niet-stopwords, score >= 90, semantische validatie
#
# 5. NON-PRIORITY, NON-STRICT FACETS
#    Overige facets (niet type/kleur/merk/winkel)
#
# 6. WINKEL FACETS (V10: alleen exact match)
#    Winkelnamen alleen bij exacte match (score = 100)
#
# 7. MERK FACETS (V10: alleen exact match)
#    Merknamen alleen bij exacte match (score = 100)
#
# 8. SUBCATEGORIE NAAM MATCHING (V14)
#    Als geen facet match: zoek of zoekterm matcht met subcategorie display_name
#    Redirect naar die subcategorie (zonder facet filter)


# ==============================================================================
# SUBCATEGORIE NAAM MATCHING (V14)
# ==============================================================================
#
# Nieuwe fallback: als geen facet match gevonden, check of de zoekterm matcht
# met een subcategorie naam binnen dezelfde main category.
#
# Voorbeeld:
#   R-URL: /products/klussen/r/scharnieren/
#   Zoekterm: "scharnieren"
#   Subcategorie: "Deurscharnieren" (klussen_486170_6356938)
#   -> Redirect naar: /products/klussen/klussen_486170_6356938/
#
# Validatie regels:
# - Fuzzy score minimaal SUBCATEGORY_MATCH_THRESHOLD (80)
# - Zoekterm moet aan BEGIN of EINDE van categorie naam zitten
# - Semantische validatie (zoals bij facet matching)
# - Alleen binnen dezelfde main_category zoeken
#
# Dit lost het probleem op van R-URLs op main category niveau
# (zoals /products/klussen/r/scharnieren/) die geen subcategory ID hebben.

SUBCATEGORY_MATCH_THRESHOLD = 80  # Minimum fuzzy score voor subcategorie match
SUBCATEGORY_MATCH_ENABLED = True  # Toggle voor deze feature
