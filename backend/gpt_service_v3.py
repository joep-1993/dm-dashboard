"""
V3 koptekst-prompt: per-maincat *informationele* koopgids-prompts.

Achtergrond: analyse van de organisch rankende Google-content voor onze
categorie-zoektermen (31 maincats, 117 zoektermen) liet zien dat wat rankt
koopgidsen zijn, geen promotieblurbs. Per maincat is een prompt opgesteld die
de echte koopvragen, beslissingscriteria en het vakjargon van dat onderwerp
behandelt. De volledige prompts staan in
    backend/data/kopteksten_maincat_prompts_v3.json  (key = main_cat_name)
en de onderbouwing in
    Downloads/claude/kopteksten_informational_prompts_2026-07-01.md

AANGESLOTEN OP PRODUCTIE sinds 2026-07-02: backend/main.py::process_single_url
gebruikt generate_product_content_v3 wanneer KOPTEKST_PROMPT_VERSION == "v3"
(de default). De maincat wordt uit de URL afgeleid via resolve_maincat_from_url.
Terugvallen op v1 kan zonder codewijziging met env KOPTEKST_PROMPT_VERSION=v1
(v1 blijft volledig intact in gpt_service.py::generate_product_content).
"""
import json
import os
from typing import Dict, List, Optional

from openai import OpenAI

# Hergebruik productie-onderdelen. De user-prompt heeft een eigen v3-variant omdat
# de v1-user-prompt "EEN doorlopende alinea" en "max. 150 woorden" afdwingt, wat de
# koopgids-structuur en lengte van v3 zou blokkeren. De productlijst en linkregels
# blijven identiek aan v1 zodat de productcontext gelijk is.
from typing import List, Dict
from backend.gpt_service import MODEL, fix_truncated_urls


def create_product_recommendation_prompt_v3(h1_title: str, products: List[Dict]) -> str:
    """User-prompt voor v3: identieke productlijst/linkregels als v1, maar
    zonder de 'één alinea'- en 150-woorden-limiet (die botsen met de koopgids-vorm).
    De inhoudelijke sturing zit in de per-maincat system message."""
    limited_products = products[:30]
    products_text = "\n".join(
        f"Product {i + 1}\nTitle: {p['title']}\nUrl: {p['url']}\nContent: {p['listviewContent'][:200]}\n"
        for i, p in enumerate(limited_products)
    )
    return f"""Opdracht
Een prijsbewuste consument landt op deze categoriepagina na het zoeken in Google. Op de pagina staan veel producten waaruit hij moet kiezen (zie de 30 populairste hieronder). Schrijf de introductietekst als een korte, informatieve mini-koopgids die de bezoeker helpt de juiste keuze te maken.
- Schrijf 2 tot 4 korte alinea's, gescheiden door een witregel (dus NIET één doorlopende alinea). Geen opsommingstekens.
- Volg de lengte- en inhoudsrichtlijnen uit de system message (koopvragen, meetbare keuzecriteria, vakjargon).
- Vermijd het noemen van prijzen.
- Gebruik waar relevant klikbare HTML-links <a href="url"> met als linktekst een KORTE, heldere omschrijving (max 3-5 woorden), bijvoorbeeld "Beeztees kattentuigje Hearts" in plaats van de volledige productnaam met maten. Gebruik alleen "urls" die hieronder voorkomen en negeer urls met een lege waarde.
- VERBODEN LINKTEKSTEN (gebruik deze NOOIT als anchor text): "klik hier", "hier klikken", "hier", "deze link", "deze pagina", "deze gids", "deze", "lees meer", "meer info", "kijk hier", "bekijk hier", "via deze link". Linktekst MOET de productnaam of een logische, beschrijvende zoekterm zijn; past dat niet natuurlijk, maak dan GEEN link.

Hieronder de context:
Zoekwoord in Google: {h1_title}

De 30 populairste producten met titel en de bijbehorende link:
{products_text}
"""

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

_PROMPTS_PATH = os.path.join(os.path.dirname(__file__), "data", "kopteksten_maincat_prompts_v3.json")

with open(_PROMPTS_PATH, encoding="utf-8") as _fh:
    # { main_cat_name: {"slug": str, "prompt": str} }
    MAINCAT_PROMPTS: Dict[str, dict] = json.load(_fh)

# Genormaliseerd lengte-/structuurbeleid. Overschrijft de (onderling afwijkende)
# lengte-instructies onderaan de individuele per-maincat prompts.
NORMALIZE_FOOTER = """

BELANGRIJK — onderstaande instructies zijn bindend en overschrijven alles hierboven wat ermee botst:

STRUCTUUR: schrijf 2 tot 4 korte, scanbare alinea's in koopgids-vorm (gebruiksdoel -> keuzecriteria -> onderhoud/veiligheid/vergelijken). Scheid elke alinea met een witregel (een lege regel). Schrijf minstens 2 alinea's; gebruik geen opsommingstekens, maar vloeiende zinnen. Korte tussenkopjes met <h3> mogen, maar zijn optioneel.

LENGTE: standaard 160 tot 240 woorden. Eenvoudige/impulsieve onderwerpen 120 tot 170 woorden. Complexe, functionele onderwerpen (meubels, huishoudelijke apparaten, voertuigen, sanitair) tot maximaal 320 woorden. Nooit meer dan 350 woorden.

VERBODEN OPENINGEN: begin NOOIT met "Bij het kiezen van", "Bij het selecteren van", "Als je op zoek bent naar", "Op zoek naar", "Ben je op zoek naar", "Zoek je", "Welkom op de" of vergelijkbare zoek-/welkomstformuleringen. Open in plaats daarvan meteen met inhoud (het gebruiksdoel of de belangrijkste keuze).

VERBODEN WOORDEN: vermijd lege kwalificaties als "ideaal", "perfect", "uitstekend", "een goede keuze" en "een heerlijke keuze". Wees concreet: leg uit WAAROM iets past.

Schrijf alleen de koptekst zelf, zonder een kop als "Koptekst:" en zonder meta-uitleg."""

# Terugvalprompt als de maincat onbekend is (de gedeelde basis zonder maincat-module).
GENERIC_BASE_V3 = """Je bent een productadviseur voor beslist.nl (een prijsvergelijker) en schrijft de introductietekst boven een categoriepagina. Doel: de bezoeker echt helpen kiezen met een korte, oprechte mini-koopgids die de concrete koopvragen beantwoordt, geen promotiepraat.

HARDE REGELS:
- Spreek de lezer aan met "je"; schrijf vanuit de bezoeker, gebruik nooit "wij", "onze", "ons" of "we".
- Noem nooit prijzen, bedragen of kortingen; de tekst moet evergreen blijven.
- Geen uitroeptekens; nuchtere, deskundige toon; geen loze superlatieven ("ideaal", "perfect").
- Link alleen naar producten uit de meegeleverde productlijst, met een korte beschrijvende ankertekst (3-5 woorden) die de productnaam is; nooit vage ankers ("klik hier", "hier", "deze link", "lees meer"); verzin nooit een URL.

INHOUD: begin bij het gebruiksdoel en help het juiste type kiezen; leg de beslissende keuzecriteria uit met concrete, meetbare kenmerken en wat ze voor de koper betekenen; behandel waar relevant compatibiliteit, onderhoud en veiligheid; gebruik het vakjargon dat kopers zelf gebruiken. Beslist vergelijkt aanbieders, dus je mag uitnodigen opties te vergelijken, maar alleen als echte keuzehulp."""


# Maps de eerste URL-padsegment-slug (main_cat_slug uit scraper_service.parse_beslist_url,
# identiek aan de keys van scraper_service.MAIN_CATEGORY_H1) naar de exacte maincat-key
# in MAINCAT_PROMPTS. De URL-slugs zijn legacy en wijken af van onze maincat-namen, dus
# expliciet in plaats van normaliseren zodat dit niet stil kan breken.
URL_SLUG_TO_MAINCAT: Dict[str, str] = {
    "autos": "Auto's",
    "main_sanitair": "Sanitair",
    "meubilair": "Meubels",
    "elektronica": "Elektronica",
    "tuin_accessoires": "Tuinartikelen",
    "horloge": "Horloges",
    "computers": "Computers",
    "schoenen": "Schoenen",
    "mode_accessoires": "Mode accessoires",
    "voor_volwassenen": "Erotiek",
    "huishoudelijke_apparatuur": "Huishoudelijk",
    "huis_tuin": "Woonaccessoires",
    "sieraden_horloges": "Sieraden",
    "accessoires": "Multimedia-accessoires",
    "eten_drinken": "Eten & drinken",
    "kantoorartikelen": "Kantoor",
    "boeken": "Boeken",
    "software": "Software",
    "fietsen": "Fietsen",
    "muziekinstrument": "Muziekinstrumenten",
    "cadeaus_gadgets_culinair": "Cadeaus & gadgets",
    "mode": "Kleding",
    "dieren_accessoires": "Dierenbenodigdheden",
    "films-series": "Films & Series",
    "speelgoed_spelletjes": "Speelgoed",
    "parfum_aftershave": "Parfumerie",
    "klussen": "Klussen",
    "gezond_mooi": "Drogisterij",
    "cddvdrom": "Games",
    "sport_outdoor_vrije-tijd": "Sport & outdoor",
    "baby_peuter": "Baby & peuter",
}

# Self-check bij import: elke mapping moet naar een bestaande prompt-key wijzen.
_missing = sorted(v for v in URL_SLUG_TO_MAINCAT.values() if v not in MAINCAT_PROMPTS)
if _missing:
    print(f"[GPT-v3] WARNING: URL_SLUG_TO_MAINCAT verwijst naar onbekende maincats: {_missing}")


def resolve_maincat_from_url(url: str) -> Optional[str]:
    """Bepaal de v3-maincat-key voor een beslist.nl categorie-URL (werkt voor zowel
    hoofd- als subcategorie-URL's; de eerste padsegment-slug bepaalt de maincat).
    Geeft None als de slug niet te mappen is; generate_product_content_v3 valt dan
    terug op de generieke v3-basisprompt."""
    try:
        from backend.scraper_service import parse_beslist_url
        main_cat_slug, _, _ = parse_beslist_url(url)
    except Exception:
        return None
    if not main_cat_slug:
        return None
    return URL_SLUG_TO_MAINCAT.get(main_cat_slug)


def build_system_message_v3(maincat: Optional[str]) -> str:
    """System message = per-maincat prompt (of generieke basis) + genormaliseerd lengte-/structuurbeleid."""
    entry = MAINCAT_PROMPTS.get((maincat or "").strip())
    base = entry["prompt"] if entry else GENERIC_BASE_V3
    return base + NORMALIZE_FOOTER


def has_prompt_for(maincat: Optional[str]) -> bool:
    return (maincat or "").strip() in MAINCAT_PROMPTS


def generate_product_content_v3(h1_title: str, products: List[Dict], maincat: Optional[str]) -> str:
    """Genereer een v3-koptekst met de per-maincat informationele prompt.

    Gebruikt dezelfde user-prompt (productcontext) als v1, zodat het verschil
    puur in de system message zit.
    """
    user_prompt = create_product_recommendation_prompt_v3(h1_title, products)
    system_message = build_system_message_v3(maincat)

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_prompt},
        ],
        max_tokens=2000,
        temperature=0.7,
    )
    content = response.choices[0].message.content
    if response.choices[0].finish_reason == "length":
        print(f"[GPT-v3] Warning: response truncated for '{h1_title}'")
    return fix_truncated_urls(content, products)
