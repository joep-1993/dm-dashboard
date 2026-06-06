"""
Koptekst prompt v2 — experimental.

Designed alongside gpt_service.py::generate_product_content (v1, productie).
NOT wired into production. To activate: see TODO at bottom of this file.

Design context (zie ook scripts/koptekst_v2_comparison.py voor benchmark op 50 URLs):
- One continuous paragraph, no H-tags, no bullets, no whitespace.
- Lengte: 700-1200 tekens, vergelijkbaar met v1.
- GEEN prijzen, GEEN concrete aantal aanbieders (beide dagelijks veranderlijk).
- RELATIEVE URLs (/p/...) — content wordt alleen op beslist.nl getoond.
- Verplichte inhoudselementen geweven in de tekst (geen lijstjes):
  1. Comparison-authority claim met VAGE kwantificeerder ("alle aanbieders", "veel aanbieders", "diverse aanbieders") — Beslist's unieke positionering.
  2. Eén concreet koopcriterium met WAAROM-uitleg.
  3. Impliciet vraag-antwoord uit de buyer journey, geweven in prose (geen FAQ-blok).
- Strikte verboden-openingen lijst (n=5000 v1-analyse toonde 90% start met "Bij het kiezen van" of "Als je op zoek").
- 5 voorbeeld-openingspatronen gegeven aan het model i.p.v. alleen forbiddens (ban-only duwt model naar de minst slechte cliché).

Status per 50-URL benchmark:
- 0% start met verboden zin (v1: 94%)
- 47/47 valid v2's hebben unieke opening-triplets
- 98% bevat comparison-authority claim (v1: 0%)
- 100% noemt "Beslist" (v1: 0%)
- 91% gebruikt relatieve links (v1: 6%)
"""
import os
import random
from openai import OpenAI
from typing import List, Dict


# Comparison-authority zinnen — varieer per koptekst.
# Elke call kiest hier één uit; zo krijgt elke koptekst een andere syntactische vorm
# i.p.v. dat het model standaard "Op Beslist vind je veel aanbieders van ..." pakt.
# Gebruik {h1_title} als placeholder.
COMPARISON_AUTHORITY_PHRASINGS = [
    "Op Beslist vergelijk je alle aanbieders van {h1_title} in één overzicht.",
    "Bij Beslist staan diverse aanbieders van {h1_title} naast elkaar.",
    "Via Beslist vergelijk je {h1_title} van meerdere winkels in één keer.",
    "Een breed aanbod {h1_title} van uiteenlopende aanbieders vind je gebundeld op Beslist.",
    "{h1_title} van talloze webwinkels staan op Beslist overzichtelijk bij elkaar.",
    "Wie {h1_title} zoekt, vindt op Beslist het aanbod van verschillende aanbieders op één plek.",
    "Beslist bundelt {h1_title} van diverse aanbieders, zodat je in één oogopslag vergelijkt.",
    "Op Beslist staat {h1_title} van meerdere webshops overzichtelijk naast elkaar.",
    "Het assortiment {h1_title} van een breed aanbod aanbieders is via Beslist in één keer te overzien.",
    "Vergelijk op Beslist {h1_title} van uiteenlopende aanbieders zonder zelf langs alle winkels te hoeven.",
    "Met Beslist krijg je {h1_title} van diverse webshops in één overzicht.",
    "Beslist verzamelt {h1_title} van talloze aanbieders — handig om verschillen tussen winkels snel te zien.",
]


def _pick_comparison_authority_phrasing(h1_title: str) -> str:
    """Pick one phrasing template at random and format with h1_title."""
    return random.choice(COMPARISON_AUTHORITY_PHRASINGS).format(h1_title=h1_title)


# Defer client instantiation tot call-time, zodat module-import niet faalt
# wanneer OPENAI_API_KEY (nog) niet geladen is. gpt_service.py doet het bij import,
# maar die wordt altijd na dotenv geladen vanuit main.py — dit is hier voorzichtiger
# omdat scripts deze module soms los importeren voor prompt-inspectie.
_client = None
_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _client


SYSTEM_MESSAGE_V2 = """Je bent een online marketeer voor beslist.nl met als doel om de bezoeker te helpen in zijn buyer journey.

# Format (verplicht)
- Schrijf ÉÉN doorlopende alinea, GEEN subkoppen, GEEN H1/H2/H3, GEEN witregels, GEEN bullets.
- Lengte: 700-1200 tekens (vergelijkbaar met de huidige productiekopteksten).
- Output is platte tekst met enkel HTML <a href> tags voor productlinks. Geen andere HTML-tags.

# Verplichte inhoudselementen
Werk deze drie elementen natuurlijk in de alinea — niet als losse zinnen achter elkaar, maar geweven door de tekst:
1. **Comparison-authority claim**: één zin die noemt dat Beslist een breed aanbod aanbieders vergelijkt voor dit onderwerp. Gebruik VAGE kwantificeerders ("alle aanbieders", "veel aanbieders", "diverse aanbieders", "meerdere winkels", "uiteenlopende aanbieders", "talloze webshops", "een breed aanbod aanbieders") — NOOIT een concreet aantal aanbieders. In het user-prompt staat een **suggested phrasing** voor dit specifieke onderwerp; gebruik die zin als richtlijn voor stijl en structuur (zelfde plaatsing van "Beslist", zelfde type kwantificeerder, vergelijkbare zinsvorm). Je mag woorden licht aanpassen zodat de zin natuurlijk binnen de alinea valt, maar wijk niet uit naar een totaal andere standaardformule. **Vermijd in het bijzonder de cliché-opener "Op Beslist vind je veel aanbieders van ..."** — die wordt te vaak gebruikt. Dit is ons unieke voordeel — alleen Beslist kan credibly claimen meerdere aanbieders te vergelijken.
2. **Concreet koopcriterium**: minstens één criterium waar de lezer op moet letten (materiaal, maat, vermogen, compatibiliteit, leeftijd, capaciteit). Leg WAAROM het uitmaakt. Niet een lijstje koopcriteria — één goed uitgewerkt criterium dat in het verhaal past.
3. **Antwoord op een veelgestelde vraag**: weef impliciet één buyer-vraag en het antwoord erop in de tekst ("Hoeveel ... heb je nodig?", "Welke variant past bij ...?", "Waar moet je op letten bij ...?"). Geen FAQ-blok — gewoon één vraag-antwoord verwerkt in de prose.

# Toon
- Spreek de lezer aan met "je", toegankelijk en informatief.
- Tweede persoon, concreet boven abstract.
- Geen "wij/ons/onze" — schrijf vanuit de bezoeker, niet vanuit het bedrijf.
- Vermijd generieke kwalificaties ("ideaal", "perfect", "uitstekend", "een goede keuze", "een heerlijke keuze"). Leg WAAROM iets geschikt is.
- Geen uitroeptekens. Geen overdreven marketing-taal. Behulpzaam en nuchter, niet als een reclamespot.

# ABSOLUUT VERBODEN
- **GEEN prijzen** — niet van individuele producten, niet als gemiddelde, niet als minimum/maximum, niet als prijsklasse. Prijzen veranderen dagelijks; de koptekst moet evergreen blijven.
- **GEEN concrete aantal aanbieders** — gebruik NOOIT een getal voor het aantal winkels/shops/aanbieders (geen "30 aanbieders", geen "70 winkels"). Het aantal aanbieders varieert dagelijks. Gebruik vage kwantificeerders: "alle aanbieders", "veel aanbieders", "diverse aanbieders", "meerdere winkels".
- GEEN flat CTA's zonder context ("Vergelijk hier!", "Bekijk nu!", "Koop snel!").
- GEEN AI-isms ("Laten we eens kijken naar", "In deze blog/gids", "Hopelijk helpt dit", "Welkom op de pagina", "Tot slot").
- GEEN filler-USP's ("snel besteld, snel in huis", "voor de beste prijs" zonder context).
- GEEN keyword stuffing — natuurlijk doseren.
- GEEN "ideaal", "perfect", "uitstekend", "een goede keuze", "een heerlijke keuze" — leg in plaats daarvan WAAROM iets geschikt is.

# Openingszinnen — STRIKT
Begin NOOIT met een van deze formules (ABSOLUTE BAN, geen uitzonderingen):
- "Bij het kiezen van ...", "Bij het selecteren van ...", "Bij het uitkiezen van ...", "Bij het overwegen van ...", "Bij de keuze voor/van ...", "Bij de aanschaf van ...", "Bij de zoektocht naar ..."
- "Het kiezen van ...", "Het maken van de juiste keuze ..."
- "Als je op zoek bent naar ...", "Op zoek naar ...", "Ben je op zoek naar ...", "Zoek je ...", "Als je zoekt naar ..."
- "Wanneer je op zoek ...", "Wanneer je overweegt ...", "Wanneer je kiest voor ..."
- "Welkom op de ... pagina", "Een goede keuze maken ..."

Gebruik in plaats daarvan een van deze 5 patronen — varieer welke je kiest:

1. **Eigenschap-eerst** (begin met een feit of kenmerk van het producttype):
   - "Een [adjectief] [product] kan een stijlvolle aanwinst zijn voor ..."
   - "Een goede [product] maakt het verschil tijdens ..."
   - "Een [product] van [materiaal] geeft je ..."

2. **Directe vraag aan de bezoeker**:
   - "Heb je een [doelgroep] [behoefte]?"
   - "Ben je vaak onderweg met [item]?"
   - "Sta je voor de keuze tussen [optie A] of [optie B]?"

3. **Use-case framing**:
   - "Voor [use-case] kies je een [product] zoals ..."
   - "Voor [doelgroep] is een [product] met [eigenschap] handig omdat ..."
   - "Met een [product] [doe je X] ..."

4. **"Je [werkwoord] [object] op basis van ..."**:
   - "Je [product] kies je op basis van [criterium 1] en [criterium 2]."
   - "Je hebt keuze uit [variant A] tot [variant B], afhankelijk van ..."

5. **Concrete benefit / feature lead**:
   - "Het [eigenschap] karakter van [materiaal] zie je bij ..."
   - "Diverse [merken/typen] [product] bieden [voordeel], maar ..."

Varieer welk patroon je gebruikt — niet 5 kopteksten op rij met patroon 1. Kies het patroon dat het beste past bij de productcategorie (vraag-vorm werkt goed voor gepersonaliseerde producten; use-case voor functionele producten; eigenschap-eerst voor stijl/decoratie).

# Links
- Gebruik 2-3 product-links uit de meegeleverde lijst, ingeweven in de tekst (niet alle in één zin).
- **Gebruik RELATIEVE URLs**: bijvoorbeeld `<a href="/p/product-name/123/456/">korte anchor text</a>`. Strip de `https://www.beslist.nl` prefix als die in de meegeleverde URLs staat. De content wordt alleen op beslist.nl getoond.
- Anchor text: max 3-5 woorden, productnaam of beschrijvende zoekterm.
- VERBODEN anchor texts: "klik hier", "hier klikken", "hier", "deze link", "deze pagina", "deze gids", "lees meer", "meer info", "kijk hier", "bekijk hier", "via deze link".
- Als de productnaam niet natuurlijk in de zin past — maak GEEN link. Herschrijf de zin liever zonder link.
- Verzin NOOIT producten of URLs die niet in de lijst staan.
- Negeer URLs uit de lijst met een lege waarde.

# Output-format
Geef alleen de alinea-tekst. Geen markdown, geen uitleg vooraf of na afloop, geen H1, geen quotes om de tekst heen.
"""


def build_user_prompt_v2(h1_title: str, products: List[Dict]) -> str:
    """Build user prompt voor v2. Strip absolute URL-prefix uit producten zodat het model
    natuurlijk relatieve URLs gebruikt."""
    limited = products[:30]
    products_text = "\n".join([
        f"Product {i+1}\nTitle: {p['title']}\nUrl: {p['url'].replace('https://www.beslist.nl', '')}\nContent: {p.get('listviewContent','')[:200]}\n"
        for i, p in enumerate(limited)
    ])
    suggested_phrasing = _pick_comparison_authority_phrasing(h1_title)
    return f"""# Opdracht
Schrijf de SEO-koptekst voor de Beslist-categoriepagina "{h1_title}".

# Context
- h1_title: {h1_title}
- GEEN prijzen, GEEN aantallen aanbieders in de output — alleen vage kwantificeerders zoals "alle aanbieders" / "veel aanbieders" / "diverse aanbieders" / "uiteenlopende aanbieders" / "talloze webshops".

# Comparison-authority zin — gebruik DEZE stijl/structuur (varieer woorden licht voor natuurlijke flow, maar houd dezelfde vorm; gebruik NIET de cliché "Op Beslist vind je veel aanbieders van ...")
{suggested_phrasing}

# 30 populairste producten (gebruik 2-3 als links, RELATIEVE URLs in /p/... vorm)
{products_text}
"""


def generate_product_content_v2(h1_title: str, products: List[Dict]) -> str:
    """v2 equivalent van gpt_service.generate_product_content. Drop-in signature.

    Wordt nog NIET automatisch aangeroepen door main.py — zie TODO onderaan."""
    from .gpt_service import fix_truncated_urls  # hergebruik bestaande URL-fix logica

    user_prompt = build_user_prompt_v2(h1_title, products)
    response = _get_client().chat.completions.create(
        model=_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_MESSAGE_V2},
            {"role": "user", "content": user_prompt},
        ],
        max_tokens=1500,
        temperature=0.7,
    )
    content = response.choices[0].message.content
    if response.choices[0].finish_reason == "length":
        print(f"[GPT v2] Warning: response was truncated for '{h1_title}'")
    return fix_truncated_urls(content, products)


# TODO — productie-wiring (NIET nu, later):
# 1. In backend/gpt_service.py: maak een router-functie die op basis van env var
#    KOPTEKST_PROMPT_VERSION (default "v1") delegateert naar generate_product_content
#    of generate_product_content_v2.
# 2. In .env.example: voeg KOPTEKST_PROMPT_VERSION=v1 toe met comment over de A/B mogelijkheid.
# 3. In backend/main.py: de bestaande endpoint die generate_product_content aanroept
#    hoeft niet te veranderen — router doet het werk.
# 4. (Optioneel) frontend toggle in frontend/js/app.js voor handmatig schakelen per run.
