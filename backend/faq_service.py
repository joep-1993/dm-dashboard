"""
FAQ Generation Service

Generates FAQ content for product category pages using the Product Search API and OpenAI.
Adapted from seo_faq/faq_generator_api.py for integration with content_top.
"""

import os
import json
import time
import random
import re
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from openai import OpenAI
from backend.category_lookup import lookup_category

# Configuration
USER_AGENT = "Beslist script voor SEO"
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")

# Reusable OpenAI client (created once at module load)
_openai_client = None

def get_openai_client() -> OpenAI:
    """Get or create the shared OpenAI client."""
    global _openai_client
    if _openai_client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key:
            _openai_client = OpenAI(api_key=api_key)
    return _openai_client

# Product Search API configuration
PRODUCT_SEARCH_API_URL = "https://productsearch-v2.api.beslist.nl/search/products"
BASE_URL = "https://www.beslist.nl"  # Base URL for building full hyperlinks

# Mapping of mainCategory URL names to IDs (for Product Search API)
MAIN_CATEGORY_IDS = {
    "autos": 37000,
    "baby_peuter": 8,
    "boeken": 701,
    "cadeaus_gadgets_culinair": 262,
    "computers": 6,
    "dieren_accessoires": 34000,
    "gezond_mooi": 286,
    "elektronica": 655,
    "voor_volwassenen": 452,
    "eten_drinken": 11,
    "fietsen": 38000,
    "films-series": 700,
    "cddvdrom": 4,
    "horloge": 30000,
    "huishoudelijke_apparatuur": 12000,
    "kantoorartikelen": 361,
    "mode": 137,
    "klussen": 35000,
    "meubilair": 10,
    "mode_accessoires": 33000,
    "accessoires": 40000,
    "muziekinstrument": 31000,
    "parfum_aftershave": 29000,
    "main_sanitair": 27000,
    "schoenen": 32000,
    "sieraden_horloges": 347,
    "software": 155,
    "speelgoed_spelletjes": 332,
    "sport_outdoor_vrije-tijd": 206,
    "tuin_accessoires": 36000,
    "huis_tuin": 165,
}


@dataclass
class FAQItem:
    """Single FAQ question-answer pair"""
    question: str
    answer: str


@dataclass
class FAQPage:
    """Structured FAQ data for a single URL/page"""
    url: str
    page_title: str
    faqs: List[FAQItem]

    def to_schema_org(self) -> Dict:
        """Convert to Schema.org FAQPage structured data format"""
        return {
            "@context": "https://schema.org",
            "@type": "FAQPage",
            "name": self.page_title,
            "mainEntity": [
                {
                    "@type": "Question",
                    "name": faq.question,
                    "acceptedAnswer": {
                        "@type": "Answer",
                        "text": faq.answer
                    }
                }
                for faq in self.faqs
            ]
        }

    def to_json_ld(self) -> str:
        """Return JSON-LD script tag for embedding in HTML"""
        schema = self.to_schema_org()
        return f'<script type="application/ld+json">\n{json.dumps(schema, indent=2, ensure_ascii=False)}\n</script>'


# --- HTTP Session Management ---

def create_faq_session() -> requests.Session:
    """Create a requests session with retry logic and connection pooling"""
    session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    # Increased pool size for better concurrency with multiple workers
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


_faq_session = create_faq_session()


def clean_url(url: str) -> str:
    """Remove query parameters from URL"""
    return url.split("?")[0] if url else ""


# --- URL Parsing and API Parameter Building ---

def parse_beslist_url(url: str) -> Tuple[Optional[str], Optional[str], Dict[str, List[str]]]:
    """
    Parse a Beslist.nl URL and extract category and filter information.

    URL formats supported:
    - /products/{maincat}/{category}/c/{filters}  (with filters)
    - /products/{maincat}/{category}/             (without filters)
    - /products/{maincat}/c/{filters}             (top-level with filters)
    - /products/{maincat}/                        (top-level without filters)

    Filters format: facet1~value1~~facet2~value2

    Returns:
        Tuple of (main_category_name, category_urlname, filters_dict)
        filters_dict maps facet names to list of filter value IDs
    """
    # Remove domain if present
    if url.startswith("http"):
        url = "/" + url.split("/", 3)[-1]

    # Remove trailing slash for consistent parsing
    url = url.rstrip("/")

    # Pattern 1: /products/{maincat}/{category}/c/{filters}
    # or: /products/{maincat}/c/{filters}
    match = re.match(r'^/products/([^/]+)(?:/([^/]+))?/c/(.+)$', url)

    if match:
        main_category = match.group(1)
        category = match.group(2)  # May be None for top-level categories
        filters_str = match.group(3)

        # Parse filters: facet1~value1~~facet2~value2 or facet1~value1~~facet1~value2
        filters: Dict[str, List[str]] = {}
        if filters_str:
            # Split by ~~ to get individual filter pairs
            filter_pairs = filters_str.split("~~")
            for pair in filter_pairs:
                if "~" in pair:
                    facet_name, value_id = pair.split("~", 1)
                    if facet_name not in filters:
                        filters[facet_name] = []
                    filters[facet_name].append(value_id)

        return main_category, category, filters

    # Pattern 2: /products/{maincat}/{category} (without /c/ filters)
    # or: /products/{maincat}
    match = re.match(r'^/products/([^/]+)(?:/([^/]+))?$', url)

    if match:
        main_category = match.group(1)
        category = match.group(2)  # May be None for top-level categories
        return main_category, category, {}  # Empty filters dict

    return None, None, {}


def build_api_params(main_category: str, category: Optional[str], filters: Dict[str, List[str]]) -> Dict:
    """
    Build API query parameters from parsed URL components.
    """
    main_cat_id = MAIN_CATEGORY_IDS.get(main_category)
    if not main_cat_id:
        return {}

    params = {
        "mainCategory": main_cat_id,
        "sort": "popularity",
        "sortDirection": "desc",
        "limit": 76,
        "offset": 0,
        "isBot": "false",
        "countryLanguage": "nl-nl",
        "experiment": "topProducts",
        "trackTotalHits": "false",
    }

    # Add category if present
    if category:
        params["category"] = category

    # Add filters - API expects filters[facetName][index]=valueId
    for facet_name, value_ids in filters.items():
        for i, value_id in enumerate(value_ids):
            params[f"filters[{facet_name}][{i}]"] = value_id

    return params


def extract_selected_facets(api_response: Dict) -> List[Dict[str, str]]:
    """
    Extract selected facet values from API response.

    Returns list of dicts with:
    - facet_name: Name of the facet group (e.g., "Kleur", "Serie")
    - facet_value: Display value (e.g., "Geel")
    - detail_value: Value for content generation (e.g., "Gele" - Dutch adjective form)
    """
    selected = []

    facets = api_response.get("facets", [])
    for facet_group in facets:
        facet_name = facet_group.get("name", "")
        values = facet_group.get("values", [])

        for value in values:
            if value.get("selected", False):
                selected.append({
                    "facet_name": facet_name,
                    "facet_value": value.get("facetValue", ""),
                    "detail_value": value.get("detailValue", value.get("facetValue", ""))
                })

    return selected


def extract_related_plp_urls(api_response: Dict, max_urls: int = 15) -> List[Dict[str, str]]:
    """
    Extract related PLP URLs from facets for use in FAQ answers.
    Uses the URL provided directly in the API response.

    Returns list of dicts with:
    - url: Full URL to the PLP
    - label: Display name for the link
    - facet_type: Type of facet (e.g., "merk", "kleur", "type")
    """
    related_urls = []

    facets = api_response.get("facets", [])

    # Priority facet types for linking (most useful for FAQ answers)
    priority_facets = ["merk", "type", "serie", "kleur", "materiaal", "doelgroep"]

    for facet_group in facets:
        facet_name = facet_group.get("name", "")
        values = facet_group.get("values", [])

        # Skip already selected facets - we want to link to related pages
        for value in values:
            if value.get("selected", False):
                continue

            count = value.get("count", 0)
            if count < 5:  # Skip facets with very few products
                continue

            facet_value = value.get("facetValue", "")
            # Get URL directly from API response
            plp_url = value.get("url", "")

            if not plp_url or not facet_value:
                continue

            # Make it a full URL if it's relative
            if plp_url.startswith("/"):
                full_url = f"{BASE_URL}{plp_url}"
            elif not plp_url.startswith("http"):
                full_url = f"{BASE_URL}/{plp_url}"
            else:
                full_url = plp_url

            related_urls.append({
                "url": full_url,
                "label": facet_value,
                "facet_type": facet_name.lower(),
                "count": count
            })

    # Sort by priority facet types, then by product count
    def sort_key(item):
        facet_lower = item["facet_type"].lower()
        priority = 999
        for i, pf in enumerate(priority_facets):
            if pf in facet_lower:
                priority = i
                break
        return (priority, -item["count"])

    related_urls.sort(key=sort_key)

    return related_urls[:max_urls]


def build_product_subject(selected_facets: List[Dict[str, str]], category_name: str = "") -> str:
    """
    Build a product subject/name from selected facet values.
    """
    if not selected_facets:
        return category_name

    # Categorize facets by type
    colors = []
    materials = []
    product_names = []
    brands = []
    target_groups = []
    other = []

    product_name_facets = {"serie", "modelnaam", "modelnaam_mob", "model"}
    product_type_facets = {"type", "type_koffiezetter", "t_klimplantrek"}
    color_facets = {"kleur", "kleurtint", "kleurtint_paars", "kleurtint_blauw", "kleurtint_groen"}
    material_facets = {"materiaal"}
    target_group_facets = {"doelgroep", "doelgroep_schoenen", "doelgroep_mode"}
    brand_facets = {"merk"}

    has_specific_product = False

    for facet in selected_facets:
        facet_name_lower = facet["facet_name"].lower()
        detail_value = facet["detail_value"]

        if any(c in facet_name_lower for c in color_facets):
            colors.append(detail_value)
        elif any(m in facet_name_lower for m in material_facets):
            materials.append(detail_value)
        elif any(p in facet_name_lower for p in product_name_facets):
            product_names.append(detail_value)
            has_specific_product = True
        elif any(t in facet_name_lower for t in product_type_facets):
            product_names.append(detail_value)
            has_specific_product = True
        elif any(t in facet_name_lower for t in target_group_facets):
            target_groups.append(detail_value)
        elif any(b in facet_name_lower for b in brand_facets):
            brands.append(detail_value)
        else:
            other.append(detail_value)

    parts = colors + materials + product_names + brands + target_groups + other

    needs_category = (
        not has_specific_product and
        category_name and
        len(parts) > 0
    )

    if needs_category:
        parts.append(category_name.lower())

    return " ".join(parts)


# --- Product Search API ---

def fetch_products_api(url: str) -> Optional[Dict]:
    """
    Fetch product data from the Product Search API.

    Returns:
        Dict with:
        - url: Original URL
        - h1_title: Page title (from category)
        - product_subject: Subject built from selected facets
        - products: List of products with title and description
        - error: (optional) Error reason if API call failed
    """
    try:
        clean = clean_url(url)

        # Parse URL
        main_category, category, filters = parse_beslist_url(clean)

        if not main_category:
            print(f"[FAQ-API] Could not parse URL: {clean}")
            return None

        # Build API parameters
        params = build_api_params(main_category, category, filters)

        if not params:
            print(f"[FAQ-API] Unknown main category: {main_category}")
            return None

        # Removed artificial delay for faster processing
        # (connection pooling and retry logic handle rate limiting)

        # Make API request
        headers = {
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }

        response = _faq_session.get(PRODUCT_SEARCH_API_URL, params=params, headers=headers, timeout=30)

        if response.status_code != 200:
            print(f"[FAQ-API] Request failed with status {response.status_code} for {clean}")
            # Check if it's a facet validation error (400 with specific error message)
            if response.status_code == 400:
                try:
                    error_data = response.json()
                    errors = error_data.get("errors", [])
                    if isinstance(errors, list):
                        for err in errors:
                            error_info = err.get("errorInfo", "")
                            # Check for "facet is not valid" or "facet value is not valid"
                            if "not valid" in error_info:
                                context = err.get("context", "unknown")
                                value = err.get("value", "unknown")
                                print(f"[FAQ-API] Invalid facet/value: context='{context}', value='{value}' for {clean}")
                                return {"error": "facet_not_available", "invalid_facet": f"{context}:{value}"}
                except Exception:
                    pass
            return {"error": "api_failed"}

        data = response.json()

        # Extract selected facets
        selected_facets = extract_selected_facets(data)

        # Extract related PLP URLs for hyperlinks in FAQ answers
        related_plp_urls = extract_related_plp_urls(data)

        # Get category name from CSV lookup (preferred) or fall back to API product data
        csv_maincat, csv_deepest = lookup_category(main_category, category)
        if csv_deepest:
            deepest_category_name = csv_deepest
        else:
            categories = data.get("products", [{}])[0].get("categories", []) if data.get("products") else []
            sub_path = category[len(main_category):] if category and main_category and category != main_category else ""
            url_depth = len([s for s in sub_path.split('_') if s.isdigit()])
            if categories and url_depth < len(categories):
                deepest_category_name = categories[url_depth].get("name", "")
            elif categories:
                deepest_category_name = categories[-1].get("name", "")
            else:
                deepest_category_name = ""

        # Build product subject from selected facets
        product_subject = build_product_subject(selected_facets, deepest_category_name)

        # Extract products and their URLs
        products = []
        product_urls = []
        api_products = data.get("products", [])[:30]  # Limit for FAQ generation

        for product in api_products:
            # Skip orResult products - only include exact matches (type="result")
            product_type = product.get("type", "")
            if product_type == "orResult":
                continue

            title = product.get("title", product.get("description", ""))[:100]
            description = product.get("description", title)[:200]
            plp_url = product.get("plpUrl", "")
            shop_count = product.get("shopCount", 0)

            if title:
                products.append({
                    "title": title,
                    "description": description
                })

            # Extract product URLs (/p/ URLs) for FAQ hyperlinks
            # Only include products with at least 2 offers (shopCount >= 2)
            if plp_url and "/p/" in plp_url and shop_count >= 2:
                # Make it a full URL if relative
                if plp_url.startswith("/"):
                    full_url = f"{BASE_URL}{plp_url}"
                else:
                    full_url = plp_url
                # Avoid duplicates
                if not any(p["url"] == full_url for p in product_urls):
                    product_urls.append({
                        "url": full_url,
                        "label": title[:50] if title else "Product"
                    })

        # Use product_subject as title if available, otherwise use category
        h1_title = product_subject if product_subject else deepest_category_name if deepest_category_name else main_category

        return {
            "url": clean,
            "h1_title": h1_title,
            "category_name": deepest_category_name,
            "products": products,
            "selected_facets": selected_facets,
            "product_urls": product_urls[:15]  # Limit to 15 product URLs for FAQ hyperlinks
        }

    except requests.RequestException as e:
        print(f"[FAQ-API] Request error for {url}: {str(e)}")
        return None
    except Exception as e:
        print(f"[FAQ-API] Error for {url}: {str(e)}")
        return None


# --- FAQ Generation ---

def generate_faqs_for_page(page_data: Dict, num_faqs: int = 5) -> Optional[FAQPage]:
    """
    Generate FAQ content for a page using AI.

    Args:
        page_data: Dict from fetch_products_api()
        num_faqs: Number of FAQ items to generate

    Returns:
        FAQPage object with structured FAQ data
    """
    client = get_openai_client()
    if not client:
        print("[FAQ] Error: OPENAI_API_KEY environment variable not set")
        return None

    # Build context from products
    products_context = ""
    if page_data.get("products"):
        products_list = "\n".join([
            f"- {p['title']}: {p['description']}"
            for p in page_data["products"][:15]
        ])
        products_context = f"\n\nBeschikbare producten:\n{products_list}"

    # Build context for product URLs (for hyperlinks in FAQ answers)
    product_urls_context = ""
    if page_data.get("product_urls"):
        urls_list = "\n".join([
            f"- {item['label']}: {item['url']}"
            for item in page_data["product_urls"][:12]
        ])
        product_urls_context = f"\n\nProductpagina's (gebruik deze voor hyperlinks in antwoorden):\n{urls_list}"

    # Build context from selected facets so the AI writes facet-specific FAQs
    facet_context = ""
    facet_instruction = ""
    if page_data.get("selected_facets"):
        facets = page_data["selected_facets"]
        facet_descriptions = [f"{f['facet_name']}: {f['facet_value']}" for f in facets]
        facet_context = f"\n\nActieve filters op deze pagina:\n" + "\n".join(f"- {d}" for d in facet_descriptions)
        facet_instruction = "\n- BELANGRIJK: Deze pagina is gefilterd op specifieke kenmerken (zie \"Actieve filters\" hierboven). Maak de vragen en antwoorden specifiek over die filters. Als er gefilterd is op een merk, stel dan vragen over dat merk en hun producten. Als er gefilterd is op een kleur, materiaal of type, stel dan vragen die specifiek over die eigenschap gaan. Schrijf GEEN generieke vragen die net zo goed op de ongefilterde categoriepagina zouden passen."

    prompt = f"""Je bent een SEO-expert die FAQ's schrijft voor e-commerce pagina's.

Pagina titel: {page_data['h1_title']}
URL: {page_data['url']}
{facet_context}
{products_context}
{product_urls_context}

Schrijf {num_faqs} veelgestelde vragen (FAQ's) die relevant zijn voor bezoekers van deze productcategorie pagina.

Vereisten:
- Vragen moeten natuurlijk klinken, zoals echte klanten ze zouden stellen
- Antwoorden moeten informatief en behulpzaam zijn (50-100 woorden per antwoord)
- Focus op koopadvies, productvergelijkingen, en praktische tips{facet_instruction}
- Schrijf in het Nederlands
- Noem geen specifieke prijzen
- BELANGRIJK: Gebruik een informele, toegankelijke toon. Gebruik "jij" en "je" in plaats van "u" en "uw". Spreek de lezer direct en vriendelijk aan.
- BELANGRIJK: Gebruik NOOIT "wij", "we", "ons", "onze", "onze producten", "onze website" of vergelijkbare eerste persoon meervoud. Schrijf neutraal en informatief, alsof je een onafhankelijke adviseur bent.
- BELANGRIJK voor hyperlinks:
  * Gebruik ALLEEN URLs uit de hierboven gegeven lijst "Productpagina's" (URLs met /p/)
  * Verzin NOOIT zelf URLs - gebruik alleen de exacte URLs die in de lijst staan
  * Gebruik GEEN URLs met /c/ (categoriepagina's) - alleen productpagina URLs met /p/
  * Gebruik GEEN generieke verwijzingen zoals "deze gids", "deze pagina", "hier" of vergelijkbare vage linkteksten
  * Linktekst moet beschrijvend zijn en verwijzen naar het specifieke product
  * HOUD DE LINKTEKST KORT (max 3-5 woorden). Vermijd lange productnamen met specificaties. Bijvoorbeeld: "Beeztees kattentuigje Hearts" in plaats van "Beeztees kattentuigje Hearts zwart 120 x 1 cm"
  * Als er geen relevante URL in de lijst staat, maak dan GEEN hyperlink
- Verwerk 1-3 hyperlinks per antwoord waar relevant (naar specifieke producten)

Geef je antwoord als JSON array met objecten die "question" en "answer" bevatten.
De "answer" mag HTML hyperlinks bevatten.
Alleen de JSON array, geen andere tekst.

Voorbeeld formaat (let op: URLs moeten EXACT uit de lijst komen, formaat /p/productnaam/category_id/pim_id/):
[
  {{"question": "Welke merken zijn populair?", "answer": "Populaire merken zijn onder andere <a href=\"https://www.beslist.nl/p/samsung-galaxy-s24/6/1234567890123/\">Samsung Galaxy S24</a>. Dit model staat bekend om zijn kwaliteit."}},
  {{"question": "Andere vraag?", "answer": "Een ander goed product is de <a href=\"https://www.beslist.nl/p/philips-airfryer/12000/9876543210987/\">Philips Airfryer</a>."}}
]"""

    try:
        response = client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2500,  # Increased for hyperlinks in answers
            temperature=0.7
        )

        content = response.choices[0].message.content.strip()

        # Parse JSON response
        # Handle potential markdown code blocks
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        content = content.strip()

        faqs_data = json.loads(content)

        # Validate and clean URLs in answers - remove any fabricated URLs
        def clean_urls_in_answer(answer: str, valid_urls: list) -> str:
            import re

            # Extract all href URLs from the answer
            href_pattern = r'<a\s+href="([^"]+)"[^>]*>([^<]+)</a>'

            def replace_invalid_link(match):
                url = match.group(1)
                link_text = match.group(2)

                # Check if URL is valid (must be /p/ format and in our list)
                is_valid = False
                if "/p/" in url and "/products/" not in url and "/c/" not in url:
                    # Check if it matches any of our provided URLs
                    for valid_url in valid_urls:
                        if valid_url in url or url in valid_url:
                            is_valid = True
                            break

                if is_valid:
                    return match.group(0)  # Keep valid link
                else:
                    return link_text  # Remove invalid link, keep text only

            return re.sub(href_pattern, replace_invalid_link, answer)

        # Get list of valid URLs from page_data
        valid_urls = [item["url"] for item in page_data.get("product_urls", [])]

        faq_items = [
            FAQItem(question=item["question"], answer=clean_urls_in_answer(item["answer"], valid_urls))
            for item in faqs_data
        ]

        return FAQPage(
            url=page_data["url"],
            page_title=page_data["h1_title"],
            faqs=faq_items
        )

    except json.JSONDecodeError as e:
        print(f"[FAQ] Failed to parse AI response as JSON: {e}")
        return None
    except Exception as e:
        print(f"[FAQ] AI generation error: {e}")
        return None


def process_single_url_faq(url: str, num_faqs: int = 5) -> Dict:
    """
    Process a single URL for FAQ generation.

    Returns:
        Dict with status, url, and optionally faq_page data
    """
    result = {"url": url, "status": "pending"}

    try:
        # Fetch product data via API
        page_data = fetch_products_api(url)

        if not page_data:
            result["status"] = "failed"
            result["reason"] = "api_failed"
            return result

        # Check if the API returned an error dict
        if page_data.get("error"):
            result["status"] = "failed"
            result["reason"] = page_data["error"]
            if page_data.get("invalid_facet"):
                result["invalid_facet"] = page_data["invalid_facet"]
            return result

        if not page_data.get("products") or len(page_data["products"]) == 0:
            result["status"] = "skipped"
            result["reason"] = "no_products_found"
            return result

        # Generate FAQs
        faq_page = generate_faqs_for_page(page_data, num_faqs)

        if not faq_page or not faq_page.faqs:
            result["status"] = "failed"
            result["reason"] = "faq_generation_failed"
            return result

        result["status"] = "success"
        result["page_title"] = faq_page.page_title
        result["faq_json"] = json.dumps([asdict(faq) for faq in faq_page.faqs], ensure_ascii=False)
        result["schema_org"] = json.dumps(faq_page.to_schema_org(), ensure_ascii=False)
        result["faq_count"] = len(faq_page.faqs)

        return result

    except Exception as e:
        result["status"] = "failed"
        result["reason"] = f"error: {str(e)}"
        return result
