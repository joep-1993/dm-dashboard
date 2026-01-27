import os
from openai import OpenAI
from typing import List, Dict

# Initialize client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Model selection (configure in .env)
MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")

def simple_completion(prompt: str, max_tokens: int = 1000) -> str:
    """
    Simple AI completion for small apps.
    Supports multiple models via AI_MODEL env var.
    """
    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=max_tokens,
        temperature=0.7
    )
    return response.choices[0].message.content

def structured_chat(messages: list, max_tokens: int = 1000) -> str:
    """
    For more complex conversations with context.
    """
    response = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        max_tokens=max_tokens,
        temperature=0.7
    )
    return response.choices[0].message.content

def create_product_recommendation_prompt(h1_title: str, products: List[Dict]) -> str:
    """
    Create the prompt for product recommendation content generation.
    Matches the n8n workflow prompt structure.
    Optimized: limits to 30 products and truncates descriptions to 200 chars to reduce tokens.
    """
    # Limit to 30 products max for faster AI processing
    limited_products = products[:30]

    products_text = "\n".join([
        f"Product {i + 1}\nTitle: {p['title']}\nUrl: {p['url']}\nContent: {p['listviewContent'][:200]}\n"  # Truncate to 200 chars
        for i, p in enumerate(limited_products)
    ])

    prompt = f"""Opdracht
Een prijsbewuste consumenten landt op een pagina na het zoeken in Google. Op de pagina staan veel producten waaruit hij moet kiezen. Zie de lijst met de 30 populairste producten hieronder.
Schrijf een korte tekst (max. 100 woorden) met als doel om de bezoeker te helpen de juiste keuze te maken.
- Schrijf de tekst als EEN doorlopende alinea, GEEN meerdere paragrafen of witregels.
- Geef concreet advies: noem bijvoorbeeld verschillen in functies, eigenschappen of gebruiksscenario's
- Vermijd het noemen van prijzen.
- Gebruik waar relevant, klikbare links naar producten en gebruik hierbij HTML-links met de tag <a href="url"> en als linktekst een KORTE, heldere omschrijving (max 3-5 woorden). Maak bijvoorbeeld van "Beeztees kattentuigje Hearts zwart 120 x 1 cm" gewoon "Beeztees kattentuigje Hearts". Gebruik alleen "urls" die hieronder in deze lijst voorkomen en negeer urls met een lege waarde.

Hieronder de context:
Zoekwoord in Google: {h1_title}

De 30 populairste producten met titel, prijs en de bijbehorede link:
{products_text}
"""
    return prompt

def generate_product_content(h1_title: str, products: List[Dict]) -> str:
    """
    Generate product recommendation content using OpenAI.
    Uses the system message and user prompt from n8n workflow.
    """
    user_prompt = create_product_recommendation_prompt(h1_title, products)

    system_message = """Je bent een online marketeer voor beslist.nl met als doel om de bezoeker te helpen in zijn buyer journey.
- Spreek de lezer aan met "je," in een toegankelijke, optimistische toon.
- Noem nooit prijzen.
- Schrijf ALTIJD als één doorlopende alinea zonder witregels of meerdere paragrafen.
- Focus op advies dat écht helpt bij het maken van een keuze (bv. voordelen, verschillen, specifieke kenmerken).
- Als het zoekwoord een merknaam bevat, zijn ALLE producten van dat merk. Link alleen naar producten uit de lijst - deze zijn al gefilterd op het juiste merk.
- Als je linkt gebruikt, gebruik de tag <a href> en kies dan de juiste url uit de lijst van meegeleverde producten. Maak nooit zelf een andere url en negeer urls met waarde [empty]
- Als je een link maakt: HOUD DE LINKTEKST KORT (max 3-5 woorden). Zorg dat de linktekst verwijst naar het correcte product, maar vermijd lange productnamen met specificaties. Bijvoorbeeld: "Beeztees kattentuigje Hearts" in plaats van "Beeztees kattentuigje Hearts zwart 120 x 1 cm".
- We moeten voorkomen dat de link tekst niet overeenkomt met de url.
- Gebruik nooit andere URLs dan degene die voorkomen in de lijst van producten."""

    messages = [
        {"role": "system", "content": system_message},
        {"role": "user", "content": user_prompt}
    ]

    response = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        max_tokens=2000,  # Increased to accommodate content with multiple product URLs
        temperature=0.7
    )

    content = response.choices[0].message.content

    # Check if response was truncated
    if response.choices[0].finish_reason == "length":
        print(f"[GPT] Warning: Response was truncated for '{h1_title}'")

    # Fix truncated URLs (GPT sometimes removes maincat_id/pimId from URLs)
    content = fix_truncated_urls(content, products)

    return content

def check_content_has_valid_links(content: str) -> bool:
    """
    Check if generated content contains valid product links.
    Returns True if content has <a href="/p/ or <a href="https://www.beslist.nl/p/ pattern.
    """
    return '<a href="/p/' in content or '<a href="https://www.beslist.nl/p/' in content


def fix_truncated_urls(content: str, products: List[Dict]) -> str:
    """
    Fix GPT-truncated URLs by matching them against the original product URLs.

    GPT sometimes truncates URLs like:
      https://www.beslist.nl/p/product-name/452/8718969401258/
    to just:
      https://www.beslist.nl/p/product-name/

    This function finds truncated URLs and replaces them with the correct full URLs.
    """
    import re

    # Build a mapping of product-name slug to full URL
    slug_to_url = {}
    for p in products:
        url = p.get('url', '')
        if url and '/p/' in url:
            # Extract the product-name slug from the URL
            # URL format: https://www.beslist.nl/p/product-name/maincat/pimId/
            match = re.search(r'/p/([^/]+)/', url)
            if match:
                slug = match.group(1)
                slug_to_url[slug] = url

    # Find all href URLs in content
    def replace_truncated(match):
        href = match.group(1)
        # Check if this is a truncated URL (only has /p/product-name/ pattern)
        truncated_match = re.match(r'https://www\.beslist\.nl/p/([^/]+)/?$', href)
        if truncated_match:
            slug = truncated_match.group(1)
            if slug in slug_to_url:
                return f'href="{slug_to_url[slug]}"'
        return match.group(0)

    # Replace truncated URLs
    fixed_content = re.sub(r'href="(https://www\.beslist\.nl/p/[^"]+)"', replace_truncated, content)

    # Count fixes
    if fixed_content != content:
        original_truncated = len(re.findall(r'href="https://www\.beslist\.nl/p/[^/]+/?"', content))
        remaining_truncated = len(re.findall(r'href="https://www\.beslist\.nl/p/[^/]+/?"', fixed_content))
        fixes = original_truncated - remaining_truncated
        if fixes > 0:
            print(f"[GPT] Fixed {fixes} truncated URL(s) in content")

    return fixed_content

# Test function
if __name__ == "__main__":
    print(f"Testing {MODEL}...")
    result = simple_completion("Say 'Hello from the AI model!'")
    print(result)
