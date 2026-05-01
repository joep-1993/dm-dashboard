"""
Facet Classifier

Classifies whether a facet (by name) is a "type-facet" — meaning its values
inherently carry the product type, making the page's category name redundant
in titles. Examples: soort_bz (Dahliabollen), t_wanddeco (Wandplaten).

Classification is keyed on facet_name and cached in pa.facet_type_classifications,
so the LLM is consulted at most once per facet name across the entire system.
"""
import json
import os
from functools import lru_cache
from typing import Optional

from backend.database import get_db_connection, return_db_connection

AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")

# Pre-seed: known type-facets (override goes here too if AI gets one wrong).
PRESEED_TYPE_FACETS = {
    't_wanddeco': 'wandplaten',
    'soort_bz': 'dahliabollen',
}


def init_facet_classifications_table():
    """Create the facet_type_classifications table and pre-seed known cases."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pa.facet_type_classifications (
                facet_name VARCHAR(100) PRIMARY KEY,
                is_type_facet BOOLEAN NOT NULL,
                sample_value TEXT,
                sample_category TEXT,
                reasoning TEXT,
                classified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        for fname, sample in PRESEED_TYPE_FACETS.items():
            cur.execute("""
                INSERT INTO pa.facet_type_classifications
                    (facet_name, is_type_facet, sample_value, reasoning)
                VALUES (%s, TRUE, %s, 'pre-seeded')
                ON CONFLICT (facet_name) DO NOTHING
            """, (fname, sample))
        conn.commit()
        print("[FACET_CLASSIFIER] Table initialized")
    finally:
        cur.close()
        return_db_connection(conn)


def _lookup(facet_name: str) -> Optional[bool]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT is_type_facet FROM pa.facet_type_classifications WHERE facet_name = %s",
            (facet_name,),
        )
        row = cur.fetchone()
        return row['is_type_facet'] if row else None
    finally:
        cur.close()
        return_db_connection(conn)


def _persist(facet_name: str, is_type: bool, sample_value: str,
             sample_category: str, reasoning: str) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO pa.facet_type_classifications
                (facet_name, is_type_facet, sample_value, sample_category, reasoning)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (facet_name) DO NOTHING
        """, (facet_name, is_type, sample_value, sample_category, reasoning))
        conn.commit()
    finally:
        cur.close()
        return_db_connection(conn)


def _classify_with_llm(facet_name: str, sample_value: str,
                       category_name: str) -> tuple[bool, str]:
    """Ask the LLM whether this facet is a type-facet. Returns (is_type, reason)."""
    from backend.ai_titles_service import get_openai_client
    client = get_openai_client()
    if not client:
        return False, "no openai client; defaulting to non-type-facet"

    prompt = (
        "Je krijgt een facet van een productpagina op een Nederlandse e-commerce site. "
        "Bepaal of dit een 'type-facet' is: een facet waarvan de waarde zelf het producttype is, "
        "zodat de categorienaam in een paginatitel overbodig zou zijn (een dubbeling).\n\n"
        f"Facet naam: {facet_name}\n"
        f"Voorbeeld facet-waarde: {sample_value}\n"
        f"Categorienaam van de pagina: {category_name}\n\n"
        "Voorbeelden van WEL type-facets: 'soort_bz' (waarde 'Dahliabollen' in categorie 'bloembollen zaden'), "
        "'t_wanddeco' (waarde 'Wandplaten' in categorie 'wanddecoratie'). De facet-waarde IS het product.\n"
        "Voorbeelden van GEEN type-facets: 'merk' (Sony), 'kleur' (Rood), 'maat' (XL), 'materiaal' (Hout). "
        "Deze beschrijven een eigenschap, niet het producttype zelf.\n\n"
        'Antwoord met JSON: {"is_type_facet": true|false, "reason": "korte uitleg"}'
    )

    try:
        resp = client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0,
        )
        data = json.loads(resp.choices[0].message.content)
        return bool(data.get("is_type_facet", False)), str(data.get("reason", ""))[:500]
    except Exception as e:
        print(f"[FACET_CLASSIFIER] LLM call failed for '{facet_name}': {e}")
        return False, f"llm error: {e}"


@lru_cache(maxsize=1024)
def classify_facet(facet_name: str, sample_value: str = "",
                   category_name: str = "") -> bool:
    """
    Return True if this facet is a type-facet (its values are the product type).
    Caches in-process and in DB. LLM is consulted at most once per facet_name.
    """
    fname = facet_name.lower().strip()
    if not fname:
        return False

    cached = _lookup(fname)
    if cached is not None:
        return cached

    is_type, reason = _classify_with_llm(fname, sample_value, category_name)
    _persist(fname, is_type, sample_value, category_name, reason)
    print(f"[FACET_CLASSIFIER] Classified '{fname}' as is_type={is_type}: {reason}")
    return is_type
