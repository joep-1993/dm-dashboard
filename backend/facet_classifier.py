"""
Facet Classifier

Classifies whether a facet is a "type-facet" — meaning its values inherently
carry the product type, making the page's category name redundant in titles.
Examples: Soort=Dahliabollen in bloembollen zaden, Type=Wandplaten in wanddecoratie.

Classification is keyed on (facet_name, category_name) because the productsearch
API returns generic labels ("Soort", "Type") that mean different things across
categories — "Soort=Bomberjacks" in Jacks IS a product type, but "Soort=Met
mouwen" in Slabbetjes is not.
"""
import json
import os
from typing import Optional

from backend.database import get_db_connection, return_db_connection

AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")

# Pre-seed: known type-facets keyed on (facet_name, category_name).
PRESEED_TYPE_FACETS = {
    ('t_wanddeco', 'wanddecoratie'): 'wandplaten',
    ('soort_bz', 'bloembollen zaden'): 'dahliabollen',
}


def init_facet_classifications_table():
    """Create the facet_type_classifications table and pre-seed known cases."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pa.facet_type_classifications (
                facet_name VARCHAR(100) NOT NULL,
                sample_category TEXT NOT NULL DEFAULT '',
                is_type_facet BOOLEAN NOT NULL,
                sample_value TEXT,
                reasoning TEXT,
                classified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (facet_name, sample_category)
            )
        """)
        # Migrate existing single-column PK to composite if needed.
        cur.execute("""
            SELECT conname FROM pg_constraint
            WHERE conrelid = 'pa.facet_type_classifications'::regclass
              AND contype = 'p'
        """)
        pk = cur.fetchone()
        if pk:
            cur.execute("""
                SELECT a.attname
                FROM pg_constraint c
                JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
                WHERE c.conname = %s
                ORDER BY array_position(c.conkey, a.attnum)
            """, (pk['conname'],))
            cols = [r['attname'] for r in cur.fetchall()]
            if cols == ['facet_name']:
                cur.execute("""
                    UPDATE pa.facet_type_classifications
                       SET sample_category = COALESCE(LOWER(TRIM(sample_category)), '')
                     WHERE sample_category IS NULL OR sample_category <> LOWER(TRIM(sample_category))
                """)
                cur.execute("ALTER TABLE pa.facet_type_classifications ALTER COLUMN sample_category SET NOT NULL")
                cur.execute("ALTER TABLE pa.facet_type_classifications ALTER COLUMN sample_category SET DEFAULT ''")
                cur.execute(f"ALTER TABLE pa.facet_type_classifications DROP CONSTRAINT {pk['conname']}")
                cur.execute("ALTER TABLE pa.facet_type_classifications ADD PRIMARY KEY (facet_name, sample_category)")
                print("[FACET_CLASSIFIER] Migrated PK to (facet_name, sample_category)")
        for (fname, cat), sample in PRESEED_TYPE_FACETS.items():
            cur.execute("""
                INSERT INTO pa.facet_type_classifications
                    (facet_name, sample_category, is_type_facet, sample_value, reasoning)
                VALUES (%s, %s, TRUE, %s, 'pre-seeded')
                ON CONFLICT (facet_name, sample_category) DO NOTHING
            """, (fname, cat, sample))
        conn.commit()
        print("[FACET_CLASSIFIER] Table initialized")
    finally:
        cur.close()
        return_db_connection(conn)


def _lookup(facet_name: str, category_name: str) -> Optional[bool]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT is_type_facet FROM pa.facet_type_classifications "
            "WHERE facet_name = %s AND sample_category = %s",
            (facet_name, category_name),
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
                (facet_name, sample_category, is_type_facet, sample_value, reasoning)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (facet_name, sample_category) DO NOTHING
        """, (facet_name, sample_category, is_type, sample_value, reasoning))
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


_MEM_CACHE: dict = {}


def classify_facet(facet_name: str, sample_value: str = "",
                   category_name: str = "") -> bool:
    """
    Return True if this facet is a type-facet (its values are the product type).
    Caches in-process and in DB. LLM is consulted at most once per
    (facet_name, category_name) pair.
    """
    fname = facet_name.lower().strip()
    cat = (category_name or "").lower().strip()
    if not fname:
        return False

    key = (fname, cat)
    if key in _MEM_CACHE:
        return _MEM_CACHE[key]

    cached = _lookup(fname, cat)
    if cached is not None:
        _MEM_CACHE[key] = cached
        return cached

    is_type, reason = _classify_with_llm(fname, sample_value, category_name)
    _persist(fname, is_type, sample_value, cat, reason)
    _MEM_CACHE[key] = is_type
    print(f"[FACET_CLASSIFIER] Classified '{fname}' in '{cat}' as is_type={is_type}: {reason}")
    return is_type
