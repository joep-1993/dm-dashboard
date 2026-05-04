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
        "Bepaal of dit een 'type-facet' is.\n\n"
        "DEFINITIE: een type-facet is een facet waarvan de waarde een ZELFSTANDIG NAAMWOORD "
        "is dat op zichzelf een herkenbaar producttype benoemt — een woord dat je in een titel "
        "kunt zetten zonder de categorienaam erbij. Het maakt niet uit dat de categorienaam "
        "een breder begrip is; juist dán is het stripen van de categorie zinvol, omdat de "
        "facet-waarde specifieker is en al duidelijk maakt om welk soort product het gaat. "
        "Een subtype dat een eigen naam heeft (bv. 'Broodplanken' binnen 'Snijplanken', "
        "'Dahliabollen' binnen 'Bloembollen', 'Wandplaten' binnen 'Wanddecoratie', "
        "'Sportscooters' binnen 'Scooters') telt dus WEL als type-facet.\n\n"
        "Een facet is GEEN type-facet als de waarden:\n"
        "- eigenschappen of kenmerken zijn (kleur, maat, materiaal, doelgroep, vermogen, "
        "  schermgrootte, energielabel)\n"
        "- bijvoeglijke beschrijvingen zijn die zonder de categorienaam onbegrijpelijk worden "
        "  ('Met mouwen', 'Rood', 'XL', 'Verduisterende', 'Losse', 'H1')\n"
        "- merken of merklijnen zijn ('Sony', 'Magic Wand', 'Big Jelly', 'VTech Toet Toet')\n"
        "- losse onderdelen of accessoires van het hoofdproduct ('Remblokken' binnen "
        "  'Remsystemen', 'Scooteruitlaten' binnen 'Scooteronderdelen')\n\n"
        f"Facet naam: {facet_name}\n"
        f"Voorbeeld facet-waarde: {sample_value}\n"
        f"Categorienaam van de pagina: {category_name}\n\n"
        "Stel jezelf de vraag: kan de facet-waarde alleen — zonder de categorienaam erachter — "
        "in een paginatitel staan en duidelijk maken welk product het is? Zo ja → true. "
        "Moet de categorienaam erbij om de waarde te begrijpen? Zo nee → false.\n\n"
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


# Facets that are NEVER type-facets in any category — hardcoded policy override.
# Reason: their values are usually adjectival/brand-line descriptors that need the
# category name to make sense in a title.
_NEVER_TYPE_FACETS = {
    'type productlijn',  # almost always a brand-tied series adjective (e.g. "Compact")
}


_MEM_CACHE: dict = {}


def batch_classify_facets(facets: list, category_name: str) -> dict:
    """
    Look up is_type_facet for every facet in a single DB round-trip.

    Same semantics as calling classify_facet() per facet, but resolves cache hits
    in memory and merges the remaining lookups into one query keyed on
    (facet_name = ANY(%s), sample_category = %s). For facets still uncached after
    the DB hit, falls through to per-facet LLM classification (rare in practice
    once preclassification has run).

    Args:
        facets: list of dicts with 'facet_name' and 'detail_value' (as returned
                by extract_selected_facets).
        category_name: category context for the lookup.

    Returns:
        dict mapping facet_name (lowercased, trimmed) -> is_type_facet (bool).
    """
    if not facets:
        return {}
    cat = (category_name or "").lower().strip()
    norm_pairs = []  # (fname_lower, sample_value)
    seen = set()
    for f in facets:
        fname = (f.get('facet_name') or '').lower().strip()
        if not fname or fname in seen:
            continue
        seen.add(fname)
        norm_pairs.append((fname, f.get('detail_value', '')))

    result: dict = {}
    missing = []
    for fname, _sample in norm_pairs:
        if fname in _NEVER_TYPE_FACETS:
            result[fname] = False
            continue
        ck = (fname, cat)
        if ck in _MEM_CACHE:
            result[fname] = _MEM_CACHE[ck]
        else:
            missing.append(fname)

    if missing:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT facet_name, is_type_facet FROM pa.facet_type_classifications "
                "WHERE sample_category = %s AND facet_name = ANY(%s)",
                (cat, missing),
            )
            for row in cur.fetchall():
                fn = row['facet_name']
                v = bool(row['is_type_facet'])
                result[fn] = v
                _MEM_CACHE[(fn, cat)] = v
        finally:
            cur.close()
            return_db_connection(conn)

    # For any facet still not classified, fall through to LLM-backed classify_facet.
    sample_by_fname = dict(norm_pairs)
    for fname in (fn for fn, _ in norm_pairs):
        if fname not in result:
            result[fname] = classify_facet(fname, sample_by_fname.get(fname, ''), category_name)
    return result


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

    # Hardcoded policy override: certain facets are never type-facets.
    if fname in _NEVER_TYPE_FACETS:
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
