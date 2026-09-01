#!/usr/bin/env python3
#!/usr/bin/env python3
"""Stap 6: alles platslaan tot één JSON-contractbestand (gratis).

Leest wat de vorige stappen hebben opgeleverd (termen, producten, kliks,
reviews, ranglijsten, prijzen, aanbiedingen) en schrijft één zelfstandig
bestand dat een paginabouwer kan consumeren. Praat met niets: geen LLM, geen
netwerk, dus veilig om opnieuw te draaien — bijvoorbeeld na verse prijzen.

De markdown-parser hieronder komt ongewijzigd uit de oorspronkelijke skill: hij
vangt vijf verschillende manieren op waarop het model zijn bronnen opschrijft,
en dat is met echte reviewtekst uitgevochten. Alleen de in- en uitvoer is
topic-gestuurd gemaakt.

    python top10/scripts/export_top10_data.py --topic airfryers
"""
import argparse, csv, datetime as dt, json, re, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_reviews import PROMPT  # noqa: E402
from rank_top10 import SYSTEM  # noqa: E402
from shared.llm_websearch import cost_for  # noqa: E402
from shared.topic import add_topic_arg, find_topic, slugify as term_slug  # noqa: E402

DATA = None        # door main() op het gekozen topic gezet
OUT_PATH = None

CITATION_RE = re.compile(r"\(\[[^\]]*\]\([^)]*\)\)|\(https?://\S+\)")
SECTION_RE = re.compile(r"^\s*#{2,3}\s+(.*?)\s*$")
WEL_NIET_RE = re.compile(r"\**\s*Wel:?\**\s*(.*?)\s*\**\s*Niet:?\**\s*(.*)", re.S | re.IGNORECASE)
QA_RE = re.compile(r"\*\*(.+?)\*\*\s*(.*?)(?=\*\*|\Z)", re.S)
MD_LINK_RE = re.compile(r"\[(.*?)\]\((https?://\S+)\)")
FENCE_RE = re.compile(r"^```\w*$")
TRAILING_SELF_CITATION_RE = re.compile(r"\s*\(\[[^\]]*\]\([^)]*\)\)\s*$")

HEADINGS = [
    "Oordeel in één zin",
    "Pluspunten",
    "Minpunten",
    "Voor wie wel / voor wie niet",
    "Wat kopers vaak vragen",
    "Betrouwbaarheid van deze samenvatting",
    "Bronnen",
]


def strip_citations(text):
    return CITATION_RE.sub("", text).strip()


def collapse_ws(text):
    return re.sub(r"\s+", " ", text).strip()


def sections(md):
    """Split a review's markdown on ##/### headings, same idea as rank_top10.py's sections()."""
    out, cur = {}, None
    for line in md.splitlines():
        m = SECTION_RE.match(line)
        if m:
            cur = m.group(1)
            out[cur] = ""
        elif cur:
            out[cur] += line + "\n"
    return out


def parse_bullets(section_text):
    """Bullets starting with -/*; wrapped continuation lines are appended to the current item."""
    items = []
    for line in section_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        m = re.match(r"^[-*]\s+(.*)", stripped)
        if m:
            items.append(strip_citations(m.group(1)))
        elif items:
            items[-1] = strip_citations((items[-1] + " " + stripped))
    return items


def parse_wel_niet(section_text):
    text = section_text.strip()
    m = WEL_NIET_RE.match(text)
    if m:
        return strip_citations(m.group(1)), strip_citations(m.group(2))
    return strip_citations(text), ""


def parse_vragen(section_text):
    vragen = []
    for m in QA_RE.finditer(section_text):
        vraag = collapse_ws(strip_citations(m.group(1)))
        antwoord = m.group(2)
        antwoord = re.sub(r"^[\s\-]+", "", antwoord)
        antwoord = collapse_ws(strip_citations(antwoord))
        # In the dashed bullet format ("- **Q?**  \n  A...\n- **Q2?**..."), the next item's
        # leading "- " bleeds into the end of THIS answer's non-greedy capture (it sits right
        # before the next "**"). Strip that trailing bullet-dash remnant too.
        antwoord = re.sub(r"\s-\s*$", "", antwoord).strip()
        vragen.append({"vraag": vraag, "antwoord": antwoord})
    return vragen


def parse_bronnen(section_text):
    """Numbered source lines. The model is inconsistent about how it wraps the URL:
    bare (`1. https://...`), markdown link (`1. [Title](https://...)`), single- or
    triple-backtick-wrapped (`` 1. `https://...` ``), with a redundant trailing
    self-citation decoration (`... ([domain](https://...))`), or with the URL pushed
    onto the next line inside a fenced code block (`1. ```text` / `https://...` / ```` ``` ````).
    All of these were found in the real review data — handle each rather than assuming one shape.
    """
    bronnen = []
    n = 0
    lines = section_text.splitlines()
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        i += 1
        if not stripped:
            continue
        m_num = re.match(r"^\d+\.\s*(.*)", stripped)
        if not m_num:
            continue
        rest = m_num.group(1).strip()

        if FENCE_RE.match(rest):
            # URL lives inside the fenced block that follows, not on the numbered line itself.
            rest = ""
            while i < len(lines) and not FENCE_RE.match(lines[i].strip()):
                candidate = lines[i].strip()
                if candidate and not rest:
                    rest = candidate
                i += 1
            if i < len(lines):  # consume the closing fence
                i += 1

        # Strip a redundant trailing self-citation like "([domain](url))", but only if
        # something meaningful (the real URL) remains — otherwise this line's *only*
        # content is a genuine "[Title](url)" markdown link and must be kept intact.
        de_cited = TRAILING_SELF_CITATION_RE.sub("", rest).strip()
        if de_cited:
            rest = de_cited
        rest = rest.replace("`", "").strip()

        n += 1
        m_link = MD_LINK_RE.search(rest)
        if m_link:
            bronnen.append({"n": n, "title": m_link.group(1), "url": m_link.group(2)})
        else:
            # A rare stray line glues trailing free text onto the URL (e.g. "https://x.nl /
            # spec via y.nl"). Keep only the URL token, not the model's commentary after it.
            if rest.lower().startswith("http") and " " in rest:
                rest = rest.split()[0]
            bronnen.append({"n": n, "title": "", "url": rest})
    return bronnen


def parse_structured(raw_markdown):
    secs = sections(raw_markdown)

    def get(heading):
        return next((secs[h] for h in secs if h.strip() == heading), "")

    oordeel = collapse_ws(strip_citations(get(HEADINGS[0])))
    pluspunten = parse_bullets(get(HEADINGS[1]))
    minpunten = parse_bullets(get(HEADINGS[2]))
    voor_wie_wel, voor_wie_niet = parse_wel_niet(get(HEADINGS[3]))
    vragen = parse_vragen(get(HEADINGS[4]))
    betrouwbaarheid = strip_citations(get(HEADINGS[5]).strip())
    bronnen = parse_bronnen(get(HEADINGS[6]))

    return {
        "oordeel": oordeel,
        "pluspunten": pluspunten,
        "minpunten": minpunten,
        "voor_wie_wel": voor_wie_wel,
        "voor_wie_niet": voor_wie_niet,
        "vragen": vragen,
        "betrouwbaarheid": betrouwbaarheid,
        "bronnen": bronnen,
    }


def load_keyword_research():
    """Zoekvolumes; ontbreekt het keyword-onderzoek, dan blijft dit leeg."""
    rows = []
    path = DATA / "keywords.csv"
    if not path.exists():
        return rows
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            # Leeg = Keyword Planner gaf geen rij terug. Dat is iets anders dan
            # nul zoekopdrachten, dus het blijft null in plaats van 0.
            raw = (row.get("search_volume") or "").strip()
            rows.append({"keyword": row["keyword"],
                         "search_volume": int(raw) if raw else None})
    return rows


def load_price_map():
    """{ean: price} using the latest (max) date per ean, not hardcoded to one date."""
    latest = {}  # ean -> (date, price)
    path = DATA / "price_snapshots.jsonl"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            ean, date, price = row["ean"], row["date"], row["price"]
            if ean not in latest or date > latest[ean][0]:
                latest[ean] = (date, price)
    return {ean: price for ean, (date, price) in latest.items()}


def load_offers_by_ean():
    offers = {}
    path = DATA / "offers_top10.csv"
    if not path.exists():
        return offers
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["shop_name"] == "Productpine.com":
                continue
            # A few rows have an empty delivery_cost (e.g. disabled/invalid offers); default to 0.0
            # since the contract's delivery_cost is a plain float, not a nullable field.
            offers.setdefault(row["ean"], []).append({
                "shop_name": row["shop_name"],
                "price": float(row["price"]),
                "delivery_cost": float(row["delivery_cost"] or 0),
                "url": row["url"],
            })
    for ean in offers:
        offers[ean].sort(key=lambda o: o["price"])
    return offers


def load_tagdata():
    """Commercieel bewijs uit Redshift (scripts/get_tagdata.py). Optioneel: zonder
    dat bestand bouwt de export gewoon door, met commerce = None per product."""
    path = DATA / "tagdata.json"
    if not path.exists():
        return {}, {}
    d = json.loads(path.read_text(encoding="utf-8"))
    return d.get("ean_score", {}), d.get("tag", {})


def build(topic, limit=None):
    keyword_research = load_keyword_research()
    terms_list = topic.terms
    products_master = json.loads((DATA / "products_master.json").read_text())
    clicks = json.loads((DATA / "clicks.json").read_text()) if (DATA / "clicks.json").exists() else {}
    price_map = load_price_map()
    offers_by_ean = load_offers_by_ean()
    ean_score, tag = load_tagdata()

    # --- reviews: load all, track cost + model ---
    review_by_key = {}
    total_review_cost = 0.0
    review_model = None
    for key in products_master:
        r_path = DATA / "results" / f"review__openai__{key}.json"
        if not r_path.exists():
            continue
        review = json.loads(r_path.read_text())
        if review.get("error"):
            continue                       # mislukte review telt niet als review
        review_by_key[key] = review
        # Reviews die zijn gedraaid toen het model nog geen tarief had, staan
        # met kosten null in de cache. Hier alsnog uitrekenen uit de bewaarde
        # tokens — dat scheelt de hele run opnieuw draaien om een prijs te
        # weten. Blijft null als het model nog steeds niet in pricing.json staat.
        cost = (review.get("cost_usd") or {}).get("total")
        if cost is None and review.get("usage"):
            recomputed = cost_for(review.get("model") or "", review["usage"],
                                  review.get("n_searches", 0))
            review["cost_usd"] = recomputed
            cost = recomputed["total"]
        if cost:
            total_review_cost += cost
        if review_model is None:
            review_model = review.get("model")

    # --- terms ---
    # Eén item per PAGINA, niet per zoekterm: termen met dezelfde productlijst
    # zijn samengevoegd. `also_targets` houdt de overige keywords vast, zodat
    # de pagina weet waar hij nog meer op mikt.
    pages_path = DATA / "pages.json"
    if pages_path.exists():
        pages = json.loads(pages_path.read_text())
    else:
        pages = [{"slug": term_slug(t["term"]), "term": t["term"],
                  "display": t.get("display") or t["term"], "volume": t.get("volume"),
                  "volume_combined": t.get("volume"),
                  "terms": [{"term": t["term"], "volume": t.get("volume")}]}
                 for t in terms_list]

    terms_out = []
    for t in pages:
        term = t["term"]
        slug = t.get("slug") or term_slug(term)
        rank_path = DATA / f"rank_{slug}.json"
        if not rank_path.exists():
            print(f"  (geen ranglijst voor '{term}' — overgeslagen)")
            continue
        rank_data = json.loads(rank_path.read_text())
        top10 = []
        for i, p in enumerate(rank_data["products"], 1):
            ean = p["ean"]
            top10.append({
                "rank": i,
                "ean": ean,
                "quality_score": p.get("quality_score"),
                "evidence": p.get("evidence"),
                "verdict": p.get("verdict"),
                "pluses": p.get("pluses") or [],
                "letop": p.get("letop"),
                "voorjou": p.get("voorjou"),
                "price_at_build": price_map.get(ean),
            })
        terms_out.append({
            "term": term,
            "display": t.get("display") or term,      # paginatitel
            "volume": t.get("volume"),
            "volume_combined": t.get("volume_combined"),
            # De overige keywords die op deze pagina uitkomen.
            "also_targets": [x for x in (t.get("terms") or []) if x["term"] != term],
            "slug": slug,
            "has_mockup_page": bool(t.get("has_mockup_page")),
            "intro": rank_data["intro"],
            "methodiek": rank_data["methodiek"],
            "rank_usage": rank_data["usage"],
            "top10": top10,
        })

    # Rankingkosten uit de tokens die per term zijn bewaard. De rank-call doet
    # geen web_search, dus alleen in- en output tellen.
    rank_cost = 0.0
    for t in terms_out:
        usage = t.get("rank_usage") or {}
        c = cost_for(topic.rank_model, {"input_tokens": usage.get("input", 0),
                                        "output_tokens": usage.get("output", 0)}, 0)
        if c["total"] is None:
            rank_cost = None
            break
        rank_cost += c["total"]
    if rank_cost is not None:
        rank_cost = round(rank_cost, 4)

    # --- products ---
    product_keys = list(products_master.keys())
    limited_keys = product_keys[:limit] if limit else product_keys
    products_out = {}
    n_reviews_written = 0
    for key in limited_keys:
        p = products_master[key]
        offers = offers_by_ean.get(key, [])
        review = review_by_key.get(key)
        if review is None:
            review_out = None
        else:
            n_reviews_written += 1
            markdown = review.get("raw_markdown") or review.get("text") or ""
            prompt_sent = PROMPT.format(title=p["title"], brand=p["brand"] or "onbekend", ean=key)
            review_out = {
                "provider": review["provider"],
                "model": review["model"],
                "n_searches": review["n_searches"],
                "queries": review["queries"],
                "latency_s": review["latency_s"],
                "usage": review["usage"],
                "cost_usd": review["cost_usd"],
                "source_urls_are_real": review.get("source_urls_are_real"),
                "unverified_urls": review.get("unverified_urls", []),
                "prompt_sent": prompt_sent,
                "raw_markdown": markdown,
                "structured": parse_structured(markdown),
                "citations": review["citations"],
                "sources_consulted": review["sources_consulted"],
            }
        products_out[key] = {
            "ean": p["ean"],
            "title": p["title"],
            "brand": p["brand"],
            "min_price_at_build": price_map.get(key),
            "image": p.get("image"),
            "plp_url": p["plpUrl"],
            "clicks": clicks.get(key, {}).get("clicks", 0),
            "live_offers": offers,
            # Wat Beslist zelf van dit EAN vindt (bt.ean_score) en wat onze pixel
            # bij de winkels heeft gemeten (bt.revenue_per_product).
            "commerce": {"ean_score": ean_score.get(key), "tag": tag.get(key)},
            "review": review_out,
        }

    export = {
        "meta": {
            "generated_at": dt.datetime.now().isoformat(),
            "topic": topic.slug,
            "category": topic.category,
            "provider": "openai",
            "review_model": review_model or topic.review_model,
            "rank_model": topic.rank_model,
            "counts": {
                "keywords": len(keyword_research),
                "terms": len(terms_list),
                "pages": len(terms_out),
                "products": len(products_master),
                "reviews": len(review_by_key),
            },
            # None betekent: het model staat niet in shared/pricing.json. Een
            # geraden tarief is erger dan geen tarief.
            "cost_usd": {
                "reviews": round(total_review_cost, 2) if total_review_cost else None,
                "ranking": rank_cost,
                "total": (round(total_review_cost + rank_cost, 2)
                          if total_review_cost and rank_cost is not None else None),
            },
        },
        "prompts": {"review": PROMPT, "rank_system": SYSTEM},
        "keyword_research": keyword_research,
        "terms": terms_out,
        "products": products_out,
    }
    return export, n_reviews_written


def main():
    ap = add_topic_arg(argparse.ArgumentParser(description=__doc__))
    ap.add_argument("--dry-run", action="store_true", help="alleen tellen, niets wegschrijven")
    ap.add_argument("--limit", type=int, default=None, help="alleen de eerste N producten")
    a = ap.parse_args()

    topic = find_topic(a.topic)
    # DATA en OUT_PATH zijn module-globals omdat de overgenomen parserfuncties
    # ze direct gebruiken; hier worden ze op het gekozen topic gezet.
    global DATA, OUT_PATH
    DATA = topic.data
    OUT_PATH = DATA / "export" / f"top10_{term_slug(topic.label)}_full.json"

    export, n_reviews_written = build(topic, limit=a.limit)
    counts = export["meta"]["counts"]

    if not a.dry_run:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUT_PATH.write_text(json.dumps(export, indent=2, ensure_ascii=False), encoding="utf-8")
        size_mb = OUT_PATH.stat().st_size / 1_000_000
        print(f"geschreven: {OUT_PATH} ({size_mb:.2f} MB)")
    else:
        print(f"[dry-run] zou schrijven: {OUT_PATH}")

    print(f"keywords={counts['keywords']} termen={counts['terms']} "
          f"producten_totaal={counts['products']} producten_in_bestand={len(export['products'])} "
          f"reviews_totaal={counts['reviews']} reviews_in_bestand={n_reviews_written}")


if __name__ == "__main__":
    main()
