# -*- coding: utf-8 -*-
"""
Stap 1 van /findfacetvalues.

Resolvet de categorie en het gevraagde facet, haalt de producten op, en destilleert
kandidaat-facetwaarden uit producttitels en -beschrijvingen.

Dit script filtert NIET op betekenis. Het levert een ruwe kandidatenlijst plus alle
context die nodig is om die lijst daarna streng op facet-soort te schiften.

    python scan.py --url <categorie-url> --facet "Opties" [--workdir DIR]
                   [--max-products 3000] [--min-title 3] [--min-desc 10]
                   [--review 500] [--mode text|measure]
"""
import argparse
import collections
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (norm, resolve_category, category_facets, facets_named,
                    facet_values, fetch_products, nl_label)

STOP = set("""
de het een en of maar want dus als dan die dat deze dit daar hier er is zijn was waren wordt worden werd
ben bent heb hebt heeft hebben had hadden zal zult zullen zou zouden kan kunt kunnen kon konden
mag mogen moet moeten wil wilt willen ook al alle alles allen andere ander anders bij van voor met naar
uit op in aan om te ten ter door over onder tussen tot per niet geen nog wel zeer erg heel meer meest
minder minst veel weinig zo zoals omdat doordat zodat waardoor waarmee waarbij waarvan waarin waarop
waaronder uw jij jouw jullie wij ons onze mij mijn hij zij hun haar hem zich elk elke iedere ieder
beide beiden geschikt ideaal ideale perfect perfecte zorgt biedt maakt zorgen bieden maken gebruikt
gebruiken echter tevens bovendien daarnaast verder tenslotte zonder zowel altijd nooit soms vaak
dankzij hoewel indien mits ondanks behalve namelijk immers zulke welke wat wie waar wanneer hoe waarom
hoeveel mogelijk eventueel wellicht misschien hierdoor hiermee hiervoor daarom
""".split())

# generieke koppen: hier eindigt geen bruikbare facetwaarde op
GENERIC_HEAD = set("""
oplossing oplossingen kwaliteit uitstraling toepassing toepassingen gebruik product producten
resultaat resultaten manier manieren keuze keuzes voordeel voordelen kenmerk kenmerken eigenschap
eigenschappen mogelijkheid mogelijkheden prijs prijzen levering verzending garantie
stuks stuk pakket pakketten verpakking verpakkingen assortiment inhoud aantal
jaar jaren dag dagen week weken maand maanden tijd tijden moment momenten plaats plaatsen
project projecten klus klussen werk werken hulp gemak comfort veiligheid duurzaamheid
prestatie prestaties merk merken fabrikant leverancier winkel shop bestelling order klant klanten
informatie details specificaties specificatie beschrijving handleiding instructie instructies
tip tips advies adviezen vraag vragen antwoord voorbeeld voorbeelden
zelver zelvers gebruiker gebruikers oorsprong modelnummer certificering nummer code artikel
artikelnummer levensduur prestaties bijdrage keus
""".split())

UNIT = set("mm cm dm m km kg g gr gram ml cl dl l liter st stk stuks pcs stks x nr no ca incl excl "
           "mtr inch watt volt ampere mah wp kwh".split())

BLOCK_EXACT = set("beslist nl com bol amazon temu www http https jpg png webp".split())

# stopwoorden die wél een facetwaarde mogen beginnen: "Met standaard", "Op wielen",
# "Zonder snoer", "Voor buiten" zijn echte waarden van o.a. het facet Opties.
LEAD_OK = set("met zonder voor op incl inclusief anti extra multi".split())


def is_num(t):
    return bool(re.fullmatch(r"[0-9]+([.,][0-9]+)?", t)) or bool(re.fullmatch(r"[0-9]+[a-z]{0,5}", t))


def ngrams(toks, n):
    for i in range(len(toks) - n + 1):
        g = toks[i:i + n]
        if any(is_num(x) or x in UNIT for x in g):
            continue
        if any(len(x) < 3 for x in g[1:]) or (len(g[0]) < 3 and g[0] not in LEAD_OK):
            continue
        if (g[0] in STOP and g[0] not in LEAD_OK) or g[-1] in STOP:
            continue
        if n > 1 and any(x in STOP and x not in LEAD_OK for x in g[1:-1]):
            continue
        if n == 1 and g[0] in LEAD_OK:
            continue
        if g[-1] in GENERIC_HEAD:
            continue
        p = " ".join(g)
        if p in BLOCK_EXACT:
            continue
        yield p


MEASURE_RE = [
    re.compile(r"\b\d+(?:[.,]\d+)?\s*[x×]\s*\d+(?:[.,]\d+)?(?:\s*[x×]\s*\d+(?:[.,]\d+)?)?\s*"
               r"(mm|cm|dm|m|inch|\")?\b", re.I),
    re.compile(r"\b\d+(?:[.,]\d+)?\s*(mm|cm|dm|m|inch|kg|g|gram|ml|l|liter|wp|watt|v|ah|mah|kwh)\b", re.I),
]


def extract_measures(text):
    out = set()
    for rx in MEASURE_RE:
        for m in rx.finditer(text or ""):
            s = re.sub(r"\s+", " ", m.group(0)).strip().lower().replace("×", "x")
            s = re.sub(r"\s*x\s*", "x", s)
            if len(s) <= 40 and re.search(r"\d", s):
                out.add(s)
    return out


def inflect(term, universe):
    """Enkel-/meervoudsvarianten, maar alleen als die vorm echt in de teksten voorkomt."""
    parts = term.split()
    head, pre = parts[-1], parts[:-1]
    cands = set()
    if not head.endswith(("en", "s")):
        cands |= {head + "en", head + "s", head + "es"}
        m = re.match(r"^(.*)([aeou])([bcdfgklmnprstvz])$", head)
        if m:
            cands.add(m.group(1) + m.group(2) * 2 + m.group(3) + "en")
        if re.search(r"[aeiou][bcdfgklmnprstvz]$", head):
            cands.add(head + head[-1] + "en")
    if head.endswith("en") and len(head) > 4:
        s = head[:-2]
        cands.add(s)
        if re.search(r"([bcdfgklmnprstvz])\1$", s):
            cands.add(s[:-1])
        m = re.match(r"^(.*)([aeou])([bcdfgklmnprstvz])$", s)
        if m:
            cands.add(m.group(1) + m.group(2) * 2 + m.group(3))
    if head.endswith("s") and len(head) > 4 and not head.endswith("ss"):
        cands.add(head[:-1])
    out = set()
    for c in cands:
        v = " ".join(pre + [c])
        if v != term and v in universe:
            out.add(v)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--facet", required=True, help="naam van het facet, bv. Opties / Type / Kleur")
    ap.add_argument("--workdir", default=None)
    ap.add_argument("--max-products", type=int, default=3000)
    ap.add_argument("--min-title", type=int, default=3, help="min. producten met de term in de titel")
    ap.add_argument("--min-desc", type=int, default=10, help="min. producten met de term in de beschrijving")
    ap.add_argument("--review", type=int, default=500, help="hoeveel kandidaten in het reviewbestand")
    ap.add_argument("--mode", choices=["text", "measure"], default="text")
    a = ap.parse_args()

    wd = a.workdir or os.path.join(os.getcwd(), "ffv_run")
    os.makedirs(wd, exist_ok=True)

    cat = resolve_category(a.url)
    print(f"Categorie : {cat['category_name']} (id {cat['category_id']}, urlName {cat['url_name']})")
    print(f"Hoofdcat. : {cat['main_category_name']} (id {cat['main_category_id']})")
    print(f"Producten : {cat['product_count']} volgens de categorielijst\n")

    # ---- facetten van deze categorie -------------------------------------
    cfs = category_facets(cat["category_id"])
    print("Facetten die deze categorie NU heeft:")
    for f in cfs:
        print(f"  {f['facet_id']:>6}  {f['name']}"
              + ("  [verborgen]" if f["is_hidden"] else "")
              + ("  [uitgeschakeld]" if f["is_enabled"] is False else ""))

    target = next((f for f in cfs if norm(f["name"] or "") == norm(a.facet)), None)
    existing_here = []
    if target:
        existing_here = facet_values(target["facet_id"])
        print(f"\nGevraagd facet '{a.facet}' ZIT AL op deze categorie (id {target['facet_id']}) "
              f"met {len(existing_here)} waarden:")
        print("  " + (", ".join(existing_here[:40]) if existing_here else "(geen)"))
    else:
        print(f"\nGevraagd facet '{a.facet}' zit NOG NIET op deze categorie.")

    # ---- referentiewaarden: hetzelfde facet elders in de taxonomie --------
    named = facets_named(a.facet)
    reference, ref_src = [], []
    for f in sorted(named, key=lambda x: -x["facet_id"])[:60]:
        vs = facet_values(f["facet_id"])
        if vs:
            ref_src.append({"facet_id": f["facet_id"], "n": len(vs), "sample": vs[:12]})
            reference.extend(vs)
    reference = sorted(set(reference))
    print(f"\nReferentie: {len(named)} facetten heten '{a.facet}' in de taxonomie, "
          f"samen {len(reference)} bestaande waarden.")
    for s in ref_src[:6]:
        print(f"  facet {s['facet_id']} ({s['n']}): {', '.join(s['sample'])}")
    if not named:
        print("  LET OP: geen facet met deze naam gevonden. Controleer de spelling, of ga verder")
        print("  op basis van de definitie van het facetsoort alleen.")

    # ---- producten --------------------------------------------------------
    print(f"\nProducten ophalen (max {a.max_products}) ...")
    ps, sfacets, total = fetch_products(cat["main_category_id"], cat["category_id"], a.max_products)
    with_desc = sum(1 for p in ps if (p.get("description") or "").strip())
    print(f"  {len(ps)} producten opgehaald, {with_desc} met beschrijving.")
    if total and len(ps) < total:
        print(f"  LET OP: de categorie heeft er {total}; er is een steekproef van {len(ps)} gebruikt.")

    # waarden die de zoekindex nu al voor dit facet teruggeeft (met producttellingen)
    counts_here = {}
    for fa in sfacets or []:
        if norm(fa.get("name") or "") == norm(a.facet):
            for v in fa.get("values") or []:
                counts_here[norm(v["facetValue"])] = v.get("count")

    # ---- kandidaten -------------------------------------------------------
    title_df, desc_df, any_df = collections.Counter(), collections.Counter(), collections.Counter()
    for p in ps:
        t = norm(" ".join(filter(None, [p.get("title"), p.get("subtitle")])))
        d = norm(p.get("description") or "")
        if a.mode == "measure":
            raw_t = " ".join(filter(None, [p.get("title"), p.get("subtitle")]))
            st = extract_measures(raw_t)
            sd = extract_measures(p.get("description") or "")
        else:
            tt, dd = t.split(), d.split()
            st = {g for n in (1, 2, 3) for g in ngrams(tt, n)}
            sd = {g for n in (1, 2, 3) for g in ngrams(dd, n)}
        for g in st:
            title_df[g] += 1
        for g in sd:
            desc_df[g] += 1
        for g in st | sd:
            any_df[g] += 1

    universe = set(any_df)
    seeds = {t for t in universe if title_df[t] >= a.min_title or desc_df[t] >= a.min_desc}
    if a.mode == "text":
        for s in list(seeds):
            seeds |= inflect(s, universe)

    ref_norm = {norm(v) for v in reference}
    here_norm = {norm(v) for v in existing_here}

    recs = []
    for t in seeds:
        recs.append({
            "kandidaat": t,
            "producten_titel": title_df[t],
            "producten_desc": desc_df[t],
            "producten_totaal": any_df[t],
            "bestaat_al_in_categorie": norm(t) in here_norm,
            "bekend_als_facetwaarde_elders": norm(t) in ref_norm,
            "producten_volgens_zoekindex": counts_here.get(norm(t)),
        })
    recs.sort(key=lambda r: (not r["bekend_als_facetwaarde_elders"],
                             -r["producten_titel"], -r["producten_totaal"]))

    # voorbeeldtitel per kandidaat (helpt bij het beoordelen)
    titles = [(norm(" ".join(filter(None, [p.get("title"), p.get("subtitle")]))), p.get("title") or "")
              for p in ps]
    for r in recs[:a.review]:
        term = r["kandidaat"]
        for n, orig in titles:
            if re.search(r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])", n):
                r["voorbeeld"] = orig[:110]
                break
        r.setdefault("voorbeeld", "")

    out = {
        "categorie": cat,
        "facet_gevraagd": a.facet,
        "facet_op_categorie": target,
        "facet_bestaande_waarden": existing_here,
        "facet_referentiewaarden": reference,
        "facet_referentiebronnen": ref_src,
        "categorie_facetten": cfs,
        "producten_opgehaald": len(ps),
        "producten_met_beschrijving": with_desc,
        "producten_in_categorie": total,
        "mode": a.mode,
        "drempels": {"min_title": a.min_title, "min_desc": a.min_desc},
        "kandidaten": recs,
    }
    json.dump(out, open(os.path.join(wd, "scan.json"), "w", encoding="utf-8"), ensure_ascii=False)

    review = recs[:a.review]
    with open(os.path.join(wd, "review.tsv"), "w", encoding="utf-8") as fh:
        fh.write("kandidaat\ttitel\tdesc\ttotaal\tbekend_elders\tbestaat_al\tvoorbeeld\n")
        for r in review:
            fh.write(f"{r['kandidaat']}\t{r['producten_titel']}\t{r['producten_desc']}\t"
                     f"{r['producten_totaal']}\t{'ja' if r['bekend_als_facetwaarde_elders'] else ''}\t"
                     f"{'ja' if r['bestaat_al_in_categorie'] else ''}\t{r.get('voorbeeld','')}\n")

    print(f"\n{len(recs)} kandidaten; de bovenste {len(review)} staan in review.tsv")
    if len(recs) > len(review):
        print(f"  LET OP: {len(recs) - len(review)} kandidaten staan NIET in review.tsv "
              f"(wel in scan.json). Verhoog --review om er meer te zien.")
    print(f"Werkmap: {wd}")


if __name__ == "__main__":
    main()
