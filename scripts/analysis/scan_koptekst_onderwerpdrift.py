#!/usr/bin/env python3
"""Scan kopteksten op ONDERWERPDRIFT: teksten die een tweede productgroep
introduceren naast het onderwerp van de pagina.

Aanleiding (2026-08-18): op de Tennisrackets-pagina
(/c/sporten_sport_outdoor~484337) opende de koptekst met "Tennis- en
padelrackets zijn essentieel ..." en linkte twee echte padelrackets. Oorzaak was
NIET het model en NIET de facetdata, maar het onderwerp dat aan GPT werd
meegegeven: de CSV-lookup levert als categorienaam "Rackets", en zodra dat de
`product_subject` wordt in plaats van "Tennis Rackets", is padel noemen correct
gedrag. Gereproduceerd: subject "Rackets" -> padel in de tekst, subject
"Tennis Rackets" -> geen padel.

DETECTOR (bewust smal gehouden, om ruis laag te houden):
neem het onderwerp uit pa.unique_titles_content.h1_title (bv. "tennisrackets"),
pak daarvan het staart-zelfstandignaamwoord (bv. "rackets") en zoek in de tekst
woorden die op datzelfde staartwoord eindigen maar een ANDERE kop hebben
(bv. "padelrackets"). Dat is precies de "X- en Y"-verbreding die we willen zien,
en het vangt ook damesschoenen-op-een-herenschoenenpagina.

Wat het NIET pretendeert: dit is geen semantische controle. Een tekst kan van het
onderwerp afdwalen zonder zo'n samenstelling te gebruiken; die gevallen mist deze
scan. De cijfers zijn dus een ondergrens.

Draaien:  ./venv/bin/python scripts/analysis/scan_koptekst_onderwerpdrift.py [--limit N] [--csv pad]
"""
import argparse
import csv
import re
import sys
import unicodedata
from collections import Counter

sys.path.insert(0, ".")
from backend.database import get_db_connection  # noqa: E402

TAG_RE = re.compile(r"<[^>]+>")
WORD_RE = re.compile(r"[a-z]{3,}")

# Staartwoorden korter dan dit leveren te veel toevallige treffers op ("oenen").
MIN_SUFFIX = 6
MIN_EXTRA_PREFIX = 3   # "padel" voor "rackets"


def norm(s: str) -> str:
    """Lowercase, accentloos, koppeltekens weg. 'Tennis-rackets' -> 'tennisrackets'."""
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"[-‐-―\s]+", "", s.lower())


def strip_html(s: str) -> str:
    return TAG_RE.sub(" ", s or "")


def drift_terms(h1_title: str, content: str, vocab: set):
    """Woorden in de tekst die op hetzelfde staartwoord eindigen als het
    onderwerp, maar met een andere kop. Lege lijst = geen drift gevonden."""
    # Alleen het LAATSTE woord van de titel telt: dat is het zelfstandig naamwoord
    # waar het onderwerp op eindigt ("Zwarte Nike Heren Schoenen" -> "schoenen").
    h1n = norm((h1_title or "").split()[-1]) if (h1_title or "").split() else ""
    # Het onderwerp moet ZELF een samenstelling zijn op dat staartwoord. Anders
    # is een langer woord in de tekst een verbijzondering en geen zijstap:
    # "schoenen" -> "voetbalschoenen" is prima, "tennisrackets" -> "padelrackets"
    # niet, want dan staan er twee elkaar uitsluitende productgroepen naast elkaar.
    if len(h1n) < MIN_SUFFIX + MIN_EXTRA_PREFIX:
        return [], False
    full = norm(h1_title)          # hele titel aaneen: "wegwerpserviezen"
    text = strip_html(content)
    # koppeltekens binnen samenstellingen wegwerken: "Tennis- en padelrackets"
    # levert zo zowel "tennis" als "padelrackets" op.
    words = {norm(w) for w in WORD_RE.findall(text.lower())}
    found = []
    # langste staart eerst: die is het meest specifiek
    for k in range(len(h1n) - MIN_EXTRA_PREFIX, MIN_SUFFIX - 1, -1):
        suf = h1n[-k:]
        # `w in h1n` sluit het normale geval uit waarin de tekst het onderwerp
        # noemt zonder de bepaling ervoor: h1 "Roze handtassen" -> tekst
        # "handtassen". Dat is geen drift maar exact hetzelfde onderwerp.
        # "padelrackets" zit niet in "tennisrackets" en blijft dus staan.
        # Het staartwoord moet een ECHT kopwoord zijn, niet een toevallige
        # letterreeks: "kettingen" mag geen "tingen" opleveren. vocab is de
        # verzameling laatste woorden van alle h1_titles, dus "rackets" en
        # "schoenen" zitten erin en "tingen"/"netrons" niet.
        if suf not in vocab:
            continue
        # `w not in full` sluit dezelfde term los-vs-aaneen uit:
        # h1 "Wegwerp serviezen" tegen tekst "wegwerpserviezen".
        # `w not in full` sluit dezelfde term los-vs-aaneen uit; de prefix-test
        # daarna sluit HERSCHIKKINGEN uit: "buiten vloerkleden" -> "buitenkleden"
        # en "Katten Dierenriemen" -> "kattenriemen" bouwen hun samenstelling uit
        # woorden die al in de titel staan. "padelrackets" op "tennisrackets" niet:
        # "padel" komt in de titel nergens voor, en dat is precies het verschil
        # tussen anders verwoord en een andere productgroep.
        hits = [w for w in words
                if w != h1n and w not in full and w.endswith(suf)
                and len(w) - len(suf) >= MIN_EXTRA_PREFIX
                and not (len(w[:-len(suf)]) >= 4 and w[:-len(suf)] in full)]
        if hits:
            found = sorted(hits)
            break
    if not found:
        return [], False
    # Ernst: staat de afwijkende term in de EERSTE zin, dan is het onderwerp zelf
    # verbreed ("Tennis- en padelrackets zijn ...") en is de hele tekst mis.
    # Verderop is het meestal een terloopse vergelijking en blijft de tekst op
    # onderwerp. Alleen de eerste groep is echt hergenereerwerk.
    opening = norm(re.split(r"(?<=[.!?])\s", strip_html(content).strip(), maxsplit=1)[0])
    return found, any(w in opening for w in found)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="alleen de eerste N rijen (test)")
    ap.add_argument("--csv", default="", help="schrijf de treffers naar dit pad")
    args = ap.parse_args()

    conn = get_db_connection()
    cur = conn.cursor()
    q = """SELECT u.url, t.h1_title, c.content, c.created_at
           FROM pa.kopteksten_content c
           JOIN pa.urls u ON c.url_id = u.url_id
           JOIN pa.unique_titles_content t ON t.url_id = c.url_id
           WHERE u.url LIKE '%%/c/%%' AND coalesce(t.h1_title, '') <> ''"""
    if args.limit:
        q += f" LIMIT {args.limit}"
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Vocabulaire van kopwoorden: het laatste woord van elke h1_title.
    vocab = set()
    for r in rows:
        parts = (dict(r)["h1_title"] or "").split()
        if parts:
            vocab.add(norm(parts[-1]))
    print(f"kopwoord-vocabulaire: {len(vocab):,} termen")

    hits, per_term, per_maincat = [], Counter(), Counter()
    opening_hits = 0
    for r in rows:
        d = dict(r)
        terms, in_opening = drift_terms(d["h1_title"], d["content"], vocab)
        if terms:
            if in_opening:
                opening_hits += 1
            hits.append((d["url"], d["h1_title"], ", ".join(terms[:4]),
                         "openingszin" if in_opening else "verderop", d["created_at"]))
            for t in terms[:4]:
                per_term[t] += 1
            per_maincat[d["url"].split("/")[2] if d["url"].count("/") > 2 else "?"] += 1

    n = len(rows)
    print(f"gescand            : {n:,} kopteksten")
    print(f"onderwerpdrift     : {len(hits):,}  ({100*len(hits)/n:.2f}%)")
    print(f"  in de openingszin: {opening_hits:,}  ({100*opening_hits/n:.2f}%)  <- onderwerp verbreed")
    print(f"  pas verderop     : {len(hits)-opening_hits:,}  ({100*(len(hits)-opening_hits)/n:.2f}%)  <- terloopse vermelding")
    print("\ntop-20 afwijkende termen:")
    for t, c in per_term.most_common(20):
        print(f"  {t:<34}{c:>7,}")
    print("\ntop-10 hoofdcategorieen:")
    for m, c in per_maincat.most_common(10):
        print(f"  {m:<34}{c:>7,}")
    print("\nvoorbeelden:")
    for u, h1, t, _sev, _ in [x for x in hits if x[3] == "openingszin"][:10]:
        print(f"  [{h1}] -> {t}\n     {u}")

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["url", "h1_title", "afwijkende_termen", "ernst", "created_at"])
            w.writerows(hits)
        print(f"\nweggeschreven: {args.csv} ({len(hits):,} rijen)")


if __name__ == "__main__":
    main()
