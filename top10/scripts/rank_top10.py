#!/usr/bin/env python3
"""Stap 5: per zoekterm scoort en rangschikt het model de kandidaten (kost geld).

Het model ziet bewust géén prijzen: dan kan het ook geen prijsclaims in de
tekst laten lekken ("goedkoopste", "prijs-kwaliteit"). Het scoort de kwaliteit
van het bewijs uit de reviews en schrijft prijsneutrale copy; de prijs komt er
in de export live bij.

Per term één bestand ``rank_<slug>.json``; bestaat dat al, dan wordt de term
overgeslagen. Zo kost een hervatte run alleen wat er nog niet stond.

    python top10/scripts/rank_top10.py --topic airfryers [--terms "beste airfryer"]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from openai import OpenAI

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared.topic import add_topic_arg, find_topic, slugify            # noqa: E402

SYSTEM = """Je maakt top-10 koopgidsen voor beslist.nl (prijsvergelijker). Je krijgt per product een
review-samenvatting (gebaseerd op echte bronnen), de populariteit op onze site (positie) en, waar
beschikbaar, echte klikdata. Je taak per product:
1. quality_score 0-100: hoe goed en betrouwbaar is dit product volgens de reviews (kwaliteit van het
   product zelf + hoeveelheid/betrouwbaarheid van het bewijs; een product met dun bewijs max ~65).
2. evidence: "sterk" | "redelijk" | "beperkt" (op basis van de betrouwbaarheidsnotitie).
3. Nederlandse teksten in een heel eenvoudige stijl: korte zinnen, alledaagse woorden, geen jargon,
   geen superlatieven-stapeling (uitleg-als-voor-een-vijfjarige, maar volwassen toon):
   - verdict: 1 zin, wat is het en voor wie (max 18 woorden)
   - pluses: 2 bullets (max 8 woorden per bullet)
   - letop: 1 bullet, eerlijk nadeel (max 10 woorden)
   - voorjou: "Voor jou als …" (max 12 woorden)
STRENG VERBODEN in alle tekst: prijzen, "goedkoop", "duur", "de goedkoopste", "prijs-kwaliteit",
kortingen. Prijzen veranderen; die tekst wordt apart toegevoegd. Verzin niets dat niet in de review staat.
Ook per pagina: intro (±60 woorden, zelfde simpele stijl, noem waar je op moet letten bij deze
selectie) en methodiek (1-2 zinnen: hoe deze lijst tot stand komt: echte reviews, kliks van bezoekers,
populariteit; prijs wordt live meegewogen).
Geef de producten in de volgorde van de ranglijst: het beste product eerst, maximaal 10 producten.
Antwoord als JSON: {"intro": str, "methodiek": str, "products": [{"ean": str, "quality_score": int,
"evidence": str, "verdict": str, "pluses": [str, str], "letop": str, "voorjou": str}]}"""


def sections(md: str) -> dict:
    out, cur = {}, None
    for line in (md or "").splitlines():
        m = re.match(r"^\s*#{2,3}\s+(.*?)\s*$", line)
        if m:
            cur = m.group(1)
            out[cur] = ""
        elif cur:
            out[cur] += line + "\n"
    return out


def condense(review: dict) -> str:
    """Review inkorten tot wat de ranking nodig heeft, zonder bronlinks.

    Zonder dit gaat een halve MB reviewtekst mee in één prompt; en de
    bronlinks zouden het model verleiden tot citeren in de copy.
    """
    text = review.get("raw_markdown") or review.get("text") or ""
    s = sections(text)
    keep = ["Oordeel in één zin", "Pluspunten", "Minpunten",
            "Voor wie wel / voor wie niet", "Betrouwbaarheid van deze samenvatting"]
    parts = []
    for k in keep:
        v = next((s[h] for h in s if h.lower().startswith(k.lower()[:12])), "")
        v = re.sub(r"\(\[[^\]]*\]\([^)]*\)\)|\(https?://\S+\)", "", v)
        parts.append(f"[{k}] {v.strip()[:700]}")
    return "\n".join(parts)


def main() -> int:
    ap = add_topic_arg(argparse.ArgumentParser(description=__doc__))
    ap.add_argument("--terms", help="alleen deze termen (komma-gescheiden)")
    ap.add_argument("--force", action="store_true", help="ook termen die al een bestand hebben")
    args = ap.parse_args()
    topic = find_topic(args.topic)

    per_term = topic.read_json("products_per_term.json")
    clicks = topic.read_json("clicks.json", default={})
    if not clicks:
        print("let op: geen clicks.json — de ranking draait zonder klikdata")
    client = OpenAI()

    wanted = [t.strip() for t in args.terms.split(",")] if args.terms else list(per_term)
    for term in wanted:
        info = per_term.get(term)
        if not info:
            print(f"{term}: onbekende term, overgeslagen")
            continue
        out_path = topic.file(f"rank_{slugify(term)}.json")
        if out_path.exists() and not args.force:
            print(f"{term}: al gedaan")
            continue

        lines = []
        for i, p in enumerate(info["products"], 1):
            r_path = topic.results / f"review__openai__{p['ean']}.json"
            if not r_path.exists():
                continue
            rev = json.loads(r_path.read_text(encoding="utf-8"))
            if rev.get("error"):
                continue
            c = clicks.get(p["ean"], {})
            click_line = (f"kliks (90 dagen): {c['clicks']}" if c.get("clicks_usable")
                          else "kliks: onvoldoende data (niet gebruiken)")
            lines.append(f"### Product {i}\nEAN: {p['ean']}\nTitel: {p['title']}\n"
                         f"Merk: {p['brand']}\n"
                         f"Populariteit op de pagina: positie {i} van {len(info['products'])}\n"
                         f"{click_line}\nReview:\n{condense(rev)}\n")
        if not lines:
            print(f"{term}: geen reviews beschikbaar, overgeslagen")
            continue

        volume = info.get("volume")
        user = (f'Pagina: "Beste {term} — top 10"'
                + (f" (zoekvolume {volume}/maand).\n" if volume else ".\n")
                + f"Kandidaten ({len(lines)}):\n\n" + "\n".join(lines))
        resp = client.responses.create(
            model=topic.rank_model,
            input=[{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}])
        txt = re.sub(r"^```(json)?|```$", "", (resp.output_text or "").strip(), flags=re.M).strip()
        try:
            data = json.loads(txt)
        except json.JSONDecodeError as e:
            topic.file(f"rank_{slugify(term)}.raw.txt").write_text(txt, encoding="utf-8")
            print(f"{term}: JSON onleesbaar ({e}); ruwe tekst bewaard")
            continue

        usage = resp.usage
        data["term"] = term
        data["model"] = topic.rank_model
        data["usage"] = {"input": getattr(usage, "input_tokens", 0),
                         "output": getattr(usage, "output_tokens", 0)}
        out_path.write_text(json.dumps(data, indent=1, ensure_ascii=False), encoding="utf-8")
        print(f"{term}: {len(data.get('products', []))} producten gescoord "
              f"(tokens {data['usage']['input']}/{data['usage']['output']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
