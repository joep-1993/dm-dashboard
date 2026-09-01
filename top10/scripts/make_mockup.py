#!/usr/bin/env python3
"""Bouwt een zelfstandige HTML-mockup van één top-10 pagina uit het exportbestand.

Leest alleen het contractbestand — geen netwerk, geen API. Bedoeld om te zien of
de gegenereerde data een pagina oplevert die klopt, niet als productiecode.

De opbouw volgt de referentiepagina: een trechter van dik naar dun. Nummer 1
krijgt de volle breedte, 2 en 3 staan als alternatieven naast elkaar, en de
rest is een compacte lijst. Wie na drie producten nog niets heeft gekozen,
scant liever dan dat hij leest.

Naast de consumentenpagina zit er een datalaag in (knop linksboven): per product
de kliks, het A-label, de pixelsessies en het aantal bronnen. Die hoort nooit op
een echte pagina, maar is precies wat je wilt zien als je beoordeelt of de
ranking te vertrouwen is.

    python top10/scripts/make_mockup.py --topic tandenborstels [--page 1]
"""
from __future__ import annotations

import argparse
import html
import json
import sys
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from mockup_style import CSS, FONT_LINK, PLACEHOLDER, TICK                # noqa: E402
from shared.topic import add_topic_arg, find_topic                        # noqa: E402

E = html.escape
SITE = "https://www.beslist.nl"


def euro(v) -> str:
    if not v:
        return "—"
    return "€ " + f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def nl(n) -> str:
    return f"{int(n or 0):,}".replace(",", ".")


def img_tag(prod: dict, alt: str) -> str:
    return f'<img src="{E(prod.get("image") or PLACEHOLDER)}" alt="{E(alt)}" loading="lazy">'


def shops_block(prod: dict, limit: int = 4) -> str:
    offers = (prod.get("live_offers") or [])[:limit]
    if not offers:
        return ""
    rows = "".join(
        f'<div class="shop{" best" if i == 0 else ""}">'
        f'<span class="shop-name">{E(o["shop_name"] or "onbekend")}</span>'
        f'<span class="shop-price">{euro(o["price"])}</span></div>'
        for i, o in enumerate(offers))
    return f'<div class="shops">{rows}</div>'


def datalayer(item: dict, prod: dict) -> str:
    com = prod.get("commerce") or {}
    score = com.get("ean_score") or {}
    tag = com.get("tag") or {}
    n_bron = len(((prod.get("review") or {}).get("structured") or {}).get("bronnen") or [])
    cells = [
        ("kliks", prod.get("clicks", 0)),
        ("A-label", score.get("ean_score_label") or "—"),
        ("score", score.get("totaal_ean_score") or "—"),
        ("winkels", score.get("shops_scored") or len(prod.get("live_offers") or [])),
        ("sessies", tag.get("total_session_starts_365") or "—"),
        ("transacties", tag.get("total_transactions_365") or "—"),
        ("bronnen", n_bron),
        ("modelscore", item.get("quality_score") or "—"),
        ("bewijs", item.get("evidence") or "—"),
    ]
    return ('<div class="datalayer">'
            + "".join(f"<span>{E(k)} <b>{E(str(v))}</b></span>" for k, v in cells)
            + "</div>")


def pluses(item: dict) -> str:
    return ("<ul class=\"pluses\">"
            + "".join(f"<li>{TICK}<span>{E(p)}</span></li>" for p in item.get("pluses") or [])
            + "</ul>")


def letop(item: dict) -> str:
    if not item.get("letop"):
        return ""
    return f'<p class="letop"><span class="lbl">Let op</span>{E(item["letop"])}</p>'


def plp(prod: dict) -> str:
    """Link naar onze eigen productpagina, niet naar de winkel.

    De knop belooft álle prijzen; die staan op de PLP en niet bij één verkoper.
    Doorlinken naar de goedkoopste winkel maakt de keuze al voor de bezoeker en
    slaat de vergelijking over — precies waarvoor hij hier is. Alleen als een
    product geen PLP heeft, valt hij terug op het beste aanbod.
    """
    if prod.get("plp_url"):
        return SITE + prod["plp_url"]
    offers = prod.get("live_offers") or []
    return offers[0]["url"] if offers else SITE


def cta(prod: dict, ghost: bool = False) -> str:
    n = len(prod.get("live_offers") or [])
    label = f"Bekijk alle {n} prijzen" if n > 1 else "Bekijk aanbieding"
    cls = "btn btn-ghost" if ghost else "btn"
    return f'<a class="{cls}" href="{E(plp(prod))}" target="_blank" rel="noopener">{E(label)}</a>'


def hero(item: dict, prod: dict, volume: int | None) -> str:
    return f'''
  <article class="hero">
    <div class="hero-band">
      <strong>Nummer 1 · onze keuze</strong>
      <span>{E(item.get("voorjou") or "")}</span>
    </div>
    <div class="hero-grid">
      {img_tag(prod, prod.get("title") or "")}
      <div>
        <div class="rank-name"><span class="n">1</span><h2>{E(prod.get("title") or "")}</h2></div>
        <p class="verdict">{E(item.get("verdict") or "")}</p>
        {pluses(item)}
        {letop(item)}
        <div class="buy buy-shops">
          <div class="price">{euro(item.get("price_at_build"))}</div>
          {shops_block(prod)}
          {cta(prod)}
        </div>
        {datalayer(item, prod)}
      </div>
    </div>
  </article>'''


def why_label(item: dict, prod: dict, top3: list[tuple[dict, dict]]) -> str:
    prices = [i.get("price_at_build") or 0 for i, _ in top3 if i.get("price_at_build")]
    if item.get("price_at_build") and prices and item["price_at_build"] == min(prices):
        return "Voordeligst van de top 3"
    clicks = [p.get("clicks", 0) for _, p in top3]
    if prod.get("clicks", 0) == max(clicks) and prod.get("clicks", 0) > 0:
        return "Meest aangeklikt"
    if (item.get("evidence") or "") == "sterk":
        return "Sterkste bewijs"
    return "Sterk alternatief"


def alt_card(item: dict, prod: dict, top3: list, second: bool) -> str:
    return f'''
    <article class="alt{' alt-b' if second else ''}">
      {img_tag(prod, prod.get("title") or "")}
      <div>
        <span class="why">{E(why_label(item, prod, top3))}</span>
        <h3><span class="n">{item["rank"]}</span>{E(prod.get("title") or "")}</h3>
        <p class="verdict">{E(item.get("verdict") or "")}</p>
        {pluses(item)}
        {letop(item)}
        <p class="voorjou">{E(item.get("voorjou") or "")}</p>
      </div>
      <div class="buy buy-shops">
        <div class="price">{euro(item.get("price_at_build"))}</div>
        {shops_block(prod, 3)}
        {cta(prod, ghost=True)}
        {datalayer(item, prod)}
      </div>
    </article>'''


def rest_row(item: dict, prod: dict) -> str:
    href = plp(prod)
    n = len(prod.get("live_offers") or [])
    return f'''
      <div class="row">
        <div class="n">{item["rank"]}</div>
        {img_tag(prod, prod.get("title") or "")}
        <div>
          <h3>{E(prod.get("title") or "")}</h3>
          <p class="voorjou">{E(item.get("voorjou") or item.get("verdict") or "")}</p>
          {letop(item)}
          {datalayer(item, prod)}
        </div>
        <div class="buy">
          <div class="price">{euro(item.get("price_at_build"))}</div>
          <a class="link" href="{E(href)}" target="_blank" rel="noopener">
            {E(f"{n} prijzen" if n > 1 else "bekijk")} ›</a>
        </div>
      </div>'''


def compare_table(top3: list) -> str:
    rows = "".join(
        f'<tr><td class="nm">{item["rank"]}. {E(prod.get("title") or "")}</td>'
        f'<td>{E(item.get("voorjou") or "")}</td>'
        f'<td>{E(item.get("letop") or "—")}</td>'
        f'<td class="p">{euro(item.get("price_at_build"))}</td></tr>'
        for item, prod in top3)
    return f'''
  <section class="sect">
    <h2>De top 3 naast elkaar</h2>
    <p class="intro">Als je twijfelt tussen deze drie, dit is het verschil.</p>
    <div class="card cmp"><table>
      <thead><tr><th>Product</th><th>Voor jou als</th><th>Let op</th><th>Vanaf</th></tr></thead>
      <tbody>{rows}</tbody>
    </table></div>
  </section>'''


def faq_block(top3: list) -> str:
    seen, items = set(), []
    for _, prod in top3:
        for v in (((prod.get("review") or {}).get("structured") or {}).get("vragen") or [])[:2]:
            key = v["vraag"].lower()[:40]
            if key in seen:
                continue
            seen.add(key)
            items.append(f'<details><summary>{E(v["vraag"])}</summary>'
                         f'<p>{E(v["antwoord"])}</p></details>')
    if not items:
        return ""
    return f'''
  <section class="sect">
    <h2>Wat kopers vaak vragen</h2>
    <div class="card faq">{"".join(items[:6])}</div>
  </section>'''


def sources_block(top: list) -> str:
    seen, links = set(), []
    for _, prod in top:
        for b in ((prod.get("review") or {}).get("structured") or {}).get("bronnen") or []:
            host = urlsplit(b["url"]).netloc
            host = host[4:] if host.startswith("www.") else host
            if not host or host in seen or "beslist.nl" in host:
                continue                      # onze eigen pagina is geen bron
            seen.add(host)
            links.append(f'<a href="{E(b["url"])}" target="_blank" rel="noopener">{E(host)}</a>')
    if not links:
        return ""
    return f'''
  <section class="sect">
    <h2>Waar deze samenvattingen op gebaseerd zijn</h2>
    <p class="intro">Elke productsamenvatting komt uit tests en koperservaringen van
    deze sites. In totaal {len(links)} verschillende bronnen.</p>
    <div class="card"><div class="bronnen">{"".join(links[:40])}</div></div>
  </section>'''


def build(topic, page_index: int) -> tuple[str, str]:
    exports = sorted((topic.data / "export").glob("*_full.json"))
    if not exports:
        raise SystemExit("geen exportbestand — draai export_top10_data.py eerst")
    data = json.loads(exports[0].read_text(encoding="utf-8"))
    pages, meta, prods = data["terms"], data["meta"], data["products"]
    if not 1 <= page_index <= len(pages):
        raise SystemExit(f"--page moet tussen 1 en {len(pages)} liggen")
    page = pages[page_index - 1]

    top = [(it, prods.get(it["ean"], {})) for it in page["top10"]]
    top3 = top[:3]
    title = f"De {len(top)} beste {page['display']}"

    chips = "".join(
        f'<a class="chip{" is-on" if i == page_index else ""}" href="#">'
        f'{E(p["display"][0].upper() + p["display"][1:])}</a>'
        for i, p in enumerate(pages, 1))

    alts = "".join(alt_card(it, pr, top3, i == 1) for i, (it, pr) in enumerate(top[1:3]))
    rows = "".join(rest_row(it, pr) for it, pr in top[3:])
    first_item, first_prod = top[0]

    body = f'''
<div class="mockbar">
  <b>MOCKUP</b> uit <code>{E(exports[0].name)}</code>
  <span>{meta["counts"]["products"]} producten · {meta["counts"]["reviews"]} reviews ·
  {meta["counts"].get("pages", len(pages))} pagina's · ${meta["cost_usd"].get("total") or "?"}</span>
  <span class="sp"></span>
  <button onclick="document.body.classList.toggle('data')">Datalaag aan/uit</button>
</div>
<header class="top"><div class="top-inner">
  <a class="wordmark" href="#">beslist<span>.nl</span></a>
  <div class="top-kicker">{E(" › ".join(meta["category"]["path"]))}</div>
</div></header>
<main class="page">
  <div class="lead">
    <h1>De {len(top)} beste <em>{E(page["display"])}</em></h1>
    <p class="lead-do">{E(page.get("intro") or "")}</p>
    <p class="stamp">Laatst bijgewerkt: {date.today().strftime("%-d %B %Y")} ·
      {nl(page.get("volume_combined"))} zoekopdrachten per maand</p>
    <p class="lead-why">{E(page.get("methodiek") or "")}</p>
  </div>

  <nav class="chooser">
    <p>Zoek je iets specifieks?</p>
    <div class="chips">{chips}</div>
  </nav>

  <div class="funnel">
    {hero(first_item, first_prod, page.get("volume_combined"))}
    <div class="alts-head">
      <h2>Twee sterke alternatieven</h2>
      <p>Kies een van deze als nummer 1 net niet past.</p>
    </div>
    <div class="alts">{alts}</div>
    <div class="rest-head">
      <h2>De rest van de top {len(top)}</h2>
      <p>Ook goed, maar met een duidelijker "maar".</p>
    </div>
    <div class="rest">{rows}</div>
  </div>

  {compare_table(top3)}
  {faq_block(top3)}
  {sources_block(top)}

  <p class="how">Deze lijst is samengesteld uit {meta["counts"]["reviews"]} productsamenvattingen
  op basis van openbare tests en koperservaringen, het klikgedrag van bezoekers op beslist.nl en
  de actuele prijzen van {sum(len(p.get("live_offers") or []) for _, p in top)} aanbiedingen.
  Prijzen wijzigen; controleer altijd de winkelpagina.</p>
</main>
<div class="keep"><div class="keep-inner">
  {img_tag(first_prod, "")}
  <div>
    <div class="who">Onze keuze</div>
    <div class="nm">{E((first_prod.get("title") or "")[:58])}</div>
  </div>
  <div class="price">{euro(first_item.get("price_at_build"))}</div>
  {cta(first_prod)}
</div></div>'''

    doc = ("<!doctype html>\n<html lang=\"nl\"><head><meta charset=\"utf-8\">"
           "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
           f"<title>{E(title)} — beslist.nl (mockup)</title>{FONT_LINK}"
           f"<style>{CSS}</style></head><body>{body}</body></html>")
    return doc, title


def main() -> int:
    ap = add_topic_arg(argparse.ArgumentParser(description=__doc__))
    ap.add_argument("--page", type=int, default=1, help="welke pagina (1 = hoogste volume)")
    ap.add_argument("--out", help="pad voor het HTML-bestand")
    args = ap.parse_args()
    topic = find_topic(args.topic)

    doc, title = build(topic, args.page)
    out = Path(args.out) if args.out else topic.data / "export" / f"mockup_p{args.page}.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(doc, encoding="utf-8")
    print(f"{title} -> {out}  ({out.stat().st_size/1024:.0f} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
