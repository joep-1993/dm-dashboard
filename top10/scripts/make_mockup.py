#!/usr/bin/env python3
"""Bouwt een zelfstandige HTML-mockup van één top-10 pagina uit het exportbestand.

Leest alleen het contractbestand — geen netwerk, geen API. Bedoeld om te zien
of de gegenereerde data een pagina oplevert die klopt, niet als productiecode.

Naast de consumentenpagina zit er een datalaag in (knop rechtsboven): per
product de kliks, het A-label, de pixelsessies en het aantal bronnen. Dat is
interne data die nooit op een echte pagina hoort, maar wel precies waar je naar
kijkt als je beoordeelt of de ranking te vertrouwen is.

    python top10/scripts/make_mockup.py --topic tandenborstels [--page 1] [--out pad.html]
"""
from __future__ import annotations

import argparse
import html
import json
import sys
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared.topic import add_topic_arg, find_topic                     # noqa: E402

E = html.escape


def nl(n) -> str:
    """1234567 -> 1.234.567 (Nederlandse duizendtallen)."""
    return f"{int(n or 0):,}".replace(",", ".")


def euro(v) -> str:
    return f"€ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if v else "—"


def card(item: dict, prod: dict, show_all: bool) -> str:
    rev = (prod.get("review") or {}).get("structured") or {}
    com = prod.get("commerce") or {}
    score_block = com.get("ean_score") or {}
    tag = com.get("tag") or {}
    offers = prod.get("live_offers") or []
    best = offers[0] if offers else None
    others = offers[1:4]

    pluses = "".join(f"<li>{E(p)}</li>" for p in item.get("pluses") or [])
    minus = f'<p class="letop"><span>Let op</span> {E(item["letop"])}</p>' if item.get("letop") else ""
    vragen = "".join(
        f'<details><summary>{E(v["vraag"])}</summary><p>{E(v["antwoord"])}</p></details>'
        for v in (rev.get("vragen") or [])[:3])
    def bron_label(b: dict) -> str:
        # Lang niet elke bron heeft een titel; een kale URL leest slecht, dus
        # dan het domein zonder 'www.'.
        if b.get("title"):
            return b["title"]
        # urlsplit en niet zelf op "//" splitsen: een URL die op "////" eindigt
        # (de beslist.nl-PLP doet dat) geeft dan een lege host.
        host = urlsplit(b["url"]).netloc
        return host[4:] if host.startswith("www.") else host or b["url"][:40]

    bronnen = "".join(
        f'<a href="{E(b["url"])}" target="_blank" rel="noopener">{E(bron_label(b))}</a>'
        for b in (rev.get("bronnen") or [])[:6])
    shops = "".join(
        f'<li><span>{E(o["shop_name"])}</span><b>{euro(o["price"])}</b></li>' for o in others)

    evidence = item.get("evidence") or "—"
    ev_class = {"sterk": "ev-strong", "redelijk": "ev-mid", "beperkt": "ev-weak"}.get(evidence, "ev-mid")

    data_layer = f'''
      <div class="datalayer">
        <span title="Outclicks, 90 dagen">kliks <b>{prod.get("clicks", 0)}</b></span>
        <span title="Het label dat Beslist zelf aan dit EAN geeft">A-label <b>{E(str(score_block.get("ean_score_label", "—")))}</b></span>
        <span title="Totale EAN-score">score <b>{score_block.get("totaal_ean_score", "—")}</b></span>
        <span title="Winkels met dit EAN">winkels <b>{score_block.get("shops_scored", len(offers))}</b></span>
        <span title="Sessies over 365 dagen, alleen winkels die onze tag draaien">sessies <b>{tag.get("total_session_starts_365") or "—"}</b></span>
        <span title="Aantal bronnen in de review">bronnen <b>{len(rev.get("bronnen") or [])}</b></span>
        <span title="Kwaliteitsscore van het model">model <b>{item.get("quality_score", "—")}</b></span>
      </div>'''

    return f'''
    <article class="card{' open' if show_all else ''}">
      <div class="rank">{item["rank"]}</div>
      <div class="body">
        <h3>{E(prod.get("title") or "")}</h3>
        <p class="verdict">{E(item.get("verdict") or "")}</p>
        <ul class="pluses">{pluses}</ul>
        {minus}
        <p class="voorjou">{E(item.get("voorjou") or "")}</p>
        <div class="meta">
          <span class="badge {ev_class}">bewijs: {E(evidence)}</span>
          <span class="badge">{len(offers)} winkel{"s" if len(offers) != 1 else ""}</span>
        </div>
        {f'<div class="qa">{vragen}</div>' if vragen else ''}
        {f'<div class="bronnen"><span>Bronnen</span>{bronnen}</div>' if bronnen else ''}
        {data_layer}
      </div>
      <aside class="buy">
        <div class="price">{euro(item.get("price_at_build") or prod.get("min_price_at_build"))}</div>
        {f'<div class="at">bij {E(best["shop_name"])}</div>' if best else ''}
        <a class="cta" href="{E((best or {}).get("url") or "#")}" target="_blank" rel="noopener">Bekijk aanbieding</a>
        {f'<ul class="shops">{shops}</ul>' if shops else ''}
      </aside>
    </article>'''


def build(topic, page_index: int, show_all: bool) -> tuple[str, str]:
    exports = sorted((topic.data / "export").glob("*_full.json"))
    if not exports:
        raise SystemExit("geen exportbestand — draai export_top10_data.py eerst")
    data = json.loads(exports[0].read_text(encoding="utf-8"))
    pages = data["terms"]
    if not 1 <= page_index <= len(pages):
        raise SystemExit(f"--page moet tussen 1 en {len(pages)} liggen")
    page = pages[page_index - 1]
    prods = data["products"]
    meta = data["meta"]

    cards = "".join(card(it, prods.get(it["ean"], {}), show_all) for it in page["top10"])
    also = ", ".join(E(x["term"]) for x in page.get("also_targets") or [])
    others = "".join(
        f'<li{" class=\'current\'" if i == page_index else ""}>Beste {E(p["display"])}'
        f'<span>{nl(p.get("volume_combined"))}</span></li>'
        for i, p in enumerate(pages, 1))

    title = f"Beste {page['display']}"
    doc = f"""<!doctype html>
<html lang="nl"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{E(title)} — mockup</title>
<style>
  :root {{
    --ink:#161320; --muted:#5d5870; --line:#e6e3ee; --bg:#f7f6fa; --card:#fff;
    --brand:#5e4a90; --brand-soft:#efeaf8; --accent:#f0a400; --good:#1f7a4d;
  }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; background:var(--bg); color:var(--ink);
    font:16px/1.55 "Segoe UI",system-ui,-apple-system,sans-serif; }}
  .mockbar {{ background:var(--ink); color:#fff; font-size:12.5px; padding:7px 18px;
    display:flex; gap:14px; align-items:center; flex-wrap:wrap; }}
  .mockbar b {{ background:var(--accent); color:#161320; padding:1px 7px; border-radius:4px; }}
  .mockbar .sp {{ margin-left:auto; }}
  .mockbar button {{ background:#2c2740; color:#fff; border:1px solid #453e60;
    border-radius:6px; padding:4px 11px; font-size:12.5px; cursor:pointer; }}
  .mockbar button:hover {{ background:#3a3455; }}
  header.page {{ background:#fff; border-bottom:1px solid var(--line); padding:30px 18px 24px; }}
  .wrap {{ max-width:1060px; margin:0 auto; }}
  .crumbs {{ color:var(--muted); font-size:13px; margin-bottom:10px; }}
  h1 {{ margin:0 0 10px; font-size:31px; letter-spacing:-.4px; }}
  .lead {{ color:var(--muted); max-width:70ch; margin:0 0 16px; }}
  .facts {{ display:flex; gap:22px; flex-wrap:wrap; font-size:13px; color:var(--muted);
    border-top:1px solid var(--line); padding-top:14px; }}
  .facts b {{ color:var(--ink); }}
  main {{ padding:26px 18px 60px; }}
  .layout {{ display:grid; grid-template-columns:1fr 260px; gap:28px; align-items:start; }}
  @media (max-width:900px) {{ .layout {{ grid-template-columns:1fr; }} }}
  .method {{ background:var(--brand-soft); border-left:3px solid var(--brand);
    padding:13px 16px; border-radius:0 8px 8px 0; font-size:14px; margin:0 0 20px; }}
  .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px;
    display:grid; grid-template-columns:54px 1fr 210px; gap:0; margin-bottom:14px; overflow:hidden; }}
  @media (max-width:780px) {{ .card {{ grid-template-columns:44px 1fr; }} .buy {{ grid-column:1/-1; }} }}
  .rank {{ background:var(--brand); color:#fff; font-size:20px; font-weight:700;
    display:flex; align-items:flex-start; justify-content:center; padding-top:16px; }}
  .card:nth-child(n+4) .rank {{ background:#8478ab; }}
  .body {{ padding:16px 18px; min-width:0; }}
  .body h3 {{ margin:0 0 6px; font-size:17px; line-height:1.3; }}
  .verdict {{ margin:0 0 10px; }}
  ul.pluses {{ margin:0 0 8px; padding-left:18px; }}
  ul.pluses li {{ margin-bottom:2px; }}
  ul.pluses li::marker {{ color:var(--good); }}
  .letop {{ margin:0 0 8px; font-size:14.5px; color:#7a3b12; }}
  .letop span {{ background:#fdf0dd; border-radius:4px; padding:1px 6px; font-weight:600; margin-right:6px; }}
  .voorjou {{ margin:0 0 10px; font-style:italic; color:var(--muted); }}
  .meta {{ display:flex; gap:8px; flex-wrap:wrap; }}
  .badge {{ font-size:12px; background:#f2f0f7; border-radius:20px; padding:3px 10px; color:var(--muted); }}
  .ev-strong {{ background:#e5f4ec; color:var(--good); }}
  .ev-mid {{ background:#fdf3e0; color:#8a5b12; }}
  .ev-weak {{ background:#fbeaea; color:#9d3232; }}
  .qa {{ margin-top:12px; border-top:1px solid var(--line); padding-top:10px; }}
  .qa details {{ font-size:14.5px; margin-bottom:5px; }}
  .qa summary {{ cursor:pointer; color:var(--brand); }}
  .qa p {{ margin:5px 0 0; color:var(--muted); }}
  .bronnen {{ margin-top:11px; font-size:12.5px; display:flex; gap:8px; flex-wrap:wrap; align-items:baseline; }}
  .bronnen span {{ color:var(--muted); }}
  .bronnen a {{ color:var(--brand); text-decoration:none; background:#f2f0f7;
    padding:2px 8px; border-radius:12px; max-width:230px; overflow:hidden;
    text-overflow:ellipsis; white-space:nowrap; }}
  .datalayer {{ display:none; margin-top:12px; padding-top:10px; border-top:1px dashed var(--line);
    gap:7px; flex-wrap:wrap; font-size:12px; color:var(--muted); }}
  .card.open .datalayer {{ display:flex; }}
  .datalayer span {{ background:#161320; color:#cfc9e0; border-radius:5px; padding:2px 8px; }}
  .datalayer b {{ color:#fff; }}
  .buy {{ border-left:1px solid var(--line); padding:16px; background:#fcfcfe; }}
  .price {{ font-size:23px; font-weight:700; }}
  .at {{ font-size:12.5px; color:var(--muted); margin-bottom:11px; }}
  .cta {{ display:block; text-align:center; background:var(--accent); color:#161320;
    font-weight:600; text-decoration:none; border-radius:7px; padding:9px; font-size:14.5px; }}
  ul.shops {{ list-style:none; margin:12px 0 0; padding:11px 0 0; border-top:1px solid var(--line);
    font-size:13px; }}
  ul.shops li {{ display:flex; justify-content:space-between; padding:2px 0; color:var(--muted); }}
  aside.side {{ position:sticky; top:16px; }}
  .box {{ background:#fff; border:1px solid var(--line); border-radius:12px; padding:15px; margin-bottom:14px; }}
  .box h4 {{ margin:0 0 9px; font-size:14px; text-transform:uppercase; letter-spacing:.4px; color:var(--muted); }}
  .box ul {{ list-style:none; margin:0; padding:0; font-size:14px; }}
  .box li {{ display:flex; justify-content:space-between; gap:10px; padding:5px 0;
    border-bottom:1px solid var(--line); }}
  .box li:last-child {{ border-bottom:0; }}
  .box li span {{ color:var(--muted); font-size:12.5px; }}
  .box li.current {{ font-weight:600; color:var(--brand); }}
</style></head>
<body>
<div class="mockbar">
  <b>MOCKUP</b> gegenereerd uit <code>{E(exports[0].name)}</code>
  <span>{meta['counts']['products']} producten · {meta['counts']['reviews']} reviews · {meta['counts'].get('pages', len(pages))} pagina's · ${meta['cost_usd'].get('total') or '?'}</span>
  <span class="sp"></span>
  <button onclick="document.querySelectorAll('.card').forEach(c=>c.classList.toggle('open'))">Datalaag aan/uit</button>
</div>
<header class="page"><div class="wrap">
  <div class="crumbs">{E(" › ".join(meta["category"]["path"]))}</div>
  <h1>{E(title)}</h1>
  <p class="lead">{E(page.get("intro") or "")}</p>
  <div class="facts">
    <div>zoekvolume <b>{nl(page.get('volume_combined'))}</b>/maand</div>
    <div>producten vergeleken <b>{meta['counts']['products']}</b></div>
    <div>reviews gelezen <b>{meta['counts']['reviews']}</b></div>
    {f'<div>ook voor: <b>{also}</b></div>' if also else ''}
  </div>
</div></header>
<main><div class="wrap layout">
  <div>
    <p class="method">{E(page.get("methodiek") or "")}</p>
    {cards}
  </div>
  <aside class="side">
    <div class="box"><h4>Andere lijsten</h4><ul>{others}</ul></div>
    <div class="box"><h4>Hoe deze lijst werkt</h4>
      <p style="font-size:13.5px;color:var(--muted);margin:0">
      Elk product is samengevat uit echte reviews en tests van het web. De volgorde
      weegt die reviews, het klikgedrag van bezoekers en de populariteit op onze
      site. Prijzen komen live uit het winkelaanbod.</p></div>
  </aside>
</div></main>
</body></html>"""
    return doc, title


def main() -> int:
    ap = add_topic_arg(argparse.ArgumentParser(description=__doc__))
    ap.add_argument("--page", type=int, default=1, help="welke pagina (1 = hoogste volume)")
    ap.add_argument("--out", help="pad voor het HTML-bestand")
    ap.add_argument("--data-layer", action="store_true", help="datalaag meteen open")
    args = ap.parse_args()
    topic = find_topic(args.topic)

    doc, title = build(topic, args.page, args.data_layer)
    out = Path(args.out) if args.out else topic.data / "export" / f"mockup_p{args.page}.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(doc, encoding="utf-8")
    print(f"{title} -> {out}  ({out.stat().st_size/1024:.0f} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
