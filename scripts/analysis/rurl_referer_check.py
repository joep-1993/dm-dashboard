#!/usr/bin/env python3
"""Waar komen de bot-hits op R-urls vandaan? Leest de RUWE CloudFront-logs uit S3.

Dit beantwoordt de vraag die `pa.bothits_*` niet kan beantwoorden: die ingest parst
acht velden en `cs(Referer)` zit daar niet bij. Discovery is dus alleen uit de ruwe
logs te zien.

Wat eruit komt (gemeten 13-09-2026, 10:00-14:00, twee grootste distributies):

  * **99,5-100% van alle bot-hits op /r/ heeft GEEN referer.** Bingbot 100,0%,
    Googlebot 99,5%. Crawlers werken hun eigen wachtrij af; er wordt op dat moment
    geen link gevolgd. Dat is de verklaring voor de willekeurige niet-product-
    zoektermen (`schaatsbaan rotterdam`, `zib polisvoorwaarden`): een backlog van
    jaren in de index van elke bot, die blijft leven omdat wij 200 + index,follow
    blijven geven.
  * **De uitzondering is de CJK-spam**: daar zit wél een referer op, en die wijst
    naar gehackte subdomeinen (`cmscof.y2.cmscof.cmscof.svsmetal.com`,
    `bbs.waav7.formfora.com`, ...). Alle 465 zijn Googlebot, geen enkele Bing.
  * **54% van al het botverkeer op R-urls is CJK-spam** (105.197 van 193.882 hits in
    vier uur, 98.350 unieke URL's), waarvan de helft een **200** krijgt.

TWEE VALKUILEN, allebei zelf ingelopen:

1. `cs-uri-stem` is **dubbel-encoded** voor deze URL's (`%25E5%25B7%259D`). Eén
   `unquote` laat `%E5...` staan en een CJK-regex matcht dan NIETS — de eerste meting
   gaf 0,0% spam. Gebruik `deep_unquote()`.
2. De log bevat per uur meerdere bestanden per distributie; pak ze allemaal, anders
   meet je een willekeurige fractie van het uur.

Gebruik:
    venv/bin/python scripts/analysis/rurl_referer_check.py                  # 13-09, 4 uur
    venv/bin/python scripts/analysis/rurl_referer_check.py --date 2026-09-13 --hours 0-23
    venv/bin/python scripts/analysis/rurl_referer_check.py --out /mnt/c/.../spam_domeinen.csv
"""
import argparse
import collections
import csv
import gzip
import io
import os
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import boto3
from dotenv import load_dotenv

load_dotenv('/home/joepvanschagen/projects/dm-dashboard/.env')

BUCKET = os.getenv("BOTHITS_S3_BUCKET",
                   "production-projectstack-1hts6sh41-logbucketbucket-10tf48d8lt2pt")
PREFIX = os.getenv("BOTHITS_S3_PREFIX", "cloudfront/")
REGION = os.getenv("BOTHITS_S3_REGION", "eu-west-1")
# De twee distributies die het www-verkeer dragen; de andere vier zijn samen <1% van
# het volume (gemeten met --dists all).
BIG_DISTS = ("E1M5IC93ZML0R0", "E3QQH7GDBASLV1")

BOT_RX = re.compile(
    r'bingbot|googlebot|googleother|duckduck|duckassist|bytespider|gptbot|'
    r'oai-searchbot|chatgpt|claudebot|perplexity|applebot|meta-externalagent|yandex',
    re.I)
CJK_RX = re.compile('[　-鿿＀-￯]')
OWN_HOST_RX = re.compile(r'(^|\.)beslist\.(nl|be|de)$', re.I)


def deep_unquote(s, rounds=3):
    """Percent-decode tot het niet meer verandert. Zie valkuil 1 in de docstring."""
    for _ in range(rounds):
        t = urllib.parse.unquote(s)
        if t == s:
            break
        s = t
    return s


def client():
    key = os.getenv("BOTHITS_S3_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
    secret = (os.getenv("BOTHITS_S3_SECRET_ACCESS_KEY")
              or os.getenv("AWS_SECRET_ACCESS_KEY"))
    if not (key and secret):
        raise SystemExit("BOTHITS_S3_ACCESS_KEY_ID / _SECRET_ACCESS_KEY ontbreken in .env")
    return boto3.client("s3", aws_access_key_id=key, aws_secret_access_key=secret,
                        region_name=REGION)


def list_keys(s3, date, hours, dists):
    pag = s3.get_paginator("list_objects_v2")
    keys = []
    for dist in dists:
        for hour in hours:
            prefix = f"{PREFIX}{dist}.{date}-{hour:02d}."
            for page in pag.paginate(Bucket=BUCKET, Prefix=prefix):
                keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def parse(s3, key):
    """Eén logbestand -> [(bot, referer, pad, status, is_cjk)] voor /r/-hits van bots."""
    rows = []
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    with gzip.open(io.BytesIO(body), "rt", encoding="utf-8", errors="replace") as f:
        cols = None
        for line in f:
            if line[:1] == "#":
                if line.startswith("#Fields:"):
                    names = line[len("#Fields:"):].split()
                    idx = {n: i for i, n in enumerate(names)}
                    cols = (idx.get('cs-uri-stem'), idx.get('cs(Referer)'),
                            idx.get('cs(User-Agent)'), idx.get('sc-status'))
                    if cols[1] is None:
                        raise RuntimeError(
                            f"{os.path.basename(key)}: geen cs(Referer)-veld in dit "
                            f"logformaat. Kolommen: {names}")
                continue
            if not cols:
                continue
            parts = line.rstrip("\n").split("\t")
            try:
                stem, ref, ua, status = (parts[c] for c in cols)
            except IndexError:
                continue
            if '/r/' not in stem:
                continue
            m = BOT_RX.search(urllib.parse.unquote_plus(ua))
            if not m:
                continue
            path = deep_unquote(stem)
            rows.append((m.group(0).lower(), ref, path, status,
                         bool(CJK_RX.search(path))))
    return rows


def ref_host(url):
    try:
        return urllib.parse.urlsplit(url).netloc.lower()
    except ValueError:
        return '?'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-09-13")
    ap.add_argument("--hours", default="10-13", help="bv '10-13' of '0-23'")
    ap.add_argument("--dists", default="big", help="'big' (default) of 'all'")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out", help="CSV-pad voor de verwijzende domeinen")
    args = ap.parse_args()

    lo, _, hi = args.hours.partition("-")
    hours = range(int(lo), int(hi or lo) + 1)
    s3 = client()

    dists = BIG_DISTS
    if args.dists == "all":
        pag = s3.get_paginator("list_objects_v2")
        dists = tuple(
            cp["Prefix"][len(PREFIX):].rstrip(".")
            for page in pag.paginate(Bucket=BUCKET, Prefix=PREFIX, Delimiter=".")
            for cp in page.get("CommonPrefixes", [])
            if re.match(r'^E[A-Z0-9]{9,}$', cp["Prefix"][len(PREFIX):].rstrip(".")))

    keys = list_keys(s3, args.date, hours, dists)
    print(f"{args.date} uur {hours.start}-{hours.stop - 1}, {len(dists)} distributies: "
          f"{len(keys)} logbestanden")

    rows = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for got in ex.map(lambda k: parse(s3, k), keys):
            rows += got
    if not rows:
        raise SystemExit("geen bot-hits op /r/ gevonden — klopt de datum?")

    cjk = [r for r in rows if r[4]]
    print(f"\nbot-hits op /r/: {len(rows):,}   waarvan CJK-spam: {len(cjk):,} "
          f"({100.0 * len(cjk) / len(rows):.1f}%)")
    print(f"unieke spam-URL's: {len({r[2] for r in cjk}):,}   "
          f"waarvan met een 200: {len({r[2] for r in cjk if r[3] == '200'}):,}")

    print("\n=== aandeel ZONDER referer per bot (de kernmeting) ===")
    per_bot = collections.defaultdict(collections.Counter)
    for bot, ref, _, _, _ in rows:
        per_bot[bot]['leeg' if ref in ('-', '') else 'ref'] += 1
    for bot, c in sorted(per_bot.items(), key=lambda kv: -sum(kv[1].values())):
        tot = sum(c.values())
        print(f"  {bot:18s} {tot:8,d} hits   {100.0 * c['leeg'] / tot:5.1f}% zonder referer")

    print("\n=== status op CJK-spam per bot ===")
    per_status = collections.defaultdict(collections.Counter)
    for bot, _, _, status, _ in cjk:
        per_status[bot][status] += 1
    for bot, c in sorted(per_status.items(), key=lambda kv: -sum(kv[1].values())):
        print(f"  {bot:14s} {sum(c.values()):7,d}  "
              + "  ".join(f"{s}={n:,}" for s, n in c.most_common(6)))

    ext = collections.Counter()
    ext_bots = collections.defaultdict(collections.Counter)
    ext_cjk = collections.Counter()
    for bot, ref, _, _, is_cjk in rows:
        if ref in ('-', ''):
            continue
        host = ref_host(urllib.parse.unquote_plus(ref))
        if OWN_HOST_RX.search(host):
            continue
        ext[host] += 1
        ext_bots[host][bot] += 1
        if is_cjk:
            ext_cjk[host] += 1

    print(f"\n=== externe verwijzende domeinen: {len(ext)} stuks, "
          f"{sum(ext.values()):,} hits ===")
    for host, n in ext.most_common(40):
        bots = ",".join(b for b, _ in ext_bots[host].most_common(2))
        print(f"  {n:5d}  spam={ext_cjk[host]:5d}  {bots:12s}  {host}")

    if args.out:
        with open(args.out, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["domein", "hits", "hits_naar_spam_url", "bots", "datum", "uren"])
            for host, n in ext.most_common():
                w.writerow([host, n, ext_cjk[host],
                            "|".join(b for b, _ in ext_bots[host].most_common()),
                            args.date, args.hours])
        print(f"\nweggeschreven: {args.out}")


if __name__ == "__main__":
    main()
