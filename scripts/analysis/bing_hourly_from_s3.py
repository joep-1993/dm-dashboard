#!/usr/bin/env python3
"""Uurpatroon van de Bing-crawl uit de RUWE CloudFront-logs in S3.

`pa.bothits_*` heeft dagkorrel, dus het effect van een instelling die halverwege een
dag ingaat (Bing Crawl Control, 14-09-2026) is daar niet te zien. Dit script telt per
UUR (UTC, = de klok in de logbestandsnaam) de hits van bingbot / adidxbot / Googlebot,
met 5xx en servetijd erbij, GESPLITST PER HOST.

Let op: distributie E1M5IC93ZML0R0 draagt **www.beslist.be** en E3QQH7GDBASLV1 draagt
www.beslist.nl. `pa.bothits_*` kent alleen de .nl-hosts, dus de absolute aantallen hier
liggen hoger dan in die tabel; vergelijk ratio's, geen niveaus.

Gebruik:
    venv/bin/python scripts/analysis/bing_hourly_from_s3.py --dates 2026-09-13,2026-09-14,2026-09-15
    venv/bin/python scripts/analysis/bing_hourly_from_s3.py --dates 2026-09-15 --hours 0-7
"""
import argparse
import collections
import gzip
import io
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import boto3
from dotenv import load_dotenv

load_dotenv('/home/joepvanschagen/projects/dm-dashboard/.env')

BUCKET = os.getenv("BOTHITS_S3_BUCKET",
                   "production-projectstack-1hts6sh41-logbucketbucket-10tf48d8lt2pt")
PREFIX = os.getenv("BOTHITS_S3_PREFIX", "cloudfront/")
REGION = os.getenv("BOTHITS_S3_REGION", "eu-west-1")
BIG_DISTS = ("E1M5IC93ZML0R0", "E3QQH7GDBASLV1")

# Volgorde telt: adidxbot's UA bevat OOK 'bingbot' (de +http://...bingbot.htm-link),
# dus adidxbot moet eerst matchen.
BOTS = ((b'adidxbot', 'adidxbot'), (b'bingbot', 'bingbot'),
        (b'Googlebot', 'Googlebot'), (b'GoogleOther', 'GoogleOther'))


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
            for page in pag.paginate(Bucket=BUCKET,
                                     Prefix=f"{PREFIX}{dist}.{date}-{hour:02d}."):
                keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def scan(s3, key):
    """(date, hour, bot) -> [hits, 5xx, 4xx, ms] voor één logbestand."""
    out = collections.defaultdict(lambda: [0, 0, 0, 0.0])
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    cols = None
    with gzip.open(io.BytesIO(body), "rb") as f:
        for line in f:
            if line[:1] == b"#":
                if line.startswith(b"#Fields:"):
                    names = line[len(b"#Fields:"):].split()
                    idx = {n.decode(): i for i, n in enumerate(names)}
                    cols = (idx['date'], idx['time'], idx['sc-status'],
                            idx['cs(User-Agent)'], idx['time-taken'],
                            idx['x-host-header'])
                continue
            if not cols:
                continue
            parts = line.split(b"\t")
            try:
                d, t, status, ua, taken, host = (parts[c] for c in cols)
            except IndexError:
                continue
            for token, name in BOTS:
                if token in ua:
                    rec = out[(d.decode(), t[:2].decode(),
                               host.decode().replace('www.', ''), name)]
                    rec[0] += 1
                    if status[:1] == b'5':
                        rec[1] += 1
                    elif status[:1] == b'4':
                        rec[2] += 1
                    try:
                        rec[3] += float(taken)
                    except ValueError:
                        pass
                    break
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="2026-09-13,2026-09-14,2026-09-15")
    ap.add_argument("--hours", default="0-23")
    ap.add_argument("--workers", type=int, default=24)
    a = ap.parse_args()
    lo, _, hi = a.hours.partition("-")
    hours = range(int(lo), int(hi or lo) + 1)

    s3 = client()
    keys = []
    for date in a.dates.split(","):
        keys += list_keys(s3, date, hours, BIG_DISTS)
    print(f"{len(keys)} logbestanden", file=sys.stderr)

    totals = collections.defaultdict(lambda: [0, 0, 0, 0.0])
    done = 0
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        for part in ex.map(lambda k: scan(s3, k), keys):
            for k, v in part.items():
                t = totals[k]
                t[0] += v[0]; t[1] += v[1]; t[2] += v[2]; t[3] += v[3]
            done += 1
            if done % 200 == 0:
                print(f"  {done}/{len(keys)}", file=sys.stderr)

    bots = [n for _, n in BOTS]
    print("datum | uur_utc | host | " + " | ".join(bots)
          + " | bing_5xx | goog_5xx | ms_googlebot | ms_bing")
    for (date, hour, host) in sorted({(d, h, ho) for d, h, ho, _ in totals}):
        row = {b: totals.get((date, hour, host, b), [0, 0, 0, 0.0]) for b in bots}
        bing_hits = row['bingbot'][0] + row['adidxbot'][0]
        bing_ms = row['bingbot'][3] + row['adidxbot'][3]
        goog_hits = row['Googlebot'][0] + row['GoogleOther'][0]
        goog_ms = row['Googlebot'][3] + row['GoogleOther'][3]
        print(" | ".join([date, hour, host] + [str(row[b][0]) for b in bots]
                         + [str(row['bingbot'][1] + row['adidxbot'][1]),
                            str(row['Googlebot'][1] + row['GoogleOther'][1]),
                            str(round(1000 * goog_ms / goog_hits)) if goog_hits else "",
                            str(round(1000 * bing_ms / bing_hits)) if bing_hits else ""]))


if __name__ == "__main__":
    main()
