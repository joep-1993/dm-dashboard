"""OLD-vs-NEW regressieharnas voor process_file() — de poort voor elke
wijziging in de hot loop.

Draait de parser serieel over een vaste set echte .gz-logbestanden en schrijft een
deterministische vingerafdruk: md5 over de gesorteerde cube-, known- en unknown-items
plus de rauwe tellers. Twee runs met identieke uitkomst => de hot loop is byte-voor-byte
onveranderd.

Gebruik (baseline VOOR de wijziging, daarna nog eens en diffen):
    PYTHONPATH=. ./venv/bin/python scripts/bothits_parse_fingerprint.py /tmp/old.json 24
    # ... wijzig process_file ...
    PYTHONPATH=. ./venv/bin/python scripts/bothits_parse_fingerprint.py /tmp/new.json 24
    python3 -c "import json;a=json.load(open('/tmp/old.json'));b=json.load(open('/tmp/new.json'));print('IDENTIEK' if a==b else 'VERSCHIL')"

BOTHITS_FP_SRC wijst naar een map met echte .gz-logs; default is de staging van
2026-08-12. Let op de staging-retentie (21 dagen) — pak een datum die er nog staat.
"""
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.bothits_ingest import (  # noqa: E402
    process_file, load_url_ids, URL_IDS,
)
from backend.bothits_verify import load as load_ip_ranges  # noqa: E402

SRC = os.getenv("BOTHITS_FP_SRC",
                os.path.expanduser("~/bothits_s3/_processed/2026-08-12"))


def fingerprint(out_path, n_files=12):
    load_url_ids()
    import backend.bothits_ingest as bi
    assert len(bi.URL_IDS) > 900_000, f"URL_IDS te klein: {len(bi.URL_IDS)}"
    ok = load_ip_ranges()
    print(f"URL_IDS={len(bi.URL_IDS):,}  ip_ranges_loaded={ok}")

    # Grootste bestanden PER DISTRIBUTIE. Alfabetisch sorteren levert de kruimels op
    # (480 B, 17 regels, nul bots); en één distributie pakken mist de hosts die
    # skip_host juist moet wegfilteren. Zo zit elke distributie erin met volume.
    allgz = [f for f in os.listdir(SRC) if f.endswith(".gz")]
    by_dist = {}
    for f in allgz:
        by_dist.setdefault(f.split(".", 1)[0], []).append(f)
    per_dist = max(1, n_files // max(1, len(by_dist)))
    files = []
    for dist in sorted(by_dist):
        biggest = sorted(by_dist[dist],
                         key=lambda f: os.path.getsize(os.path.join(SRC, f)),
                         reverse=True)[:per_dist]
        files.extend(sorted(biggest))
    files = sorted(files)
    assert files, f"geen .gz in {SRC}"
    print(f"steekproef: {len(files)} bestanden over {len(by_dist)} distributies, "
          f"{sum(os.path.getsize(os.path.join(SRC, f)) for f in files)/1e6:.1f} MB")

    tot_cube, tot_known, tot_unknown = {}, {}, {}
    raw_total = bot_total = 0
    per_file = []

    failed_total = 0
    for fn in files:
        cube, known, unknown, raw, bot, failed = process_file(os.path.join(SRC, fn))
        raw_total += raw
        bot_total += bot
        failed_total += failed
        per_file.append({"file": fn, "raw": raw, "bot": bot,
                         "cube": len(cube), "known": len(known),
                         "unknown": len(unknown)})
        for k, v in cube.items():
            t = tot_cube.setdefault(k, [0, 0, 0])
            for i in range(3):
                t[i] += v[i]
        for k, v in known.items():
            t = tot_known.setdefault(k, [0, 0, 0, 0, 0, 0])
            for i in range(6):
                t[i] += v[i]
        for k, v in unknown.items():
            tot_unknown[k] = tot_unknown.get(k, 0) + v

    def md5_of(d):
        h = hashlib.md5()
        for k in sorted(d, key=repr):
            h.update(repr(k).encode()); h.update(repr(d[k]).encode())
        return h.hexdigest()

    res = {
        "files": len(files),
        "raw_lines": raw_total,
        "bot_lines": bot_total,
        "failed_files": failed_total,
        "cube_rows": len(tot_cube),
        "known_rows": len(tot_known),
        "unknown_rows": len(tot_unknown),
        "md5_cube": md5_of(tot_cube),
        "md5_known": md5_of(tot_known),
        "md5_unknown": md5_of(tot_unknown),
        "per_file": per_file,
    }
    json.dump(res, open(out_path, "w"), indent=1)
    for k in ("files", "raw_lines", "bot_lines", "cube_rows", "known_rows",
              "unknown_rows", "md5_cube", "md5_known", "md5_unknown"):
        print(f"  {k:<12} {res[k]}")
    return res


if __name__ == "__main__":
    fingerprint(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 12)
