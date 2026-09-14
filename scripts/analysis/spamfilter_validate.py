#!/usr/bin/env python3
"""Toets het R-url-spamfilter tegen TWEE ijkpunten.

    python3 spamfilter_validate.py            # gebruikt de cache als die er is
    python3 spamfilter_validate.py --refresh  # haalt het klantverkeer opnieuw op

WAAROM TWEE IJKPUNTEN — lees dit voordat je een regel aanpast.

De eerste versie van dit filter is op 14-09-2026 gevallen omdat hij maar tegen
één ijkpunt was getoetst: `pa.bothits_unknown_daily`. Die tabel bevat per
definitie alleen URL's die NIET in `pa.urls` staan, dus legitieme R-urls zaten
er structureel niet in — "nul valse positieven" kon daar niet anders dan
uitkomen. Getoetst tegen echt bezoekersverkeer bleken de regels 277 zoekopdrachten
van echte klanten te blokkeren: `雅诗兰黛面霜` (Estee Lauder-creme),
`토니스 초콜릿` (Tony's Chocolonely), `بلايستيشن 5` (PlayStation 5),
`45.km.auto`, `inbouw_vaatwasser_52_cm.diep`.

Dus: ijkpunt A is echt mensenverkeer uit `datamart.dim_visit` (Redshift) en dat
is de enige maat die telt voor valse positieven. Ijkpunt B is bevestigde spam
uit bothits en meet alleen de dekking.

HET ONTWERPPRINCIPE. Het onderscheidende kenmerk van spam is niet het schrift en
niet de lengte, maar de PAYLOAD: elke spam-URL draagt een manier om de spammer te
bereiken. Een Chinese klant die naar een creme zoekt heeft die niet. Regels die
op schrift of lengte alleen filteren raken klanten; regels die op de payload
filteren niet.
"""
import argparse
import os
import re
import sys
import unicodedata
import urllib.parse

CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".rurl_real_traffic.tsv")

PG = dict(host="10.1.32.9", port=5432, database="n8n-vector-db", user="dbadmin",
          password="Q9fGRKtUdvdtxsiCM12HeFe0Nki0PvmjZRFLZ9ArmlWdMnDQXX8SdxKnPniqGmq6",
          connect_timeout=15)

# ---------------------------------------------------------------------------
# DE REGELS. Toepassen op de GEDECODEERDE, NFKC-GENORMALISEERDE zoekterm —
# niet op de percent-encodeerde vorm. NFKC maakt van de fullwidth-ontwijking
# `ｍ２２２２．ｖｉｐ` gewoon `m2222.vip`.
# ---------------------------------------------------------------------------

# A — wegwerpdomein als volledig token. De TLD-lijst is gemeten: deze komen nul
#     keer voor in klantverkeer. `.com` staat er bewust NIET in (389 keer echt
#     tegen 10 keer spam: kruidvat.nl, bol.com, blokker.nl zijn echte zoekopdrachten).
REGEL_A = re.compile(
    r'(?<![a-z0-9.-])[a-z0-9][a-z0-9-]{0,30}'
    r'\.(?:vip|cyou|icu|bingo|tw|cc|xyz|pw|su|fun)(?![a-z0-9-])', re.I)

# B — contact-handle gevolgd door minstens 6 CIJFERS. De cijfer-eis is nodig:
#     zonder die eis raakt `whatsapp` een echte telefoonadvertentie
#     (`senifone … whatsapp … sos_functie`) en `telegram` de zoekterm
#     `jamin_chocotelegram_get_well_soon`.
REGEL_B = re.compile(
    r'(?<![a-z])(?:微信|WeChat|QQ|飞机|telegram|whatsapp)(?![a-z])[^0-9]{0,4}\d{6,}', re.I)

# C — CJK-haakpaar met een domein of lang nummer erin. ALLEEN CJK-haken:
#     vierkante haken `[ ]` zitten in echte artikelnummers
#     (`velux_ggl_mk04_[ggl_mk04_207021]`).
_C_PAAR = re.compile(r'[【『❰〖]([^】』❱〗]{2,40})[】』❱〗]')
_C_PAYLOAD = re.compile(r'\.[a-z]{2,6}(?![a-z])|\d{5,}', re.I)

# E — decoratief scheidingsteken. Spam gebruikt deze als scheiding tussen
#     herhaalde zinsdelen; klanten gebruiken ze nooit (0 van 757.571 termen).
REGEL_E = re.compile(r'[▷㊣☞乀→←⇒》《〉〈‹›❰❱❮❯✦★☀☸✿❤▶◀※〖〗]')

# F — Chinese contactmarkering. De `@handle`-variant is eruit gehaald: die
#     raakte `Samsonite_Fold@way`, `ch@t350` en `tempur_pro@_luxe`.
REGEL_F = re.compile(r'网址|網址|加微|微信号|威信|薇信|телеграм')

# D — vangnet voor keyword-gestufte blokken zonder herkenbare payload.
#     40 is gemeten: bij 30 raak je 11 echte klanten, bij 20 raak je er 38.
#     Leestekens (U+2000-U+206F) tellen niet mee.
DREMPEL_NIET_LATIJN = 40


def normaliseer(term):
    return unicodedata.normalize("NFKC", term)


def niet_latijn(term):
    return sum(1 for c in term if ord(c) > 127 and not (0x2000 <= ord(c) <= 0x206F))


def regel_c(term):
    return any(_C_PAYLOAD.search(binnen) for binnen in _C_PAAR.findall(term))


REGELS = (
    ("A wegwerpdomein",  lambda t: bool(REGEL_A.search(t))),
    ("B contact-handle", lambda t: bool(REGEL_B.search(t))),
    ("C haak + payload", regel_c),
    ("D >=40 niet-Latijn", lambda t: niet_latijn(t) >= DREMPEL_NIET_LATIJN),
    ("E decoratief teken", lambda t: bool(REGEL_E.search(t))),
    ("F 网址 / 加微",     lambda t: bool(REGEL_F.search(t))),
)


def blokkeren(term):
    """True als deze zoekterm een 410 hoort te krijgen."""
    t = normaliseer(term)
    return any(fn(t) for _, fn in REGELS)


# ---------------------------------------------------------------------------
# Ijkpunten
# ---------------------------------------------------------------------------

# Onafhankelijk spam-label voor ijkpunt B. Bewust ALLEEN ondubbelzinnige markers:
# `slot` stond hier eerst in (voor gokspam) en matchte `fietsslot`, `waterslot`
# en `kinderslot`; `按摩` (massage) is een echt product. Zelfde klasse fout als
# `bet` in `beton`.
SPAM_LABEL = re.compile(
    r'ｙｕｅ|ｓｍ６|ｍ１６|微信|找小姐|约炮|外围女|楼凤|高仿|a货|乱伦|嫩穴|鬼父'
    r'|线上投注|电竞馆|谷歌|留痕|蜘蛛池|快餐多少钱'
    r'|\.(?:vip|cyou|bingo|icu)\b|kiếm\+tiền|gacor|togel|situs\+', re.I)


def decodeer(url, passes=3):
    for _ in range(passes):
        nieuw = urllib.parse.unquote(url, errors="replace")
        if nieuw == url:
            break
        url = nieuw
    return url


def zoekterm(pad):
    d = decodeer(pad)
    return (d.split("/r/", 1)[1] if "/r/" in d else "").rstrip("/")


def haal_klantverkeer(refresh=False):
    """Ijkpunt A — R-urls waar echte mensen op zijn geland (Redshift)."""
    if os.path.exists(CACHE) and not refresh:
        with open(CACHE, encoding="utf-8") as f:
            return [(int(v), t) for t, v in (r.rstrip("\n").split("\t") for r in f if "\t" in r)]

    sys.path.insert(0, os.path.expanduser("~/.claude/skills/beslist-query/scripts"))
    import psycopg2
    env = os.path.expanduser("~/.claude/skills/beslist-query/.env")
    cfg = {}
    with open(env) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, val = line.split("=", 1)
                cfg[k] = val.strip().strip("'\"")
    conn = psycopg2.connect(host=cfg["REDSHIFT_HOST"], port=cfg.get("REDSHIFT_PORT", "5439"),
                            database=cfg["REDSHIFT_DATABASE"], user=cfg["REDSHIFT_USER"],
                            password=cfg["REDSHIFT_PASSWORD"])
    cur = conn.cursor()
    cur.execute("""
        SELECT dv.url, count(*) AS visits
        FROM datamart.dim_visit dv
        WHERE dv.year = '2026' AND dv.month IN ('08', '09') AND dv.deleted_ind = 0
          AND dv.url LIKE '%/r/%'
        GROUP BY 1
    """)
    rijen = []
    with open(CACHE, "w", encoding="utf-8") as f:
        for url, visits in cur.fetchall():
            term = zoekterm(urllib.parse.urlsplit(url).path)
            if not term or SPAM_LABEL.search(term):
                continue          # spam die toevallig ook bezoek kreeg telt niet mee
            rijen.append((int(visits), term))
            f.write("%s\t%d\n" % (term.replace("\t", " ").replace("\n", " "), visits))
    conn.close()
    return rijen


def haal_spam():
    """Ijkpunt B — door crawlers opgehaalde URL's met een onafhankelijk spam-signaal."""
    import psycopg2
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    cur.execute("""SELECT url FROM pa.bothits_unknown_daily
                   WHERE log_date >= '2026-08-20' AND url_type = 'search' GROUP BY 1""")
    termen = []
    for (url,) in cur.fetchall():
        t = zoekterm(url)
        if t and SPAM_LABEL.search(t):
            termen.append(t)
    conn.close()
    return termen


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true",
                    help="klantverkeer opnieuw uit Redshift halen in plaats van uit de cache")
    args = ap.parse_args()

    klanten = haal_klantverkeer(args.refresh)
    spam = haal_spam()
    klant_visits = sum(v for v, _ in klanten)

    print("IJKPUNT A  echte klanten : %7d zoektermen (%s visits, aug+sep 2026)"
          % (len(klanten), "{:,}".format(klant_visits)))
    print("IJKPUNT B  bevestigde spam: %7d zoektermen (bothits, vanaf 20-08)\n" % len(spam))

    print("%-20s %14s %16s %9s" % ("regel", "spam", "echte termen", "visits"))
    for naam, fn in REGELS:
        sp = sum(1 for t in spam if fn(normaliseer(t)))
        fp = [x for x in klanten if fn(normaliseer(x[1]))]
        print("%-20s %7d/%-6d %16d %9d"
              % (naam, sp, len(spam), len(fp), sum(x[0] for x in fp)))

    gevangen = sum(1 for t in spam if blokkeren(t))
    fout = [x for x in klanten if blokkeren(x[1])]
    fout_visits = sum(x[0] for x in fout)
    print("\nSAMEN  spam gevangen : %d/%d = %.1f%%" % (gevangen, len(spam), 100 * gevangen / len(spam)))
    print("       klanten geraakt: %d termen, %d visits (%.6f%% van het verkeer)"
          % (len(fout), fout_visits, 100 * fout_visits / klant_visits))
    for visits, term in sorted(fout, reverse=True)[:20]:
        print("         %4dv  %s" % (visits, term[:88]))

    # Valse positieven zijn de harde grens: een geblokkeerde klant is erger dan
    # doorgelaten spam, want de spam vangt laag 2 (nul treffers -> noindex) alsnog.
    return 1 if fout_visits > 5 else 0


if __name__ == "__main__":
    sys.exit(main())
