"""Bot Hits — controleer of een bot-IP echt bij de operator hoort die de
user-agent claimt, puur op de OFFICIEEL gepubliceerde IP-ranges.

Herkomst: `~/bothits_verify.py` uit de oude CSV-pijplijn. Daarvan is bewust
alléén de RANGE_SOURCES-tabel overgenomen (Joep, 2026-08-11). Wat er niet in
komt, en waarom:

  * **Geen reverse-DNS.** Dat was in het oude script het dure deel (8s timeout,
    64 threads) en het is meetbaar onnodig: op 2026-03-10 viel 100% van de hits
    van Googlebot, GoogleOther, Bing én Apple binnen de gepubliceerde ranges.
    Een prefix-check op de ~17.500 unieke IP's van een dag kost microseconden.
  * **Geen keep-set van IP's.** Het oude script gooide niet-geverifieerd verkeer
    weg. Dat willen we hier niet: de spoof-graad is 0,4% van de hits (en 98%
    daarvan is OpenAI), dus filteren zou geen cijfer noemenswaardig veranderen
    terwijl het wel data kost. De uitkomst wordt een dimensie op de cube, zodat
    "failed" een tripwire is in plaats van een stille correctie.
  * **Geen BOT_OPERATOR uit dat script.** Die dekte 11 botnamen en bevatte
    Googlebot noch bingbot — alleen `Google-Extended`, dat hier 0 hits heeft. Het
    zou dus precies de 69% Google-traffic ongemoeid laten. Hieronder wordt op
    FAMILIE gemapt, dezelfde sleutel die de parser al kent.

Vier uitkomsten, en het verschil tussen de laatste twee is belangrijk:

    verified      IP valt binnen een gepubliceerde range van de geclaimde operator
    failed        operator publiceert ranges, dit IP zit er niet in -> tripwire
    unverifiable  operator publiceert géén ranges (Meta, ByteDance, Amazon,
                  CommonCrawl, de SEO-tools, other-bot). Niet verdacht, niet te
                  controleren — en dat eerlijk labelen is beter dan gokken.
    unchecked     de lijsten waren deze run niet op te halen. NOOIT 'failed',
                  want een kapotte fetch mag echte Googlebot niet als spoof
                  wegzetten.

Meta valt in `unverifiable` omdat hun endpoint sinds kort 404 geeft (ze
publiceren via whois op AS32934). Dat is 7,39% van het verkeer, dus die
categorie is geen afrondingsfout.
"""
import bisect
import ipaddress
import json
import logging
import os
import time
import urllib.request

logger = logging.getLogger(__name__)

# Overgenomen uit ~/bothits_verify.py, aangevuld met de drie bronnen die daar
# ontbraken maar wel de grootste crawlers dekken: bingbot, Applebot en Googles
# user-triggered fetchers. Alle tien zijn op 2026-08-11 opgehaald en leverden
# samen 1.459 prefixes.
RANGE_SOURCES = {
    "google": [
        "https://developers.google.com/static/search/apis/ipranges/googlebot.json",
        "https://developers.google.com/static/search/apis/ipranges/special-crawlers.json",
        "https://developers.google.com/static/search/apis/ipranges/user-triggered-fetchers-google.json",
    ],
    "bing": ["https://www.bing.com/toolbox/bingbot.json"],
    "openai": [
        "https://openai.com/gptbot.json",
        "https://openai.com/chatgpt-user.json",
        "https://openai.com/searchbot.json",
    ],
    "anthropic": ["https://claude.com/crawling/bots.json"],
    "perplexity": [
        "https://www.perplexity.ai/perplexitybot.json",
        "https://www.perplexity.ai/perplexity-user.json",
    ],
    "apple": ["https://search.developer.apple.com/applebot.json"],
}

# Familie (zoals de parser hem noemt) -> operator. Wat hier niet in staat is
# 'unverifiable': er is geen publieke lijst om tegen te toetsen.
FAMILY_OPERATOR = {
    "Googlebot": "google", "GoogleOther": "google", "Google-AI": "google",
    "Bing": "bing",
    "OpenAI": "openai",
    "Anthropic": "anthropic",
    "Perplexity": "perplexity",
    "Apple": "apple", "Apple-AI": "apple",
}

CACHE = os.path.expanduser(os.getenv("BOTHITS_RANGE_CACHE", "~/.cache/bothits/ipranges.json"))
CACHE_TTL_H = int(os.getenv("BOTHITS_RANGE_TTL_H", "24"))
HDR = {"User-Agent": "Beslist script voor SEO"}

# operator -> (starts, ends) als gesorteerde int-lijsten, per adresfamilie.
# Bisect in plaats van een lus over ~1.500 netwerken: dat laatste is 17.500 IP's
# x 1.500 = 26M containment-checks per dag, dit is O(log n) per IP.
_TABLE = {}
_loaded = False
_memo = {}


def _fetch_all():
    """-> ({operator: [cidr]}, alles_gelukt).

    ALL-OR-NOTHING PER OPERATOR, en dat is de hele bedoeling (fase 3 van de audit).
    Hier stond `if cidrs: out[op] = cidrs`, oftewel "eentje is genoeg": viel bij Google
    één van de drie bronnen weg, dan kwam die operator met een INCOMPLETE prefixlijst
    binnen en kreeg elk echt crawler-IP daarbuiten `failed`. Bij bing, anthropic en
    apple is er maar één bron-URL, dus daar kantelde de hele familie in één keer.

    Dat is precies wat de ontwerpnotitie bovenaan verbiedt: een mislukte fetch mag echte
    Googlebot nooit als spoof wegzetten. Een operator die niet compleet binnenkomt gaat
    er nu helemaal uit, en `verdict()` geeft voor zo'n operator `unchecked`.
    """
    out, all_ok = {}, True
    for op, urls in RANGE_SOURCES.items():
        cidrs, ok = [], True
        for u in urls:
            try:
                raw = urllib.request.urlopen(
                    urllib.request.Request(u, headers=HDR), timeout=20).read()
                for p in json.loads(raw).get("prefixes", []):
                    cidr = p.get("ipv4Prefix") or p.get("ipv6Prefix")
                    if cidr:
                        cidrs.append(cidr)
            except Exception as exc:
                logger.warning("bothits verify: %s niet op te halen: %s", u, exc)
                ok = False
        if not ok or not cidrs:
            all_ok = False
            logger.error("bothits verify: operator %s onvolledig opgehaald (%s van %s "
                         "bronnen) — die familie wordt deze run 'unchecked' i.p.v. "
                         "onterecht 'failed'", op, "0" if not cidrs else "<alle>",
                         len(urls))
            continue
        out[op] = cidrs
    return out, all_ok


def _load_cidrs(force=False):
    """Prefixes uit de cache of van de bronnen. -> {operator: [cidr]} of {}."""
    if not force and os.path.exists(CACHE):
        try:
            age_h = (time.time() - os.path.getmtime(CACHE)) / 3600
            data = json.load(open(CACHE))
            if age_h < CACHE_TTL_H and data:
                return data
        except Exception:
            pass
    data, all_ok = _fetch_all()
    if data and all_ok:
        try:
            os.makedirs(os.path.dirname(CACHE), exist_ok=True)
            json.dump(data, open(CACHE, "w"))
        except Exception as exc:
            logger.warning("bothits verify: cache niet te schrijven: %s", exc)
        return data
    # Onvolledig opgehaald: een VERLOPEN cache is beter dan een halve verse lijst.
    # Ranges verschuiven met weken, niet met uren, dus oude prefixes leveren hooguit
    # een handvol verkeerde verdicts op — terwijl een half opgehaalde operator een hele
    # familie op 'failed' zet. En bewust NIET wegschrijven: dan zou die halve lijst zich
    # 24 uur lang als waarheid voordoen.
    if os.path.exists(CACHE):
        try:
            cached = json.load(open(CACHE))
            if cached:
                logger.warning("bothits verify: terugval op de cache (%s operators); de "
                               "verse ophaal was onvolledig", len(cached))
                return cached
        except Exception:
            pass
    if data:
        logger.warning("bothits verify: geen cache, dus verder met %s van %s operators; "
                       "de rest wordt 'unchecked'", len(data), len(RANGE_SOURCES))
    return data


def load(force=False):
    """Bouw de opzoektabel. Aanroepen in de PARENT vóór het forken, zoals
    load_url_ids(): de workers erven hem dan via fork in plaats van hem twaalf
    keer op te halen."""
    global _TABLE, _loaded
    cidrs = _load_cidrs(force)
    table = {}
    for op, lst in cidrs.items():
        per_ver = {4: [], 6: []}
        for cidr in lst:
            try:
                net = ipaddress.ip_network(cidr)
            except ValueError:
                continue
            per_ver[net.version].append((int(net.network_address),
                                         int(net.broadcast_address)))
        entry = {}
        for ver, iv in per_ver.items():
            iv.sort()
            entry[ver] = ([s for s, _e in iv], [e for _s, e in iv])
        table[op] = entry
    _TABLE = table
    _loaded = bool(table)
    _memo.clear()
    n = sum(len(v[4][0]) + len(v[6][0]) for v in table.values())
    logger.info("bothits verify: %s operators, %s prefixes%s",
                len(table), n, "" if _loaded else " (LEEG — alles wordt 'unchecked')")
    return _loaded


def _in(op, ip_obj):
    entry = _TABLE.get(op)
    if not entry:
        return False
    starts, ends = entry.get(ip_obj.version, ([], []))
    if not starts:
        return False
    n = int(ip_obj)
    # Laatste range die op of vóór n begint; die moet n dan ook omvatten.
    i = bisect.bisect_right(starts, n) - 1
    return i >= 0 and n <= ends[i]


def verdict(ip, family):
    """-> 'verified' | 'failed' | 'unverifiable' | 'unchecked'."""
    op = FAMILY_OPERATOR.get(family)
    if op is None:
        return "unverifiable"
    if not _loaded:
        return "unchecked"
    if op not in _TABLE:
        # De operator publiceert wél een lijst, maar die is deze run niet (volledig)
        # opgehaald. Zonder deze regel viel hij door naar _in(), dat False teruggeeft
        # voor een onbekende operator, en dan werd élke hit van die familie 'failed' —
        # een storing bij bing.com als bewijs van spoofing (fase 3).
        return "unchecked"
    key = (ip, op)
    v = _memo.get(key)
    if v is None:
        try:
            obj = ipaddress.ip_address(ip)
        except ValueError:
            # Kapot c-ip-veld is geen spoof maar rommel; het als 'failed' tellen
            # zou de tripwire vervuilen met iets dat niemand kan onderzoeken.
            v = "unchecked"
        else:
            v = "verified" if _in(op, obj) else "failed"
        _memo[key] = v
    return v
