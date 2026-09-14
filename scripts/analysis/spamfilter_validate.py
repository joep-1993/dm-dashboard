#!/usr/bin/env python3
"""Toets een spam-filterregex voor R-urls tegen de echt gecrawlde URL's in bothits.

Meet twee dingen: hoeveel van de spam wordt gevangen, en hoeveel legitieme
zoektermen worden onterecht geraakt. Pas PATROON aan en draai opnieuw.

Let op: pa.bothits_unknown_daily bewaart per dag de top-500 per botfamilie, dus
de aantallen zijn een ONDERGRENS, geen totaal.
"""
import re, sys, psycopg2, urllib.parse, unicodedata

VANAF = "2026-08-20"

ANKER = r'^/products/(?:[a-z0-9_-]+/)*r/'

# REGEL 1 - teldrempel. %C0-%C5 staat er bewust NIET in: e-accent = %C3%A9, dus
# accenten in Nederlandse/Franse zoektermen blijven buiten schot. De lookahead
# sluit %E2%80 / %E2%81 uit = Algemene Interpunctie; lange producttitels
# gebruiken de en-dash als scheidingsteken en zouden anders geraakt worden.
REGEL1 = re.compile(
    ANKER + r'(?:.*?%(?!E2%8[01])(?:[EF][0-9A-F]|D[0-9A-F]|C[EF])){5}', re.I)

# REGEL 2 - signatuur, ongeacht aantal. De haken zijn de universele wikkel
# waarin spammers hun domein of WeChat/Telegram-nummer zetten. Vangt de korte
# gokspam die regel 1 nooit haalt (melbet, gacorslot, vj999 - twee tekens).
REGEL2 = re.compile(
    ANKER + r'.*?%(?:E3%80%9[01]|E3%80%8[EF]|E2%9D%B[01]|EF%B9%8[3-6]|EF%BC%BB)', re.I)


# REGEL 3 - willekeurig achtervoegsel. De spamtool hangt achter elke zoekterm
# een punt met 3-4 willekeurige letters (.vqpc .thms .trxg .jipq). Dit is de
# breedste regel: hij vangt ook de families zonder enig niet-Latijns teken
# (naamsreputatie-spam "chen liangbin - education CEO strategy.vqpc").
# De negatieve lookahead beschermt echte zoektermen: "bol.com" moet blijven.
ECHTE_EXT = (r'(?:com|net|org|nl|be|de|fr|uk|eu|info|biz|html|htm|php|aspx|asp|'
             r'pdf|doc|docx|xls|xlsx|ppt|jpg|jpeg|png|gif|webp|psd|zip|rar|'
             r'mp3|mp4|txt|csv|xml|json|exe|dmg|apk|iso)')
REGEL3 = re.compile(r'\.(?!' + ECHTE_EXT + r'$)[a-z]{3,4}$', re.I)


def matcht(wire, term=None):
    if REGEL1.search(wire) or REGEL2.search(wire):
        return True
    return bool(term and REGEL3.search(term))


SCHRIFTEN = ('CJK', 'HIRAGANA', 'KATAKANA', 'HANGUL', 'CYRILLIC', 'GREEK',
             'ARABIC', 'HEBREW', 'THAI', 'DEVANAGARI', 'FULLWIDTH')
# Spamfamilies in Latijns schrift: herkenbaar aan de lenticulaire haken en
# een wegwerpdomein in de zoekterm (Vietnamees "kiem tien online" c.s.).
LATIJNSE_SPAM = re.compile(r'[【】『』❰❱]|\.(cyou|vip|xyz|top|icu|bet|bingo)\b', re.I)
# Het willekeurige achtervoegsel telt bij het LABELEN ook als spam-indicatie,
# anders meet je regel 3 tegen een ijkpunt dat die familie niet kent.
SUFFIX_SPAM = re.compile(r'\.(?!' + ECHTE_EXT + r'$)[a-z]{3,4}$', re.I)


def decodeer(u, passes=3):
    for _ in range(passes):
        n = urllib.parse.unquote(u, errors='replace')
        if n == u:
            break
        u = n
    return u


def is_spam(d):
    if LATIJNSE_SPAM.search(d):
        return True
    term = (d.split('/r/', 1)[1] if '/r/' in d else '').rstrip('/')
    if term and SUFFIX_SPAM.search(term):
        return True
    for ch in d:
        cp = ord(ch)
        if cp < 128 or 0x2000 <= cp <= 0x206F:   # ASCII + interpunctie overslaan
            continue
        try:
            naam = unicodedata.name(ch)
        except ValueError:
            return True
        if any(t in naam for t in SCHRIFTEN):
            return True
    return False


def main():
    conn = psycopg2.connect(
        host="10.1.32.9", port=5432, database="n8n-vector-db", user="dbadmin",
        password="Q9fGRKtUdvdtxsiCM12HeFe0Nki0PvmjZRFLZ9ArmlWdMnDQXX8SdxKnPniqGmq6",
        connect_timeout=15)
    cur = conn.cursor()
    cur.execute("""SELECT u.url, b.bot_family, sum(u.hits)
                   FROM pa.bothits_unknown_daily u
                   JOIN pa.bothits_bot b ON b.bot_id = u.bot_id
                   WHERE u.log_date >= %s AND u.url_type = 'search'
                   GROUP BY 1, 2""", (VANAF,))
    rows = cur.fetchall()
    conn.close()

    gevangen = gemist = onterecht = legitiem = 0
    for url, bot, hits in rows:
        d = decodeer(url)
        # De vorm die de edge in de request-URI ziet
        wire = urllib.parse.quote(d, safe="/~*'()!$&+,;=:@?")
        term = (d.split('/r/', 1)[1] if '/r/' in d else '').rstrip('/')
        match = matcht(wire, term)
        if is_spam(d):
            if match:
                gevangen += 1
            else:
                gemist += 1
                print(f"  GEMIST     {hits:4}h {bot:12} {d[:95]}")
        else:
            legitiem += 1
            if match:
                onterecht += 1
                print(f"  ONTERECHT  {hits:4}h {bot:12} {d[:95]}")

    spam = gevangen + gemist
    print(f"\nR-urls sinds {VANAF}: {len(rows)}  (spam {spam}, legitiem {legitiem})")
    print(f"  gevangen          : {gevangen}/{spam} ({100*gevangen/spam:.1f}%)")
    print(f"  valse positieven  : {onterecht} van {legitiem}")
    return 0 if onterecht == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
