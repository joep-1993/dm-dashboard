#!/usr/bin/env python3
"""Meet het effect van Bing Crawl Control (ingesteld 14-09-2026) op pa.bothits_*.

Basislijn = 5-7 sep (voor de robots.txt-wijziging), piek = 11-13 sep.
Draai zonder argumenten; pakt alles t/m de laatst geladen logdatum.
"""
import psycopg2

DSN = dict(host="10.1.32.9", port=5432, database="n8n-vector-db", user="dbadmin",
           password="Q9fGRKtUdvdtxsiCM12HeFe0Nki0PvmjZRFLZ9ArmlWdMnDQXX8SdxKnPniqGmq6",
           connect_timeout=15)

QUERIES = [
    ("Ingest-status (is de dag compleet?)", """
        SELECT log_date, hours_present, is_complete, failed_files, ingested_at
        FROM pa.bothits_ingest ORDER BY log_date DESC LIMIT 4
    """),
    ("Bing per dag — bingbot vs adidxbot", """
        SELECT d.log_date,
               sum(CASE WHEN b.bot_name ILIKE 'bingbot' THEN d.hits ELSE 0 END) AS bingbot,
               sum(CASE WHEN b.bot_name = 'adidxbot' THEN d.hits ELSE 0 END) AS adidxbot,
               sum(d.hits) AS bing_totaal,
               round(sum(d.bytes)/1024.0/1024/1024, 1) AS gb,
               round(sum(d.sum_time_ms)/1000.0/60/60, 1) AS uur_serve
        FROM pa.bothits_daily d JOIN pa.bothits_bot b ON b.bot_id = d.bot_id
        WHERE b.bot_family = 'Bing' AND d.log_date >= '2026-09-05'
        GROUP BY 1 ORDER BY 1
    """),
    ("Schade-KPI's — 5xx en responstijd", """
        SELECT d.log_date,
               sum(CASE WHEN d.status_class='5xx' THEN d.hits ELSE 0 END) AS alle_bots_5xx,
               sum(CASE WHEN d.status_class='5xx' AND b.bot_family='Bing' THEN d.hits ELSE 0 END) AS bing_5xx,
               round(sum(CASE WHEN b.bot_family='Googlebot' THEN d.sum_time_ms END)::numeric
                     / NULLIF(sum(CASE WHEN b.bot_family='Googlebot' THEN d.hits END),0)) AS ms_googlebot,
               round(sum(CASE WHEN b.bot_family='Bing' THEN d.sum_time_ms END)::numeric
                     / NULLIF(sum(CASE WHEN b.bot_family='Bing' THEN d.hits END),0)) AS ms_bing,
               round(100.0*sum(CASE WHEN b.bot_family='Bing' THEN d.hits ELSE 0 END)/sum(d.hits),1) AS pct_bing
        FROM pa.bothits_daily d JOIN pa.bothits_bot b ON b.bot_id = d.bot_id
        WHERE d.log_date >= '2026-09-05'
        GROUP BY 1 ORDER BY 1
    """),
    ("Waar Bing heen gaat — url_type per dag", """
        SELECT d.url_type,
               sum(CASE WHEN d.log_date BETWEEN '2026-09-05' AND '2026-09-07' THEN d.hits END)/3 AS basis_dag,
               sum(CASE WHEN d.log_date BETWEEN '2026-09-11' AND '2026-09-13' THEN d.hits END)/3 AS piek_dag,
               sum(CASE WHEN d.log_date >= '2026-09-14' THEN d.hits END) AS na_crawlcontrol
        FROM pa.bothits_daily d JOIN pa.bothits_bot b ON b.bot_id = d.bot_id
        WHERE b.bot_family = 'Bing' AND d.log_date >= '2026-09-05'
        GROUP BY 1 ORDER BY 3 DESC NULLS LAST
    """),
    ("Echt Bing? (IP-verificatie)", """
        SELECT d.log_date, d.verify_state, sum(d.hits) AS hits
        FROM pa.bothits_daily d JOIN pa.bothits_bot b ON b.bot_id = d.bot_id
        WHERE b.bot_family = 'Bing' AND d.log_date >= '2026-09-13'
        GROUP BY 1,2 ORDER BY 1, 3 DESC
    """),
]


def main():
    conn = psycopg2.connect(**DSN)
    cur = conn.cursor()
    for titel, sql in QUERIES:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        print(f"\n=== {titel}")
        print(" | ".join(cols))
        for r in rows:
            print(" | ".join("" if v is None else str(v) for v in r))
    conn.close()


if __name__ == "__main__":
    main()
