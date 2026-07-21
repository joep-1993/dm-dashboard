import os
from dotenv import load_dotenv
load_dotenv('/home/joepvanschagen/projects/dm-dashboard/.env')
import psycopg2
rs = psycopg2.connect(host=os.getenv('REDSHIFT_HOST'), port=os.getenv('REDSHIFT_PORT'),
                      dbname=os.getenv('REDSHIFT_DB'), user=os.getenv('REDSHIFT_USER'),
                      password=os.getenv('REDSHIFT_PASSWORD'), connect_timeout=20)
c = rs.cursor()
# recent 2 weeks: 2026-05-21..06-03 ; prior 2 weeks: 2026-05-07..05-20
SQL = """
with v as (
  select dv.main_cat_name, dv.deepest_subcat_name,
         sum(case when fcv.dim_date_key between 20260521 and 20260603 then 1 else 0 end) as recent,
         sum(case when fcv.dim_date_key between 20260507 and 20260520 then 1 else 0 end) as prior
  from datamart.fct_visits fcv
  join datamart.dim_visit dv on fcv.dim_visit_key = dv.dim_visit_key
  join chan_deriv.ref_channel_derivation_stats chan on dv.aff_id=chan.aff_id and dv.channel_id=chan.channel_id
  where dv.is_real_visit = 1 and chan.marketing_channel='SEO'
    and fcv.dim_date_key between 20260507 and 20260603
    and dv.deepest_subcat_name is not null
  group by 1,2
)
select main_cat_name, deepest_subcat_name, recent, prior,
       round(recent::numeric / nullif(prior,0), 2) as ratio
from v
where recent >= 100
order by ratio desc nulls last
limit 45
"""
c.execute(SQL)
print(f"{'main':<22}{'subcat':<34}{'recent':>8}{'prior':>8}{'ratio':>8}")
print("-"*82)
for r in c.fetchall():
    ratio = r[4] if r[4] is not None else float('inf')
    print(f"{(r[0] or '')[:21]:<22}{(r[1] or '')[:33]:<34}{r[2]:>8,}{r[3]:>8,}{str(r[4]):>8}")
rs.close()
