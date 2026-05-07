-- Performance indexes added after the FAQ + Kopteksten cutover.
--
-- Symptom: dashboards loading in 2+ minutes after cutover.
-- Root cause: the ORDER BY ... DESC LIMIT N queries (recent results panels,
-- validation history) were doing parallel hash joins against pa.urls + a
-- top-N heapsort on the content table. Postgres preferred the hash join
-- over an index scan because of the join-then-sort plan.
--
-- Two fixes:
--   1. Add explicit btree indexes on created_at / updated_at — used by
--      future workloads even if not by the dashboard queries below.
--   2. (Code change in same commit) rewrite the dashboard queries as
--      subquery-LIMIT-then-JOIN — sorts the smaller table first then
--      joins via PK lookup. ~25× speedup (1.6s → 0.1s).
--
-- Also: ANALYZE the new tables once after the cutover so the planner has
-- accurate row-count estimates.

CREATE INDEX IF NOT EXISTS idx_kopteksten_content_created    ON pa.kopteksten_content(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_kopteksten_content_updated    ON pa.kopteksten_content(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_faq_content_v2_created        ON pa.faq_content_v2(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_faq_content_v2_updated        ON pa.faq_content_v2(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_unique_titles_content_updated ON pa.unique_titles_content(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_unique_titles_jobs_updated    ON pa.unique_titles_jobs(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_kopteksten_jobs_updated       ON pa.kopteksten_jobs(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_faq_jobs_updated              ON pa.faq_jobs(updated_at DESC);

ANALYZE pa.urls;
ANALYZE pa.kopteksten_content;
ANALYZE pa.kopteksten_jobs;
ANALYZE pa.faq_content_v2;
ANALYZE pa.faq_jobs;
ANALYZE pa.unique_titles_content;
ANALYZE pa.unique_titles_jobs;
ANALYZE pa.url_validation;
