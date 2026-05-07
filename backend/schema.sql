-- ===========================================================================
-- Content Top schema (post Big Bang refactor — 2026-05-07)
--
-- The old per-tool URL-keyed tables (jvs_seo_werkvoorraad,
-- jvs_seo_werkvoorraad_kopteksten_check, content_urls_joep, faq_tracking,
-- faq_content, faq_validation_results, url_validation_tracking,
-- link_validation_results, unique_titles) have been collapsed into a
-- single URL catalog plus per-tool job/content tables.
--
-- This file is a *reference* — the canonical creation script is in
-- migrations/2026-05-07-bigbang-step1-create-new-tables.sql; run that
-- on a fresh database. content_history (audit-only) is created at
-- runtime by backend/database.py::init_db().
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS pa;

-- See migrations/2026-05-07-bigbang-step1-create-new-tables.sql for:
--   pa.urls                       — single URL catalog
--   pa.kopteksten_jobs            — per-URL kopteksten state
--   pa.kopteksten_content         — generated kopteksten content
--   pa.kopteksten_link_validation — link-validation results for kopteksten
--   pa.faq_jobs                   — per-URL FAQ state
--   pa.faq_content_v2             — generated FAQ content (will be renamed
--                                    to pa.faq_content once the legacy
--                                    pa.faq_content is dropped)
--   pa.faq_link_validation        — link-validation results for FAQ
--   pa.unique_titles_jobs         — per-URL Unique Titles state
--   pa.unique_titles_content      — generated unique titles
--   pa.url_validation             — shared URL validation across tools
