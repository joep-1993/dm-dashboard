-- ============================================================================
-- Big Bang refactor — Step 4: rename old tables.
--
-- Renames the legacy URL-keyed tables to *_old_2026_05_07 so any unmigrated
-- code fails loudly with "relation does not exist" instead of silently
-- writing to stale data.
--
-- The pre-cutover snapshot stays accessible at the *_old_2026_05_07 names
-- for at least one week. Step 5 will DROP TABLE them.
-- ============================================================================

BEGIN;

ALTER TABLE pa.jvs_seo_werkvoorraad                 RENAME TO jvs_seo_werkvoorraad_old_2026_05_07;
ALTER TABLE pa.jvs_seo_werkvoorraad_kopteksten_check RENAME TO jvs_seo_werkvoorraad_kopteksten_check_old_2026_05_07;
ALTER TABLE pa.content_urls_joep                    RENAME TO content_urls_joep_old_2026_05_07;
ALTER TABLE pa.faq_tracking                         RENAME TO faq_tracking_old_2026_05_07;
ALTER TABLE pa.faq_content                          RENAME TO faq_content_old_2026_05_07;
ALTER TABLE pa.faq_validation_results               RENAME TO faq_validation_results_old_2026_05_07;
ALTER TABLE pa.url_validation_tracking              RENAME TO url_validation_tracking_old_2026_05_07;
ALTER TABLE pa.link_validation_results              RENAME TO link_validation_results_old_2026_05_07;
ALTER TABLE pa.unique_titles                        RENAME TO unique_titles_old_2026_05_07;

COMMIT;

-- Rollback (if something breaks immediately after deploy):
-- BEGIN;
-- ALTER TABLE pa.jvs_seo_werkvoorraad_old_2026_05_07                 RENAME TO jvs_seo_werkvoorraad;
-- ALTER TABLE pa.jvs_seo_werkvoorraad_kopteksten_check_old_2026_05_07 RENAME TO jvs_seo_werkvoorraad_kopteksten_check;
-- ALTER TABLE pa.content_urls_joep_old_2026_05_07                    RENAME TO content_urls_joep;
-- ALTER TABLE pa.faq_tracking_old_2026_05_07                         RENAME TO faq_tracking;
-- ALTER TABLE pa.faq_content_old_2026_05_07                          RENAME TO faq_content;
-- ALTER TABLE pa.faq_validation_results_old_2026_05_07               RENAME TO faq_validation_results;
-- ALTER TABLE pa.url_validation_tracking_old_2026_05_07              RENAME TO url_validation_tracking;
-- ALTER TABLE pa.link_validation_results_old_2026_05_07              RENAME TO link_validation_results;
-- ALTER TABLE pa.unique_titles_old_2026_05_07                        RENAME TO unique_titles;
-- COMMIT;
