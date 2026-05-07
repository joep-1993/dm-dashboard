-- ============================================================================
-- Big Bang refactor — Step 2: backfill new tables from existing data.
-- Run inside a single transaction; abort on first error.
--
-- Source → destination:
--   pa.urls               ← UNION DISTINCT of canonicalized URLs from all sources
--   pa.kopteksten_jobs    ← pa.jvs_seo_werkvoorraad_kopteksten_check
--   pa.faq_jobs           ← pa.faq_tracking
--   pa.unique_titles_jobs ← pa.unique_titles (status inferred from ai_processed)
--   pa.kopteksten_content ← pa.content_urls_joep
--   pa.faq_content_v2     ← pa.faq_content
--   pa.unique_titles_content ← pa.unique_titles (only ai_processed=TRUE rows)
--   pa.url_validation     ← pa.url_validation_tracking
-- ============================================================================

BEGIN;

-- Canonicalization function (mirrors backend/content_publisher.py::_normalize_url
-- plus a host-strip + leading-./ fix for known dirty data).
CREATE OR REPLACE FUNCTION pa.canonicalize_url(u text) RETURNS text AS $$
DECLARE
    cleaned text;
BEGIN
    IF u IS NULL OR u = '' OR u = 'undefined' THEN
        RETURN NULL;
    END IF;
    cleaned := u;
    -- Strip protocol+host if present (full beslist.nl URLs in source data)
    IF cleaned ~* '^https?://[^/]+' THEN
        cleaned := regexp_replace(cleaned, '^https?://[^/]+', '');
    END IF;
    -- Fix leading ./
    IF left(cleaned, 2) = './' THEN
        cleaned := substring(cleaned from 2);
    END IF;
    -- Reject anything that doesn't start with /
    IF left(cleaned, 1) <> '/' THEN
        RETURN NULL;
    END IF;
    -- Strip query string and fragment
    cleaned := split_part(split_part(cleaned, '?', 1), '#', 1);
    -- Trailing-slash rule
    IF position('/c/' in cleaned) > 0 THEN
        cleaned := rtrim(cleaned, '/');
    ELSE
        IF right(cleaned, 1) <> '/' THEN
            cleaned := cleaned || '/';
        END IF;
    END IF;
    -- Defence against empty result
    IF cleaned = '' OR cleaned = '/' THEN
        -- Allow root '/' only if it was the input
        IF u = '/' THEN
            RETURN '/';
        ELSE
            RETURN NULL;
        END IF;
    END IF;
    RETURN cleaned;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ----------------------------------------------------------------------------
-- pa.urls — populate from union of all source URL columns
-- ----------------------------------------------------------------------------
INSERT INTO pa.urls (url)
SELECT DISTINCT pa.canonicalize_url(url) AS url
FROM (
    SELECT url FROM pa.jvs_seo_werkvoorraad
    UNION ALL SELECT url FROM pa.jvs_seo_werkvoorraad_kopteksten_check
    UNION ALL SELECT url FROM pa.faq_tracking
    UNION ALL SELECT url FROM pa.faq_content
    UNION ALL SELECT url FROM pa.faq_validation_results
    UNION ALL SELECT url FROM pa.unique_titles
    UNION ALL SELECT url FROM pa.url_validation_tracking
    UNION ALL SELECT content_url AS url FROM pa.link_validation_results
    UNION ALL SELECT url FROM pa.content_urls_joep
) src
WHERE pa.canonicalize_url(url) IS NOT NULL
ON CONFLICT (url) DO NOTHING;

-- ----------------------------------------------------------------------------
-- pa.kopteksten_jobs ← pa.jvs_seo_werkvoorraad_kopteksten_check
-- ----------------------------------------------------------------------------
INSERT INTO pa.kopteksten_jobs (url_id, status, last_error, created_at, updated_at)
SELECT u.url_id, k.status, k.skip_reason, k.created_at, k.created_at
FROM pa.jvs_seo_werkvoorraad_kopteksten_check k
JOIN pa.urls u ON u.url = pa.canonicalize_url(k.url)
ON CONFLICT (url_id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- pa.faq_jobs ← pa.faq_tracking
-- ----------------------------------------------------------------------------
INSERT INTO pa.faq_jobs (url_id, status, skip_reason, created_at, updated_at)
SELECT u.url_id, t.status, t.skip_reason, t.created_at, t.created_at
FROM pa.faq_tracking t
JOIN pa.urls u ON u.url = pa.canonicalize_url(t.url)
ON CONFLICT (url_id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- pa.unique_titles_jobs ← pa.unique_titles (status inferred)
-- ----------------------------------------------------------------------------
INSERT INTO pa.unique_titles_jobs (url_id, status, last_error, created_at, updated_at)
SELECT u.url_id,
       CASE
           WHEN ut.ai_processed = TRUE  AND ut.ai_error IS NULL THEN 'success'
           WHEN ut.ai_processed = TRUE  AND ut.ai_error IS NOT NULL THEN 'failed'
           WHEN ut.ai_processed = FALSE AND ut.ai_error IS NOT NULL THEN 'failed'
           ELSE 'pending'
       END AS status,
       ut.ai_error,
       COALESCE(ut.created_at, ut.ai_processed_at, now()) AS created_at,
       COALESCE(ut.ai_processed_at, ut.created_at, now()) AS updated_at
FROM pa.unique_titles ut
JOIN pa.urls u ON u.url = pa.canonicalize_url(ut.url)
ON CONFLICT (url_id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- pa.kopteksten_content ← pa.content_urls_joep
-- (page_title not available in source — leaves NULL)
-- ----------------------------------------------------------------------------
INSERT INTO pa.kopteksten_content (url_id, content, created_at, updated_at)
SELECT u.url_id, c.content, c.created_at, c.created_at
FROM pa.content_urls_joep c
JOIN pa.urls u ON u.url = pa.canonicalize_url(c.url)
ON CONFLICT (url_id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- pa.faq_content_v2 ← pa.faq_content
-- (faq_json / schema_org kept as TEXT to match existing storage; some rows
--  contain literal newlines in strings that don't parse as strict JSONB)
-- ----------------------------------------------------------------------------
INSERT INTO pa.faq_content_v2 (url_id, page_title, faq_json, schema_org, created_at, updated_at)
SELECT u.url_id, f.page_title, f.faq_json, f.schema_org, f.created_at, f.created_at
FROM pa.faq_content f
JOIN pa.urls u ON u.url = pa.canonicalize_url(f.url)
ON CONFLICT (url_id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- pa.unique_titles_content ← pa.unique_titles (only successful rows)
-- ----------------------------------------------------------------------------
INSERT INTO pa.unique_titles_content (url_id, h1_title, title, description, original_h1, created_at, updated_at)
SELECT u.url_id, ut.h1_title, ut.title, ut.description, ut.original_h1,
       COALESCE(ut.created_at, ut.ai_processed_at, now()),
       COALESCE(ut.ai_processed_at, ut.created_at, now())
FROM pa.unique_titles ut
JOIN pa.urls u ON u.url = pa.canonicalize_url(ut.url)
WHERE ut.ai_processed = TRUE
  AND (ut.h1_title IS NOT NULL OR ut.title IS NOT NULL OR ut.description IS NOT NULL)
ON CONFLICT (url_id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- pa.url_validation ← pa.url_validation_tracking
-- (status='skipped' means the URL was deemed invalid → is_valid=FALSE)
-- ----------------------------------------------------------------------------
INSERT INTO pa.url_validation (url_id, last_checked_at, is_valid, reason)
SELECT u.url_id, v.checked_at,
       CASE WHEN v.status = 'skipped' THEN FALSE ELSE TRUE END AS is_valid,
       v.skip_reason
FROM pa.url_validation_tracking v
JOIN pa.urls u ON u.url = pa.canonicalize_url(v.url)
ON CONFLICT (url_id) DO NOTHING;

COMMIT;

-- ============================================================================
-- Rollback (run BEFORE step 3, to wipe backfilled data and start over):
-- ============================================================================
-- BEGIN;
-- TRUNCATE pa.url_validation, pa.unique_titles_content, pa.faq_content_v2,
--          pa.kopteksten_content, pa.unique_titles_jobs, pa.faq_jobs,
--          pa.kopteksten_jobs, pa.urls RESTART IDENTITY CASCADE;
-- COMMIT;
