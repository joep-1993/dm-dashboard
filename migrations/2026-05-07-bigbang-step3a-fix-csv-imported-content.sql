-- Fix: backfill of pa.unique_titles_content was too narrow.
--
-- Step 2's backfill copied content into pa.unique_titles_content only for rows
-- where pa.unique_titles.ai_processed = TRUE. But ~400k OLD rows had
-- ai_processed = FALSE AND title/h1 populated (CSV imports / manual upserts
-- via bulk_upsert_titles, which sets the content but doesn't flip
-- ai_processed). The OLD eligibility query accepted them as "done" because
-- title/h1 were populated; the NEW query thought they were pending because
-- the content row didn't exist.
--
-- Net effect on dashboard: Unique Titles showed 399,972 pending instead of
-- the legacy ~3. After this fix: pending=3, success=961,001.
--
-- Run once on the migrated DB.

BEGIN;

INSERT INTO pa.unique_titles_content
    (url_id, h1_title, title, description, original_h1, created_at, updated_at)
SELECT u.url_id, ut.h1_title, ut.title, ut.description, ut.original_h1,
       COALESCE(ut.created_at, ut.ai_processed_at, now()),
       COALESCE(ut.ai_processed_at, ut.created_at, now())
FROM pa.unique_titles ut
JOIN pa.urls u ON u.url = pa.canonicalize_url(ut.url)
WHERE ut.ai_processed = FALSE
  AND (ut.h1_title IS NOT NULL OR ut.title IS NOT NULL OR ut.description IS NOT NULL)
ON CONFLICT (url_id) DO NOTHING;

-- Flip those URLs' job status from 'pending' (set during step 2 backfill,
-- because ai_processed=FALSE AND ai_error IS NULL was the catch-all) to
-- 'success' since they have valid content.
UPDATE pa.unique_titles_jobs j
   SET status = 'success', updated_at = CURRENT_TIMESTAMP
  FROM pa.unique_titles_content c
 WHERE j.url_id = c.url_id
   AND j.status = 'pending'
   AND c.title IS NOT NULL AND c.title <> ''
   AND c.h1_title IS NOT NULL AND c.h1_title <> '';

COMMIT;
