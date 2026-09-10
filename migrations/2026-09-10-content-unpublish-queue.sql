-- 2026-09-10 — content unpublish queue: make a content DELETE reach the live store.
--
-- WHY THIS EXISTS
-- Deleting a koptekst/FAQ row in Postgres is only half a removal: the record stays
-- live at website-configuration until something pushes the removal. The only thing
-- that ever did was the prune in content_records_publisher, and it walks
-- pa.kopteksten_push_state:
--
--     FROM pa.kopteksten_push_state s ... WHERE NOT EXISTS (<publishable> ...)
--
-- So the prune can only see a URL whose STATE row survived. Two bulk cleanups proved
-- how easily that breaks:
--   * 2026-08-06 — 4,306 kopteksten + 4,431 FAQs deleted for maincat-level /c/ URLs,
--     push_state rows deleted in the same transaction (correctly: the recipe says not
--     to leave orphan satellite rows). Result: the store's only pointer went with
--     them and the prune was blind. Found 5 weeks later because a URL was missing
--     from the tool while its koptekst — dead product link and all — was still live.
--   * 2026-08-31 — 1,888 URLs deleted from pa.urls, 531 of them with a published
--     koptekst, logged at the time as "a separate decision".
-- On the FAQ side there is no prune at all, and /faq is additive, so nothing would
-- ever have removed those.
--
-- The store cannot be reconciled from the outside: GET /automated-content/records
-- requires a url parameter (401 without one), so there is no way to ask it what it
-- holds and diff that against Postgres.
--
-- WHAT THIS DOES INSTEAD
-- Records the deletion itself, at the moment it happens, in a table that is not a
-- satellite of the content row and therefore cannot be cleaned up along with it.
-- backend/content_unpublish_queue.py drains it from the daily publish.
--
-- Three trigger paths, because rows leave in three ways:
--   1. DELETE on the content table          → the ordinary case
--   2. DELETE on pa.urls                    → cascades into the content tables (both
--      have ON DELETE CASCADE); by the time the cascade fires, the pa.urls row is
--      already gone, so path 1 can no longer resolve the url TEXT. This trigger runs
--      BEFORE and captures OLD.url, and the ON CONFLICT below never lets the
--      cascade's NULL overwrite it.
--   3. TRUNCATE on a content table          → row triggers do not fire at all
--
-- Every path wraps its INSERT in an exception block: a content DELETE must never fail
-- because of this queue. A warning in the server log is the acceptable failure.

CREATE TABLE IF NOT EXISTS pa.content_unpublish_queue (
    kind        text        NOT NULL CHECK (kind IN ('koptekst', 'faq')),
    url_id      bigint      NOT NULL,
    -- Kept as text on purpose: pa.urls may be gone by drain time (path 2), and the
    -- API is keyed on the url, not on our url_id.
    url         text,
    deleted_at  timestamptz NOT NULL DEFAULT now(),
    actioned_at timestamptz,
    attempts    integer     NOT NULL DEFAULT 0,
    last_error  text,
    PRIMARY KEY (kind, url_id)
);

CREATE INDEX IF NOT EXISTS content_unpublish_queue_pending_idx
    ON pa.content_unpublish_queue (kind, deleted_at)
    WHERE actioned_at IS NULL;

COMMENT ON TABLE pa.content_unpublish_queue IS
    'Tombstones for deleted koptekst/FAQ content: what still has to be removed from the '
    'live website-configuration store. Written by triggers, drained by '
    'backend/content_unpublish_queue.py during the daily publish. Never clean this up '
    'together with a content delete — that is the exact failure it exists to prevent.';


-- Path 1: a row deleted from a content table.
CREATE OR REPLACE FUNCTION pa.tg_queue_content_unpublish() RETURNS trigger AS $$
DECLARE
    v_kind text := TG_ARGV[0];
    v_url  text;
BEGIN
    BEGIN
        SELECT u.url INTO v_url FROM pa.urls u WHERE u.url_id = OLD.url_id;
        INSERT INTO pa.content_unpublish_queue (kind, url_id, url)
        VALUES (v_kind, OLD.url_id, v_url)
        ON CONFLICT (kind, url_id) DO UPDATE
           SET deleted_at  = now(),
               actioned_at = NULL,
               attempts    = 0,
               last_error  = NULL,
               url         = COALESCE(EXCLUDED.url, content_unpublish_queue.url);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'content_unpublish_queue: could not queue % %: %',
                      v_kind, OLD.url_id, SQLERRM;
    END;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Path 2: a pa.urls row deleted, cascading into whatever content hangs off it.
CREATE OR REPLACE FUNCTION pa.tg_queue_url_unpublish() RETURNS trigger AS $$
BEGIN
    BEGIN
        INSERT INTO pa.content_unpublish_queue (kind, url_id, url)
        SELECT k.kind, OLD.url_id, OLD.url
          FROM (SELECT 'koptekst'::text AS kind
                 WHERE EXISTS (SELECT 1 FROM pa.kopteksten_content c
                                WHERE c.url_id = OLD.url_id)
                UNION ALL
                SELECT 'faq'::text
                 WHERE EXISTS (SELECT 1 FROM pa.faq_content_v2 f
                                WHERE f.url_id = OLD.url_id)) k
        ON CONFLICT (kind, url_id) DO UPDATE
           SET deleted_at  = now(),
               actioned_at = NULL,
               attempts    = 0,
               last_error  = NULL,
               url         = COALESCE(EXCLUDED.url, content_unpublish_queue.url);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'content_unpublish_queue: could not queue url_id %: %',
                      OLD.url_id, SQLERRM;
    END;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Path 3: TRUNCATE. Row triggers never fire, so snapshot the whole table BEFORE it
-- empties. Statement-level, so this is one INSERT ... SELECT, not one per row.
CREATE OR REPLACE FUNCTION pa.tg_queue_content_unpublish_truncate() RETURNS trigger AS $$
DECLARE
    v_kind text := TG_ARGV[0];
BEGIN
    BEGIN
        EXECUTE format(
            'INSERT INTO pa.content_unpublish_queue (kind, url_id, url)
             SELECT %L, c.url_id, u.url
               FROM %I.%I c LEFT JOIN pa.urls u ON u.url_id = c.url_id
             ON CONFLICT (kind, url_id) DO UPDATE
                SET deleted_at = now(), actioned_at = NULL, attempts = 0,
                    last_error = NULL,
                    url = COALESCE(EXCLUDED.url, content_unpublish_queue.url)',
            v_kind, TG_TABLE_SCHEMA, TG_TABLE_NAME);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'content_unpublish_queue: could not queue TRUNCATE of %.%: %',
                      TG_TABLE_SCHEMA, TG_TABLE_NAME, SQLERRM;
    END;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS kopteksten_content_queue_unpublish ON pa.kopteksten_content;
CREATE TRIGGER kopteksten_content_queue_unpublish
    AFTER DELETE ON pa.kopteksten_content
    FOR EACH ROW EXECUTE FUNCTION pa.tg_queue_content_unpublish('koptekst');

DROP TRIGGER IF EXISTS faq_content_v2_queue_unpublish ON pa.faq_content_v2;
CREATE TRIGGER faq_content_v2_queue_unpublish
    AFTER DELETE ON pa.faq_content_v2
    FOR EACH ROW EXECUTE FUNCTION pa.tg_queue_content_unpublish('faq');

DROP TRIGGER IF EXISTS urls_queue_unpublish ON pa.urls;
CREATE TRIGGER urls_queue_unpublish
    BEFORE DELETE ON pa.urls
    FOR EACH ROW EXECUTE FUNCTION pa.tg_queue_url_unpublish();

DROP TRIGGER IF EXISTS kopteksten_content_queue_unpublish_truncate ON pa.kopteksten_content;
CREATE TRIGGER kopteksten_content_queue_unpublish_truncate
    BEFORE TRUNCATE ON pa.kopteksten_content
    FOR EACH STATEMENT EXECUTE FUNCTION pa.tg_queue_content_unpublish_truncate('koptekst');

DROP TRIGGER IF EXISTS faq_content_v2_queue_unpublish_truncate ON pa.faq_content_v2;
CREATE TRIGGER faq_content_v2_queue_unpublish_truncate
    BEFORE TRUNCATE ON pa.faq_content_v2
    FOR EACH STATEMENT EXECUTE FUNCTION pa.tg_queue_content_unpublish_truncate('faq');
