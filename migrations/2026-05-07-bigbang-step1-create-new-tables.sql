-- ============================================================================
-- Big Bang refactor — Step 1: create new tables alongside existing ones.
-- Reversible: DROP TABLE statements at the bottom (commented out).
-- No app changes yet, no data moved yet, no FKs to existing tables.
-- ============================================================================

BEGIN;

-- Catalog: one row per canonical URL, shared by all tools.
CREATE TABLE IF NOT EXISTS pa.urls (
    url_id              BIGSERIAL PRIMARY KEY,
    url                 TEXT NOT NULL UNIQUE,
    main_cat_name       TEXT,
    deepest_subcat_name TEXT,
    first_seen_at       TIMESTAMP NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMP,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    notes               TEXT
);
CREATE INDEX IF NOT EXISTS idx_urls_main_cat        ON pa.urls (main_cat_name);
CREATE INDEX IF NOT EXISTS idx_urls_deepest_subcat  ON pa.urls (deepest_subcat_name);
CREATE INDEX IF NOT EXISTS idx_urls_active          ON pa.urls (is_active) WHERE is_active;

-- Per-tool job state (status / attempts / errors).
CREATE TABLE IF NOT EXISTS pa.kopteksten_jobs (
    url_id      BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    status      TEXT NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_kopteksten_jobs_status ON pa.kopteksten_jobs (status);

CREATE TABLE IF NOT EXISTS pa.faq_jobs (
    url_id       BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    status       TEXT NOT NULL,
    skip_reason  TEXT,
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT,
    created_at   TIMESTAMP NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_faq_jobs_status      ON pa.faq_jobs (status);
CREATE INDEX IF NOT EXISTS idx_faq_jobs_skip_reason ON pa.faq_jobs (skip_reason);

CREATE TABLE IF NOT EXISTS pa.unique_titles_jobs (
    url_id          BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    http_status     INTEGER,    -- HTTP status from URL probe (was unique_titles.status_code)
    final_url       TEXT,       -- final URL after redirects (was unique_titles.final_url)
    last_checked_at TIMESTAMP,  -- last URL probe timestamp (was unique_titles.checked_at)
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_unique_titles_jobs_status ON pa.unique_titles_jobs (status);

-- Per-tool generated content (FK to urls, no longer keyed by URL string).
CREATE TABLE IF NOT EXISTS pa.kopteksten_content (
    url_id      BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    page_title  TEXT,
    content     TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pa.faq_content_v2 (
    url_id      BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    page_title  TEXT,
    faq_json    TEXT,    -- kept TEXT (not JSONB) since some legacy rows have
    schema_org  TEXT,    -- literal newlines that don't parse as strict JSONB

    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pa.unique_titles_content (
    url_id             BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    h1_title           TEXT,
    title              TEXT,
    description        TEXT,
    original_h1        TEXT,
    title_score        INTEGER,
    title_score_issue  TEXT,
    created_at         TIMESTAMP NOT NULL DEFAULT now(),
    updated_at         TIMESTAMP NOT NULL DEFAULT now()
);

-- Shared URL validation (the URL itself is reachable / has products).
CREATE TABLE IF NOT EXISTS pa.url_validation (
    url_id           BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    last_checked_at  TIMESTAMP NOT NULL DEFAULT now(),
    http_status      INTEGER,
    is_valid         BOOLEAN,
    reason           TEXT
);
CREATE INDEX IF NOT EXISTS idx_url_validation_is_valid ON pa.url_validation (is_valid);

-- Per-tool LINK validation (links INSIDE the generated content). These were
-- pa.link_validation_results (kopteksten) and pa.faq_validation_results.
-- They are conceptually different from pa.url_validation and stay separate.
CREATE TABLE IF NOT EXISTS pa.kopteksten_link_validation (
    url_id              BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    total_links         INTEGER,
    valid_links         INTEGER,
    broken_links        INTEGER,
    broken_link_details JSONB,
    validated_at        TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_kopt_link_val_validated ON pa.kopteksten_link_validation(validated_at);

CREATE TABLE IF NOT EXISTS pa.faq_link_validation (
    url_id        BIGINT PRIMARY KEY REFERENCES pa.urls(url_id) ON DELETE CASCADE,
    total_links   INTEGER,
    valid_links   INTEGER,
    gone_links    INTEGER,
    validated_at  TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_faq_link_val_validated ON pa.faq_link_validation(validated_at);

COMMIT;

-- ============================================================================
-- Rollback (run individually if step 1 needs to be undone, before any data
-- has moved):
-- ============================================================================
-- DROP TABLE IF EXISTS pa.faq_link_validation;
-- DROP TABLE IF EXISTS pa.kopteksten_link_validation;
-- DROP TABLE IF EXISTS pa.url_validation;
-- DROP TABLE IF EXISTS pa.unique_titles_content;
-- DROP TABLE IF EXISTS pa.faq_content_v2;
-- DROP TABLE IF EXISTS pa.kopteksten_content;
-- DROP TABLE IF EXISTS pa.unique_titles_jobs;
-- DROP TABLE IF EXISTS pa.faq_jobs;
-- DROP TABLE IF EXISTS pa.kopteksten_jobs;
-- DROP TABLE IF EXISTS pa.urls;
