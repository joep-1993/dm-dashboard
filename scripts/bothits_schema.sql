-- ============================================================================
-- Bot Hits — CloudFront crawler-log analytics
-- ============================================================================
-- Grain decisions are driven by measurement, not taste (see cc1/BOTHITS_PROCESS.md):
--
--   * A full URL-grain fact table is 154M rows over 116 days. Aggregating to
--     week or month buys almost nothing (1.05x) because bots crawl a DIFFERENT
--     set of facet URLs every single day — the URL space, not the date grain,
--     is what is unbounded.
--   * 86% of those rows are URLs that do not exist in pa.urls: facet
--     combinations the crawlers assemble themselves. Keeping URL detail only
--     for known pa.urls cuts the fact table to ~21M rows.
--   * That discarded 86% is a finding, not junk — it is crawl budget burned on
--     non-indexable pages. So it stays fully visible in the cube (via
--     is_known_url + facet_depth) and its worst offenders are named in
--     bothits_unknown_daily. Only the unbounded long tail is dropped.
-- ============================================================================

CREATE TABLE IF NOT EXISTS pa.bothits_host (
    host_id     smallserial PRIMARY KEY,
    host        text NOT NULL UNIQUE
);

-- bot_class groups families for the dashboard: ai / search / seo-tool /
-- social / monitoring / other.
-- is_tracked is the URL-level whitelist. Untracked agents are still counted in
-- full in the cube — they just don't get per-URL rows, so a catch-all like
-- "other-bot" can never quietly inflate the fact table. Flip the flag and
-- re-ingest a date to change your mind.
CREATE TABLE IF NOT EXISTS pa.bothits_bot (
    bot_id      smallserial PRIMARY KEY,
    bot_family  text NOT NULL,
    bot_name    text NOT NULL,
    bot_class   text NOT NULL DEFAULT 'other',
    is_tracked  boolean NOT NULL DEFAULT true,
    UNIQUE (bot_family, bot_name)
);

-- ---------------------------------------------------------------------------
-- The cube: every bot hit is counted here, including product pages and the
-- unknown-URL tail. Low cardinality (~15k rows/day) so the dashboard's
-- timeseries, bot split, url-type split and cache stats all come from here.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pa.bothits_daily (
    log_date      date     NOT NULL,
    host_id       smallint NOT NULL REFERENCES pa.bothits_host(host_id),
    bot_id        smallint NOT NULL REFERENCES pa.bothits_bot(bot_id),
    url_type      text     NOT NULL,
    facet_depth   smallint NOT NULL,   -- 0 = no /c/ facets, else nr of facets
    is_known_url  boolean  NOT NULL,   -- present in pa.urls
    status_class  text     NOT NULL,   -- 2xx / 3xx / 4xx / 5xx
    edge_result   text     NOT NULL,   -- CloudFront Hit / Miss / RefreshHit ...
    -- Komt het IP echt van de operator die de user-agent claimt? Getoetst aan de
    -- officieel gepubliceerde IP-ranges (backend/bothits_verify.py), zonder rDNS.
    -- verified / failed / unverifiable (operator publiceert geen lijst) /
    -- unchecked (lijsten niet op te halen, of rij geladen vóór 2026-08-11).
    -- Bewust een DIMENSIE en geen filter: de spoof-graad is 0,4% van de hits, dus
    -- 'failed' is nuttiger als tripwire dan als stille correctie.
    verify_state  text     NOT NULL DEFAULT 'unchecked',
    hits          bigint   NOT NULL,
    bytes         bigint   NOT NULL DEFAULT 0,
    sum_time_ms   bigint   NOT NULL DEFAULT 0,
    PRIMARY KEY (log_date, host_id, bot_id, url_type, facet_depth,
                 is_known_url, status_class, edge_result, verify_state)
);
CREATE INDEX IF NOT EXISTS ix_bothits_daily_date ON pa.bothits_daily (log_date);
CREATE INDEX IF NOT EXISTS ix_bothits_daily_bot  ON pa.bothits_daily (bot_id, log_date);

-- ---------------------------------------------------------------------------
-- URL grain, restricted to URLs that exist in pa.urls. url_id is an FK so the
-- row stays narrow and joins straight onto main_cat_name / deepest_subcat_name.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pa.bothits_url_daily (
    log_date  date     NOT NULL,
    url_id    bigint   NOT NULL,
    host_id   smallint NOT NULL REFERENCES pa.bothits_host(host_id),
    bot_id    smallint NOT NULL REFERENCES pa.bothits_bot(bot_id),
    hits      integer  NOT NULL,
    bytes     bigint   NOT NULL DEFAULT 0,
    n_2xx     integer  NOT NULL DEFAULT 0,
    n_3xx     integer  NOT NULL DEFAULT 0,
    n_4xx     integer  NOT NULL DEFAULT 0,
    n_5xx     integer  NOT NULL DEFAULT 0,
    PRIMARY KEY (log_date, url_id, host_id, bot_id)
);
CREATE INDEX IF NOT EXISTS ix_bothits_url_daily_url  ON pa.bothits_url_daily (url_id);
CREATE INDEX IF NOT EXISTS ix_bothits_url_daily_date ON pa.bothits_url_daily (log_date);

-- ---------------------------------------------------------------------------
-- Named worst offenders among URLs NOT in pa.urls: top 500 per day per bot
-- family. Bounded (~870k rows for the whole backfill) but enough to answer
-- "which facet combinations is OpenAI wasting its crawl budget on".
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pa.bothits_unknown_daily (
    log_date     date     NOT NULL,
    host_id      smallint NOT NULL REFERENCES pa.bothits_host(host_id),
    bot_id       smallint NOT NULL REFERENCES pa.bothits_bot(bot_id),
    url          text     NOT NULL,
    url_type     text     NOT NULL,
    facet_depth  smallint NOT NULL,
    hits         integer  NOT NULL,
    PRIMARY KEY (log_date, host_id, bot_id, url)
);
CREATE INDEX IF NOT EXISTS ix_bothits_unknown_date ON pa.bothits_unknown_daily (log_date, hits DESC);

-- ---------------------------------------------------------------------------
-- Idempotency ledger. Ingest is keyed on log_date: re-running a date deletes
-- and rewrites it, so dropping the same CloudFront folder in twice is safe.
-- ---------------------------------------------------------------------------
-- hours_present / is_complete: hoeveel van de 24 uurbuckets deze logdatum draagt.
-- Een halve dag naast hele dagen leest als een verkeersinstorting die nooit gebeurde,
-- dus dat wordt vastgelegd en niet gladgestreken; het dashboard waarschuwt erover.
-- Deze twee stonden tot 2026-08-13 alleen in de live database (met de hand ge-ALTERd)
-- en niet in dit bestand — waardoor een herbouw uit dit schema een ledger opleverde
-- waar de ingest niet in kon schrijven. SCHEMA_MIGRATE in backend/bothits_ingest.py
-- laat een bestaande installatie hierheen convergeren.
CREATE TABLE IF NOT EXISTS pa.bothits_ingest (
    log_date      date PRIMARY KEY,
    files         integer,
    raw_lines     bigint,
    bot_lines     bigint,
    known_rows    integer,
    source_dirs   text,
    duration_s    integer,
    hours_present smallint,
    is_complete   boolean NOT NULL DEFAULT true,
    ingested_at   timestamp NOT NULL DEFAULT now()
);
