-- Per-facet positioning rules for the H1 title generator.
--
-- Lets us pin specific facets to the START or END of the generated H1 (or
-- accept either) without code changes. Empty table = no behaviour change,
-- which is the soft revert path. Hard revert = restore
-- backend/ai_titles_service.py from the .bak.2026-05-19_pre_facet_position_rules
-- snapshot.
--
-- Lookup key is the facet's url_slug (e.g. 'thema_speelgoed') because the
-- Dutch display label ('Thema''s') collides across categories.

CREATE TABLE IF NOT EXISTS pa.facet_position_rules (
    facet_slug      text NOT NULL,
    position        text NOT NULL CHECK (position IN ('start','end','start_or_end')),
    scope_category  text,           -- optional Dutch category name (lowercased); NULL = applies in every category
    reasoning       text,
    source          text NOT NULL DEFAULT 'manual' CHECK (source IN ('manual','llm_suggested')),
    created_at      timestamp NOT NULL DEFAULT now(),
    updated_at      timestamp NOT NULL DEFAULT now()
);

-- (facet_slug, scope_category) uniqueness, treating NULL as a real value so
-- a global rule (NULL) cannot coexist with itself.
CREATE UNIQUE INDEX IF NOT EXISTS facet_position_rules_slug_scope_uq
  ON pa.facet_position_rules (facet_slug, COALESCE(scope_category, ''));

COMMENT ON TABLE  pa.facet_position_rules IS 'Per-facet H1 positioning rules consumed by backend/ai_titles_service.py. Empty table = no per-facet rules applied (= pre-2026-05-19 behaviour).';
COMMENT ON COLUMN pa.facet_position_rules.facet_slug IS 'Taxonomy facet url_slug (e.g. thema_speelgoed). Match field on URLs.';
COMMENT ON COLUMN pa.facet_position_rules.position IS 'start | end | start_or_end. Anything else is rejected by CHECK.';
COMMENT ON COLUMN pa.facet_position_rules.scope_category IS 'Optional Dutch category name (lowercased) to scope the rule to. NULL = rule applies everywhere.';
