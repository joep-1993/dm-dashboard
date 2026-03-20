-- Content Top Schema - SEO Content Generation

-- Create schema
CREATE SCHEMA IF NOT EXISTS pa;

-- Work queue table: stores URLs to be processed
CREATE TABLE IF NOT EXISTS pa.jvs_seo_werkvoorraad (
    url VARCHAR(500) PRIMARY KEY,
    kopteksten INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tracking table: tracks which URLs have been attempted
CREATE TABLE IF NOT EXISTS pa.jvs_seo_werkvoorraad_kopteksten_check (
    url VARCHAR(500) PRIMARY KEY,
    status VARCHAR(50) DEFAULT 'pending',  -- 'success', 'skipped', 'failed'
    skip_reason VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Output table: stores generated content
CREATE TABLE IF NOT EXISTS pa.content_urls_joep (
    id SERIAL PRIMARY KEY,
    url VARCHAR(500) NOT NULL,
    content TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_werkvoorraad_kopteksten ON pa.jvs_seo_werkvoorraad(kopteksten);
CREATE INDEX IF NOT EXISTS idx_content_urls_created ON pa.content_urls_joep(created_at DESC);

-- Shared URL validation tracking (skipped URLs shared across features)
CREATE TABLE IF NOT EXISTS pa.url_validation_tracking (
    url VARCHAR(500) PRIMARY KEY,
    status VARCHAR(50) DEFAULT 'skipped',  -- 'skipped'
    skip_reason VARCHAR(255),
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_url_validation_status ON pa.url_validation_tracking(status);

-- FAQ tracking table (tracks which URLs have FAQ generation attempted)
CREATE TABLE IF NOT EXISTS pa.faq_tracking (
    url VARCHAR(500) PRIMARY KEY,
    status VARCHAR(50) DEFAULT 'pending',  -- 'success', 'skipped', 'failed'
    skip_reason VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- FAQ output table (stores generated FAQs)
CREATE TABLE IF NOT EXISTS pa.faq_content (
    id SERIAL PRIMARY KEY,
    url VARCHAR(500) NOT NULL,
    page_title VARCHAR(500),
    faq_json TEXT,           -- JSON array of {question, answer}
    schema_org TEXT,         -- Schema.org JSON-LD
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_faq_content_created ON pa.faq_content(created_at DESC);
