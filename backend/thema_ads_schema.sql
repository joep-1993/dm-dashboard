-- Thema Ads Processing Job Tracking Schema

-- Jobs table: tracks each processing job
CREATE TABLE IF NOT EXISTS thema_ads_jobs (
    id SERIAL PRIMARY KEY,
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, running, paused, completed, failed
    total_ad_groups INTEGER DEFAULT 0,
    processed_ad_groups INTEGER DEFAULT 0,
    successful_ad_groups INTEGER DEFAULT 0,
    failed_ad_groups INTEGER DEFAULT 0,
    input_file VARCHAR(255),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);

-- Job items: tracks each individual ad group being processed
CREATE TABLE IF NOT EXISTS thema_ads_job_items (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES thema_ads_jobs(id) ON DELETE CASCADE,
    customer_id VARCHAR(50) NOT NULL,
    ad_group_id VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, processing, completed, failed
    new_ad_resource VARCHAR(500),
    error_message TEXT,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Input data: stores uploaded CSV/Excel data
CREATE TABLE IF NOT EXISTS thema_ads_input_data (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES thema_ads_jobs(id) ON DELETE CASCADE,
    customer_id VARCHAR(50) NOT NULL,
    ad_group_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_job_items_job_id ON thema_ads_job_items(job_id);
CREATE INDEX IF NOT EXISTS idx_job_items_status ON thema_ads_job_items(status);
CREATE INDEX IF NOT EXISTS idx_input_data_job_id ON thema_ads_input_data(job_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON thema_ads_jobs(status);
