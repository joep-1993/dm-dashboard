import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

# Connection pools for reusing connections across requests
_pg_pool = None
_redshift_pool = None

def _get_pg_pool():
    """Get or create PostgreSQL connection pool"""
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=20,  # Increased from 10 to support more parallel workers
            dsn=os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/myapp"),
            cursor_factory=RealDictCursor
        )
    return _pg_pool

def _get_redshift_pool():
    """Get or create Redshift connection pool"""
    global _redshift_pool
    if _redshift_pool is None:
        _redshift_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,  # Increased from 5 to support more parallel workers
            host=os.getenv("REDSHIFT_HOST"),
            port=os.getenv("REDSHIFT_PORT", "5439"),
            dbname=os.getenv("REDSHIFT_DB"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD"),
            cursor_factory=RealDictCursor,
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=60,
            keepalives_interval=10,
            keepalives_count=5
        )
    return _redshift_pool

def get_db_connection():
    """Get PostgreSQL connection from pool"""
    p = _get_pg_pool()
    return p.getconn()

def return_db_connection(conn):
    """Return PostgreSQL connection to pool"""
    if conn:
        p = _get_pg_pool()
        p.putconn(conn)

def get_redshift_connection():
    """Get Redshift connection from pool, with stale connection recovery"""
    p = _get_redshift_pool()
    conn = p.getconn()
    # Test if the connection is still alive
    try:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
    except Exception:
        # Connection is dead, close it and get a fresh one
        try:
            p.putconn(conn, close=True)
        except Exception:
            pass
        conn = p.getconn()
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
    return conn

def return_redshift_connection(conn):
    """Return Redshift connection to pool"""
    if conn:
        p = _get_redshift_pool()
        p.putconn(conn)

def get_output_connection():
    """Get connection for output operations - Redshift or PostgreSQL based on config"""
    use_redshift = os.getenv("USE_REDSHIFT_OUTPUT", "false").lower() == "true"
    if use_redshift:
        return get_redshift_connection()
    return get_db_connection()

def return_output_connection(conn):
    """Return output connection to appropriate pool"""
    use_redshift = os.getenv("USE_REDSHIFT_OUTPUT", "false").lower() == "true"
    if use_redshift:
        return_redshift_connection(conn)
    else:
        return_db_connection(conn)

def init_db():
    """Initialize database tables"""
    conn = get_db_connection()
    cur = conn.cursor()

    # Create schema if not exists
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS pa;
    """)

    # Create work queue table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pa.jvs_seo_werkvoorraad (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL UNIQUE,
            kopteksten INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create tracking table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pa.jvs_seo_werkvoorraad_kopteksten_check (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create output table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pa.content_urls_joep (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Shared URL validation tracking (skipped URLs across kopteksten + FAQ)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pa.url_validation_tracking (
            url VARCHAR(500) PRIMARY KEY,
            status VARCHAR(50) DEFAULT 'skipped',
            skip_reason VARCHAR(255),
            checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create link validation tracking table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pa.link_validation_results (
            id SERIAL PRIMARY KEY,
            content_url TEXT NOT NULL,
            total_links INTEGER DEFAULT 0,
            broken_links INTEGER DEFAULT 0,
            valid_links INTEGER DEFAULT 0,
            broken_link_details JSONB,
            validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create content history/backup table (stores content before reset/deletion)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pa.content_history (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            content TEXT,
            reset_reason TEXT,
            reset_details JSONB,
            original_created_at TIMESTAMP,
            reset_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Thema Ads tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS thema_ads_jobs (
            id SERIAL PRIMARY KEY,
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            total_ad_groups INTEGER DEFAULT 0,
            processed_ad_groups INTEGER DEFAULT 0,
            successful_ad_groups INTEGER DEFAULT 0,
            failed_ad_groups INTEGER DEFAULT 0,
            skipped_ad_groups INTEGER DEFAULT 0,
            input_file VARCHAR(255),
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            error_message TEXT
        )
    """)

    # Add skipped_ad_groups column if it doesn't exist (migration)
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='thema_ads_jobs' AND column_name='skipped_ad_groups'
            ) THEN
                ALTER TABLE thema_ads_jobs ADD COLUMN skipped_ad_groups INTEGER DEFAULT 0;
            END IF;
        END $$;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS thema_ads_job_items (
            id SERIAL PRIMARY KEY,
            job_id INTEGER REFERENCES thema_ads_jobs(id) ON DELETE CASCADE,
            customer_id VARCHAR(50) NOT NULL,
            campaign_id VARCHAR(50),
            campaign_name TEXT,
            ad_group_id VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            new_ad_resource VARCHAR(500),
            error_message TEXT,
            processed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS thema_ads_input_data (
            id SERIAL PRIMARY KEY,
            job_id INTEGER REFERENCES thema_ads_jobs(id) ON DELETE CASCADE,
            customer_id VARCHAR(50) NOT NULL,
            campaign_id VARCHAR(50),
            campaign_name TEXT,
            ad_group_id VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create indexes for Thema Ads
    cur.execute("CREATE INDEX IF NOT EXISTS idx_job_items_job_id ON thema_ads_job_items(job_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_job_items_status ON thema_ads_job_items(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_input_data_job_id ON thema_ads_input_data(job_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON thema_ads_jobs(status)")

    # Create indexes for SEO content tables (performance optimization)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_content_urls_url ON pa.content_urls_joep(url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_werkvoorraad_check_url ON pa.jvs_seo_werkvoorraad_kopteksten_check(url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_werkvoorraad_check_status ON pa.jvs_seo_werkvoorraad_kopteksten_check(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_link_validation_content_url ON pa.link_validation_results(content_url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_content_history_url ON pa.content_history(url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_content_history_reset_at ON pa.content_history(reset_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_url_validation_status ON pa.url_validation_tracking(status)")

    conn.commit()
    cur.close()
    conn.close()
    print("Database initialized with SEO workflow and Thema Ads tables")

if __name__ == "__main__":
    init_db()
