import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import logging

logger = logging.getLogger(__name__)

# Global connection pool (initialized on first use)
_connection_pool = None


def _init_pool():
    """Initialize the connection pool."""
    global _connection_pool
    if _connection_pool is None:
        database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/myapp")
        try:
            _connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=2,  # Minimum connections in pool
                maxconn=20,  # Maximum connections in pool
                dsn=database_url,
                cursor_factory=RealDictCursor
            )
            logger.info("Database connection pool initialized (2-20 connections)")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    return _connection_pool


def get_db_connection():
    """
    Get a database connection from the pool.

    IMPORTANT: The pool was causing issues with high concurrency.
    Temporarily reverting to direct connections until we can properly
    implement connection pooling with proper lifecycle management.

    Returns a connection that should be closed when done:
        conn = get_db_connection()
        try:
            # Use connection
        finally:
            conn.close()
    """
    # Temporarily disable pooling - it was causing exhaustion issues
    # The pool needs proper connection lifecycle management across async operations
    return psycopg2.connect(
        os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/myapp"),
        cursor_factory=RealDictCursor
    )


def return_db_connection(conn):
    """
    Return a connection to the pool (optional, close() also works).

    Args:
        conn: Connection to return to pool
    """
    # No-op now that pooling is disabled
    if conn and not conn.closed:
        conn.close()

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

    # Create indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_job_items_job_id ON thema_ads_job_items(job_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_job_items_status ON thema_ads_job_items(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_input_data_job_id ON thema_ads_input_data(job_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON thema_ads_jobs(status)")

    # System settings table for queue state
    cur.execute("""
        CREATE TABLE IF NOT EXISTS system_settings (
            id SERIAL PRIMARY KEY,
            setting_key VARCHAR(100) UNIQUE NOT NULL,
            setting_value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_system_settings_key ON system_settings(setting_key)")

    # Insert default auto_queue_enabled setting
    cur.execute("""
        INSERT INTO system_settings (setting_key, setting_value)
        VALUES ('auto_queue_enabled', 'false')
        ON CONFLICT (setting_key) DO NOTHING
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Database initialized with SEO workflow and Thema Ads tables")

def get_auto_queue_enabled():
    """Get the auto-queue enabled state from database."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT setting_value FROM system_settings
            WHERE setting_key = 'auto_queue_enabled'
        """)
        result = cur.fetchone()

        if result:
            return result['setting_value'].lower() == 'true'
        return False  # Default to disabled

    finally:
        cur.close()
        conn.close()


def set_auto_queue_enabled(enabled: bool):
    """Set the auto-queue enabled state in database."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO system_settings (setting_key, setting_value, updated_at)
            VALUES ('auto_queue_enabled', %s, CURRENT_TIMESTAMP)
            ON CONFLICT (setting_key)
            DO UPDATE SET setting_value = EXCLUDED.setting_value,
                         updated_at = CURRENT_TIMESTAMP
        """, ('true' if enabled else 'false',))

        conn.commit()
        logger.info(f"Auto-queue {'enabled' if enabled else 'disabled'}")

    finally:
        cur.close()
        conn.close()


def store_activation_plan(plan_data: dict, reset_labels: bool = False):
    """
    Store activation plan in database.

    Args:
        plan_data: Dict of customer_id -> theme_name mappings
        reset_labels: If True, removes ACTIVATION_DONE labels from customers in plan

    Returns:
        Number of customers in plan
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Clear existing plan
        cur.execute("DELETE FROM activation_plan")

        # Insert new plan
        for customer_id, theme_name in plan_data.items():
            cur.execute("""
                INSERT INTO activation_plan (customer_id, theme_name, uploaded_at, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (customer_id)
                DO UPDATE SET theme_name = EXCLUDED.theme_name,
                             updated_at = CURRENT_TIMESTAMP
            """, (customer_id, theme_name))

        conn.commit()
        logger.info(f"Stored activation plan with {len(plan_data)} customers")

        # Reset ACTIVATION_DONE labels if requested
        if reset_labels and plan_data:
            logger.info(f"Resetting ACTIVATION_DONE labels for {len(plan_data)} customers")
            try:
                from pathlib import Path
                from dotenv import load_dotenv

                # Load environment variables
                env_path = Path(__file__).parent.parent / "thema_ads_optimized" / ".env"
                if env_path.exists():
                    load_dotenv(env_path)

                    from config import load_config_from_env
                    from google_ads_client import initialize_client

                    config = load_config_from_env()
                    client = initialize_client(config.google_ads)

                    # Remove ACTIVATION_DONE labels from ad groups
                    for customer_id in plan_data.keys():
                        try:
                            from label_manager import remove_label_from_customer
                            removed = remove_label_from_customer(client, customer_id, "ACTIVATION_DONE")
                            logger.info(f"  Customer {customer_id}: Removed ACTIVATION_DONE from {removed} ad groups")
                        except Exception as e:
                            logger.warning(f"  Failed to reset labels for customer {customer_id}: {e}")
            except Exception as e:
                logger.warning(f"Failed to reset ACTIVATION_DONE labels: {e}")

        return len(plan_data)

    finally:
        cur.close()
        conn.close()


def get_activation_plan(customer_ids: list = None):
    """
    Get activation plan from database.

    Args:
        customer_ids: Optional list of customer IDs to filter by

    Returns:
        Dict of customer_id -> theme_name mappings
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        if customer_ids:
            cur.execute("""
                SELECT customer_id, theme_name
                FROM activation_plan
                WHERE customer_id = ANY(%s)
            """, (customer_ids,))
        else:
            cur.execute("SELECT customer_id, theme_name FROM activation_plan")

        plan = {row['customer_id']: row['theme_name'] for row in cur.fetchall()}
        return plan

    finally:
        cur.close()
        conn.close()


def clear_activation_missing_ads():
    """Clear all missing ads records."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("DELETE FROM activation_missing_ads")
        conn.commit()
        logger.info("Cleared activation_missing_ads table")

    finally:
        cur.close()
        conn.close()


def add_activation_missing_ad(customer_id: str, campaign_id: str, campaign_name: str,
                              ad_group_id: str, ad_group_name: str, required_theme: str):
    """Add a record for an ad group missing required theme ad."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO activation_missing_ads
                (customer_id, campaign_id, campaign_name, ad_group_id, ad_group_name, required_theme)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (customer_id, campaign_id, campaign_name, ad_group_id, ad_group_name, required_theme))

        conn.commit()

    finally:
        cur.close()
        conn.close()


def get_activation_missing_ads():
    """Get all missing ads records."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT customer_id, campaign_id, campaign_name, ad_group_id, ad_group_name, required_theme, detected_at
            FROM activation_missing_ads
            ORDER BY customer_id, ad_group_id
        """)

        return cur.fetchall()

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    init_db()
