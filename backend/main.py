from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
from datetime import datetime
from io import StringIO, BytesIO
import csv
import json
import os
import asyncio
import tempfile
import time
from functools import partial, wraps
from concurrent.futures import ThreadPoolExecutor
from backend.database import get_db_connection, get_output_connection, return_db_connection, return_output_connection
from backend.scraper_service import scrape_product_page, sanitize_content
from backend.gpt_service import generate_product_content, check_content_has_valid_links
from backend.link_validator import validate_content_links, validate_and_fix_content_links, update_content_in_redshift
import psycopg2

app = FastAPI(title="Content Top - SEO Content Generation", version="1.0.0")

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict this in production
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend static files
app.mount("/static", StaticFiles(directory="frontend"), name="static")

def retry_on_redshift_serialization_error(max_retries=3, initial_delay=0.1):
    """
    Decorator to retry database operations that fail due to Redshift serialization conflicts.
    Error 1023: Serializable isolation violation on table
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except psycopg2.Error as e:
                    error_msg = str(e)
                    # Check for Redshift serialization conflict (error code 1023)
                    if "1023" in error_msg or "Serializable isolation violation" in error_msg:
                        if attempt < max_retries - 1:
                            print(f"[RETRY] Redshift serialization conflict detected (attempt {attempt + 1}/{max_retries}), retrying in {delay}s...")
                            time.sleep(delay)
                            delay *= 2  # Exponential backoff
                            continue
                    # Re-raise if not a serialization error or max retries exceeded
                    raise
            return None
        return wrapper
    return decorator

@app.get("/")
def read_root():
    return {
        "status": "running",
        "project": "content_top",
        "description": "SEO Content Generation API",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/health")
def health_check():
    return {"status": "healthy", "service": "content_top"}

@app.post("/api/generate")
async def generate_text(prompt: str):
    """Example endpoint for AI generation"""
    from backend.gpt_service import simple_completion
    try:
        result = simple_completion(prompt)
        return {"response": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def process_single_url(url: str, conservative_mode: bool = False):
    """Process a single URL - runs in thread pool
    Returns tuple: (result_dict, redshift_operations)

    Args:
        url: URL to process
        conservative_mode: If True, use conservative scraping rate (max 2 URLs/sec)
    """
    result = {"url": url, "status": "pending"}
    redshift_ops = []  # Store Redshift operations to batch later
    conn = None
    final_status = None
    final_reason = None

    try:
        # Scrape the URL first (no DB operations yet)
        scraped_data = scrape_product_page(url, conservative_mode=conservative_mode)

        # Check for 503 error (rate limiting) - should stop batch processing
        if scraped_data and scraped_data.get('error') == '503':
            final_status = 'failed'
            final_reason = 'rate_limited_503'
            result["status"] = "failed"
            result["reason"] = "rate_limited_503"
            # DO NOT mark as processed in Redshift - keep in pending for retry
            # 503 errors should stop the batch to avoid further rate limiting
        elif not scraped_data:
            final_status = 'failed'
            final_reason = 'scraping_failed'
            result["status"] = "failed"
            result["reason"] = "scraping_failed"
            # Mark as processed without content (kopteksten = 2) - other failures won't benefit from retry
            redshift_ops.append(('update_werkvoorraad_processed', url))
        elif not scraped_data['products'] or len(scraped_data['products']) == 0:
            final_status = 'skipped'
            final_reason = 'no_products_found'
            result["status"] = "skipped"
            result["reason"] = "no_products_found"
            # Mark as processed without content (kopteksten = 2) to avoid re-fetching
            redshift_ops.append(('update_werkvoorraad_processed', url))
        else:
            # Generate AI content
            try:
                print(f"[DEBUG] Generating AI content for {url[:80]}... with {len(scraped_data['products'])} products")
                ai_content = generate_product_content(
                    scraped_data['h1_title'],
                    scraped_data['products']
                )
                print(f"[DEBUG] AI content generated, length: {len(ai_content)}")

                # Sanitize content for SQL
                sanitized = sanitize_content(ai_content)

                # Check if content has valid links
                has_valid_links = check_content_has_valid_links(ai_content)
                print(f"[DEBUG] Generated content for {url[:80]}... - Has valid links: {has_valid_links}")
                print(f"[DEBUG] Content preview: {ai_content[:200]}...")

                if not has_valid_links:
                    final_status = 'failed'
                    final_reason = 'no_valid_links'
                    result["status"] = "failed"
                    result["reason"] = "no_valid_links"
                    # Mark as processed without content (kopteksten = 2) - no valid links means no usable content
                    redshift_ops.append(('update_werkvoorraad_processed', url))
                else:
                    # Collect Redshift operations for batch execution
                    redshift_ops.append(('insert_content', url, sanitized))
                    redshift_ops.append(('update_werkvoorraad_success', url))

                    final_status = 'success'
                    result["status"] = "success"
                    result["content_preview"] = ai_content[:100] + "..."

            except Exception as e:
                final_status = 'failed'
                final_reason = f"ai_generation_error: {str(e)}"
                result["status"] = "failed"
                result["reason"] = f"ai_generation_error: {str(e)}"
                # Mark as processed without content (kopteksten = 2) - AI generation failed
                redshift_ops.append(('update_werkvoorraad_processed', url))

        # Single DB transaction at the end with final status
        conn = get_db_connection()
        cur = conn.cursor()

        if final_reason:
            # Truncate skip_reason to 255 characters to fit VARCHAR(255) column
            truncated_reason = final_reason[:255] if len(final_reason) > 255 else final_reason
            cur.execute("""
                INSERT INTO pa.jvs_seo_werkvoorraad_kopteksten_check (url, status, skip_reason)
                VALUES (%s, %s, %s)
                ON CONFLICT (url) DO UPDATE SET status = EXCLUDED.status, skip_reason = EXCLUDED.skip_reason
            """, (url, final_status, truncated_reason))
        else:
            cur.execute("""
                INSERT INTO pa.jvs_seo_werkvoorraad_kopteksten_check (url, status)
                VALUES (%s, %s)
                ON CONFLICT (url) DO UPDATE SET status = EXCLUDED.status, skip_reason = NULL
            """, (url, final_status))

        conn.commit()
        print(f"[PROCESSING] {url} - Status: {final_status}" + (f" - Reason: {final_reason}" if final_reason else ""))
        return (result, redshift_ops)

    except Exception as e:
        result["status"] = "failed"
        result["reason"] = f"error: {str(e)}"
        # Try to record error in DB
        try:
            if not conn:
                conn = get_db_connection()
                cur = conn.cursor()
            # Truncate error message to 255 characters to fit VARCHAR(255) column
            error_msg = f"error: {str(e)}"[:255]
            cur.execute("""
                INSERT INTO pa.jvs_seo_werkvoorraad_kopteksten_check (url, status, skip_reason)
                VALUES (%s, 'failed', %s)
                ON CONFLICT (url) DO UPDATE SET status = 'failed', skip_reason = EXCLUDED.skip_reason
            """, (url, error_msg))
            conn.commit()
        except:
            pass  # If DB fails, just return the result
        return (result, redshift_ops)
    finally:
        if conn:
            cur.close()
            return_db_connection(conn)  # Return connection to pool instead of closing

@app.post("/api/process-urls")
def process_urls(batch_size: int = 2, parallel_workers: int = 1, conservative_mode: bool = False):
    """
    Process batch of URLs for SEO content generation.
    Fetches specified number of URLs, scrapes content, generates AI text, and saves to database.
    Supports parallel processing with configurable workers.

    Args:
        batch_size: Number of URLs to process
        parallel_workers: Number of parallel workers (1-10), ignored if conservative_mode is True
        conservative_mode: If True, use conservative scraping rate (max 2 URLs/sec) with 1 worker. Default: False
    """
    print(f"[ENDPOINT] process_urls called - batch_size={batch_size}, workers={parallel_workers}, conservative={conservative_mode}")
    output_conn = None
    local_conn = None

    try:
        # Validate parameters
        if batch_size < 1:
            raise HTTPException(status_code=400, detail="Batch size must be at least 1")

        if parallel_workers < 1 or parallel_workers > 10:
            raise HTTPException(status_code=400, detail="Parallel workers must be between 1 and 10")

        # Conservative mode always uses 1 worker for maximum safety
        if conservative_mode:
            parallel_workers = 1

        print(f"[ENDPOINT] Getting output connection...")
        # Get unprocessed URLs from Redshift
        output_conn = get_output_connection()
        print(f"[ENDPOINT] Got output connection, creating cursor...")
        output_cur = output_conn.cursor()

        # Fetch unprocessed URLs from Redshift (kopteksten=0 means pending)
        try:
            print(f"[ENDPOINT] Querying for {batch_size} pending URLs...")
            output_cur.execute("""
                SELECT url FROM pa.jvs_seo_werkvoorraad_shopping_season
                WHERE kopteksten = 0
                LIMIT %s
            """, (batch_size,))

            rows = output_cur.fetchall()
            print(f"[ENDPOINT] Got {len(rows)} URLs from Redshift")
        finally:
            print(f"[ENDPOINT] Closing cursor and returning connection...")
            output_cur.close()
            return_output_connection(output_conn)
            output_conn = None
            print(f"[ENDPOINT] Connection returned to pool")

        if not rows:
            return {
                "status": "complete",
                "message": "No URLs to process",
                "processed": 0
            }

        urls = [row['url'] for row in rows]

        # Process URLs in parallel using ThreadPoolExecutor
        # Use partial to bind conservative_mode parameter
        process_func = partial(process_single_url, conservative_mode=conservative_mode)
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            result_tuples = list(executor.map(process_func, urls))

        # Separate results and Redshift operations
        results = []
        all_redshift_ops = []
        rate_limited = False

        for result, ops in result_tuples:
            results.append(result)
            all_redshift_ops.extend(ops)

            # Check for 503 error (rate limiting) - stop immediately
            if result['status'] == 'failed' and result.get('reason') == 'rate_limited_503':
                rate_limited = True
                print(f"[RATE LIMIT DETECTED] 503 error detected - stopping batch immediately")
                break

        # Batch execute all Redshift operations - wrapped with retry logic for serialization conflicts
        @retry_on_redshift_serialization_error(max_retries=5, initial_delay=0.2)
        def execute_batch_redshift_ops():
            """Execute all Redshift operations in a single transaction with retry on serialization conflicts"""
            output_conn = get_output_connection()
            output_cur = output_conn.cursor()
            try:
                # Separate operations by type for batch execution
                insert_content_data = []
                update_werkvoorraad_success_urls = []  # kopteksten = 1 (has content)
                update_werkvoorraad_processed_urls = []  # kopteksten = 2 (processed but no content)

                for op in all_redshift_ops:
                    if op[0] == 'insert_content':
                        _, url, content = op
                        insert_content_data.append((url, content))
                    elif op[0] == 'update_werkvoorraad_success':
                        _, url = op
                        update_werkvoorraad_success_urls.append((url,))
                    elif op[0] == 'update_werkvoorraad_processed':
                        _, url = op
                        update_werkvoorraad_processed_urls.append((url,))

                # Use individual executes instead of executemany for better Redshift compatibility
                print(f"[ENDPOINT] Executing {len(insert_content_data)} inserts, {len(update_werkvoorraad_success_urls)} success updates, {len(update_werkvoorraad_processed_urls)} processed updates")

                if insert_content_data:
                    print(f"[ENDPOINT] Inserting {len(insert_content_data)} content records...")
                    for url, content in insert_content_data:
                        output_cur.execute("""
                            INSERT INTO pa.content_urls_joep (url, content)
                            VALUES (%s, %s)
                        """, (url, content))
                    print(f"[ENDPOINT] Content inserts complete")

                # Update for successful URLs (kopteksten = 1) - BATCH UPDATE to prevent serialization conflicts
                if update_werkvoorraad_success_urls:
                    print(f"[ENDPOINT] Updating {len(update_werkvoorraad_success_urls)} successful URLs...")
                    url_list = [url for (url,) in update_werkvoorraad_success_urls]
                    placeholders = ','.join(['%s'] * len(url_list))
                    output_cur.execute(f"""
                        UPDATE pa.jvs_seo_werkvoorraad_shopping_season
                        SET kopteksten = 1
                        WHERE url IN ({placeholders})
                    """, url_list)
                    print(f"[ENDPOINT] Success updates complete")

                # Update for processed-without-content URLs (kopteksten = 2) - BATCH UPDATE to prevent serialization conflicts
                if update_werkvoorraad_processed_urls:
                    print(f"[ENDPOINT] Updating {len(update_werkvoorraad_processed_urls)} processed URLs...")
                    url_list = [url for (url,) in update_werkvoorraad_processed_urls]
                    placeholders = ','.join(['%s'] * len(url_list))
                    output_cur.execute(f"""
                        UPDATE pa.jvs_seo_werkvoorraad_shopping_season
                        SET kopteksten = 2
                        WHERE url IN ({placeholders})
                    """, url_list)
                    print(f"[ENDPOINT] Processed updates complete")

                print(f"[ENDPOINT] Committing transaction...")
                output_conn.commit()
                print(f"[ENDPOINT] Transaction committed successfully")
            except Exception as db_error:
                output_conn.rollback()
                raise db_error
            finally:
                print(f"[ENDPOINT] Cleaning up output connection...")
                output_cur.close()
                return_output_connection(output_conn)
                print(f"[ENDPOINT] Output connection cleanup complete")

        if all_redshift_ops:
            execute_batch_redshift_ops()

        processed_count = sum(1 for r in results if r['status'] == 'success')
        skipped_count = sum(1 for r in results if r['status'] == 'skipped')
        failed_count = sum(1 for r in results if r['status'] == 'failed')

        if rate_limited:
            print(f"[BATCH STOPPED - RATE LIMITED] Processed: {processed_count}/{len(urls)} | Skipped: {skipped_count} | Failed: {failed_count}")
        else:
            print(f"[BATCH COMPLETE] Processed: {processed_count}/{len(urls)} | Skipped: {skipped_count} | Failed: {failed_count}")

        return {
            "status": "rate_limited" if rate_limited else "success",
            "processed": processed_count,
            "total_attempted": len(results),
            "rate_limited": rate_limited,
            "message": "Stopped due to rate limiting - wait before retrying" if rate_limited else None,
            "results": results
        }

    except Exception as e:
        print(f"[ERROR] process_urls failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Ensure connections are always returned to pool
        if local_conn:
            return_db_connection(local_conn)
        if output_conn:
            return_output_connection(output_conn)

@app.get("/api/status")
def get_status():
    """Get processing status and counts"""
    try:
        # Get counts from Redshift
        output_conn = get_output_connection()
        output_cur = output_conn.cursor()

        # Get total URLs from Redshift
        output_cur.execute("SELECT COUNT(*) as total FROM pa.jvs_seo_werkvoorraad_shopping_season")
        total = output_cur.fetchone()['total']

        # Get processed URLs (actual content records in Redshift)
        output_cur.execute("SELECT COUNT(*) as processed FROM pa.content_urls_joep")
        processed = output_cur.fetchone()['processed']

        # Get local tracking for skipped/failed stats
        conn = get_db_connection()
        cur = conn.cursor()

        # Get skipped URLs
        cur.execute("""
            SELECT COUNT(*) as skipped
            FROM pa.jvs_seo_werkvoorraad_kopteksten_check
            WHERE status = 'skipped'
        """)
        skipped = cur.fetchone()['skipped']

        # Get failed URLs
        cur.execute("""
            SELECT COUNT(*) as failed
            FROM pa.jvs_seo_werkvoorraad_kopteksten_check
            WHERE status = 'failed'
        """)
        failed = cur.fetchone()['failed']

        # Get pending URLs directly from Redshift (URLs with kopteksten=0)
        output_cur.execute("SELECT COUNT(*) as pending FROM pa.jvs_seo_werkvoorraad_shopping_season WHERE kopteksten = 0")
        pending = output_cur.fetchone()['pending']

        # Get recent results from the output database (Redshift or PostgreSQL)
        # Note: Redshift table may not have id or created_at columns, so we just get 5 rows
        try:
            output_cur.execute("""
                SELECT url, content
                FROM pa.content_urls_joep
                LIMIT 5
            """)
            recent_rows = output_cur.fetchall()
            recent = [{'url': r['url'], 'content': r['content'], 'created_at': None} for r in recent_rows]
        except Exception as e:
            print(f"[DEBUG] Failed to get recent results: {e}")
            recent = []

        cur.close()
        return_db_connection(conn)
        output_cur.close()
        return_output_connection(output_conn)

        return {
            "total_urls": total,
            "processed": processed,
            "skipped": skipped,
            "failed": failed,
            "pending": pending,
            "recent_results": recent
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/export/csv")
async def export_csv():
    """Export all generated content as CSV"""
    try:
        conn = get_output_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT url, content
            FROM pa.content_urls_joep
            ORDER BY created_at DESC
        """)
        rows = cur.fetchall()

        cur.close()
        return_db_connection(conn)

        # Create CSV in memory with UTF-8 BOM for proper Excel compatibility
        output = BytesIO()
        output.write('\ufeff'.encode('utf-8'))  # UTF-8 BOM

        text_output = StringIO()
        writer = csv.writer(text_output, quoting=csv.QUOTE_ALL, lineterminator='\n')
        writer.writerow(['url', 'content'])

        for row in rows:
            # Replace newlines in content with spaces to prevent row breaks
            content = row['content'].replace('\n', ' ').replace('\r', ' ') if row['content'] else ''
            writer.writerow([row['url'], content])

        # Write CSV text to output with UTF-8 encoding
        output.write(text_output.getvalue().encode('utf-8'))

        # Return as downloadable file
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename=content_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/export/json")
async def export_json():
    """Export all generated content as JSON"""
    try:
        conn = get_output_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT url, content
            FROM pa.content_urls_joep
        """)
        rows = cur.fetchall()

        cur.close()
        return_db_connection(conn)

        # Convert to JSON-serializable format
        data = []
        for row in rows:
            data.append({
                'url': row['url'],
                'content': row['content'],
                'created_at': row['created_at'].isoformat() if row['created_at'] else None
            })

        # Return as downloadable file
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        return StreamingResponse(
            iter([json_str]),
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename=content_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/upload-urls")
async def upload_urls(file: UploadFile = File(...)):
    """Upload a text file with URLs (one per line) to add to the work queue"""
    try:
        # Read file content
        content = await file.read()

        # Try to decode with UTF-8 BOM first, then fall back to UTF-8
        try:
            text_content = content.decode('utf-8-sig')
        except:
            text_content = content.decode('utf-8')

        # Handle both newlines and semicolons as separators (for CSV format)
        lines = text_content.strip().replace('\r\n', '\n').replace('\r', '\n').split('\n')

        # Extract URLs from each line (first column if CSV)
        urls = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # If line contains semicolons, it's CSV format - take first column
            if ';' in line:
                url = line.split(';')[0].strip()
            else:
                url = line.strip()

            if url and not url.startswith('url'):  # Skip CSV header
                urls.append(url)

        if not urls:
            raise HTTPException(status_code=400, detail="No URLs found in file")

        # Insert URLs into Redshift work queue
        output_conn = get_output_connection()
        output_cur = output_conn.cursor()

        added_count = 0
        duplicate_count = 0
        base_url = "https://www.beslist.nl"

        # Convert relative URLs to absolute URLs
        full_urls = []
        for url in urls:
            if url.startswith('/'):
                full_url = base_url + url
            else:
                full_url = url
            full_urls.append(full_url)

        # Get existing URLs from database in batches (Redshift performs better with smaller batches)
        existing_urls = set()
        batch_size = 500  # Check in batches of 500

        for i in range(0, len(full_urls), batch_size):
            batch = full_urls[i:i + batch_size]
            placeholders = ','.join(['%s'] * len(batch))
            output_cur.execute(f"""
                SELECT url FROM pa.jvs_seo_werkvoorraad_shopping_season
                WHERE url IN ({placeholders})
            """, batch)
            existing_urls.update(row['url'] for row in output_cur.fetchall())

        # Filter out duplicates
        new_urls = [(url,) for url in full_urls if url not in existing_urls]
        duplicate_count = len(full_urls) - len(new_urls)

        # Batch insert new URLs
        if new_urls:
            # Insert in batches for better Redshift performance
            insert_batch_size = 100
            for i in range(0, len(new_urls), insert_batch_size):
                batch = new_urls[i:i + insert_batch_size]
                output_cur.executemany("""
                    INSERT INTO pa.jvs_seo_werkvoorraad_shopping_season (url, kopteksten)
                    VALUES (%s, 0)
                """, batch)
            added_count = len(new_urls)

        output_conn.commit()
        output_cur.close()
        return_output_connection(output_conn)

        return {
            "status": "success",
            "total_urls": len(urls),
            "added": added_count,
            "duplicates": duplicate_count,
            "message": f"Added {added_count} new URLs, {duplicate_count} duplicates skipped"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/result/{url:path}")
async def delete_result(url: str):
    """Delete a result and reset the URL back to pending state"""
    try:
        # Delete from Redshift output table and update werkvoorraad - with retry on serialization conflicts
        @retry_on_redshift_serialization_error(max_retries=5, initial_delay=0.2)
        def delete_from_redshift():
            output_conn = get_output_connection()
            output_cur = output_conn.cursor()
            try:
                # Delete content
                output_cur.execute("""
                    DELETE FROM pa.content_urls_joep
                    WHERE url = %s
                """, (url,))

                # Reset kopteksten flag in werkvoorraad
                output_cur.execute("""
                    UPDATE pa.jvs_seo_werkvoorraad_shopping_season
                    SET kopteksten = 0
                    WHERE url = %s
                """, (url,))

                output_conn.commit()
            except Exception as e:
                output_conn.rollback()
                raise e
            finally:
                output_cur.close()
                return_output_connection(output_conn)

        delete_from_redshift()

        # Delete from local tracking table
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check
            WHERE url = %s
        """, (url,))
        conn.commit()
        cur.close()
        return_db_connection(conn)

        return {
            "status": "success",
            "message": f"Result deleted and URL reset to pending",
            "url": url
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def validate_single_content_es(content_data: tuple) -> dict:
    """Validate and fix links in a single content item using Elasticsearch lookup.

    Args:
        content_data: Tuple of (content_url, content)

    Returns:
        Dict with validation results including:
        - content_url: The URL this content belongs to
        - has_changes: Whether URLs were replaced
        - replaced_urls: List of {old_url, new_url} replacements
        - gone_urls: URLs where product is gone (need reprocessing)
        - valid_urls: URLs that were already correct
        - corrected_content: Content with corrected URLs (if any changes)
    """
    content_url, content = content_data
    return validate_and_fix_content_links(content, content_url)

@app.post("/api/validate-links")
def validate_links(batch_size: int = 10, parallel_workers: int = 3, conservative_mode: bool = False):
    """
    Validate and fix hyperlinks in generated content using Elasticsearch lookup.

    - If a product URL differs from the canonical plpUrl, the content is auto-corrected
    - If a product is GONE from Elasticsearch, the content is deleted and URL queued for reprocessing
    - URLs with only corrected links keep their kopteksten=1 status

    Args:
        batch_size: Number of content items to validate
        parallel_workers: Number of parallel workers (1-10)
        conservative_mode: Legacy parameter, kept for compatibility (ignored)
    """
    try:
        # Validate parameters
        if batch_size < 1:
            raise HTTPException(status_code=400, detail="Batch size must be at least 1")

        if parallel_workers < 1 or parallel_workers > 10:
            raise HTTPException(status_code=400, detail="Parallel workers must be between 1 and 10")

        # Use local PostgreSQL for all operations
        conn = get_db_connection()
        cur = conn.cursor()

        # Get validated URLs efficiently using a set for O(1) lookup
        cur.execute("SELECT content_url FROM pa.link_validation_results")
        validated_urls_set = set(row['content_url'] for row in cur.fetchall())

        # Fetch content from local database
        cur.execute("""
            SELECT url, content
            FROM pa.content_urls_joep
            LIMIT %s
        """, (batch_size * 3 if validated_urls_set else batch_size,))

        all_rows = cur.fetchall()
        # Filter out already validated URLs in Python
        rows = [row for row in all_rows if row['url'] not in validated_urls_set][:batch_size]

        if not rows:
            cur.close()
            return_db_connection(conn)
            return {
                "status": "complete",
                "message": "No content to validate",
                "validated": 0,
                "urls_corrected": 0,
                "moved_to_pending": 0
            }

        # Prepare content items for parallel validation
        content_items = [(row['url'], row['content']) for row in rows]

        # Process validations in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            validation_results = list(executor.map(validate_single_content_es, content_items))

        results = []
        urls_corrected = 0
        moved_to_pending = 0
        urls_with_gone_products = []  # Only these get kopteksten reset to 0
        urls_to_update_content = []  # URLs where content needs updating (replaced URLs)

        # Process validation results
        for validation_result in validation_results:
            content_url = validation_result['content_url']
            has_replaced = len(validation_result['replaced_urls']) > 0
            has_gone = len(validation_result['gone_urls']) > 0

            # Calculate totals for tracking
            total_links = len(validation_result['valid_urls']) + len(validation_result['replaced_urls']) + len(validation_result['gone_urls'])

            # Save validation results to local tracking table
            cur.execute("""
                INSERT INTO pa.link_validation_results
                (content_url, total_links, broken_links, valid_links, broken_link_details)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                content_url,
                total_links,
                len(validation_result['gone_urls']),  # Only GONE URLs are truly broken
                len(validation_result['valid_urls']) + len(validation_result['replaced_urls']),  # Replaced URLs are now valid
                json.dumps({
                    'gone_urls': validation_result['gone_urls'],
                    'replaced_urls': validation_result['replaced_urls']
                })
            ))

            # Handle URL replacements - update content in local database
            if has_replaced and not has_gone:
                # Only replaced URLs, no gone URLs - update content, keep kopteksten=1
                urls_to_update_content.append((content_url, validation_result['corrected_content']))
                urls_corrected += 1

            # Handle gone products - need to regenerate content
            if has_gone:
                urls_with_gone_products.append(content_url)
                moved_to_pending += 1

            results.append({
                'url': content_url,
                'total_links': total_links,
                'valid_urls': len(validation_result['valid_urls']),
                'replaced_urls': validation_result['replaced_urls'],
                'gone_urls': validation_result['gone_urls'],
                'content_corrected': has_replaced and not has_gone,
                'moved_to_pending': has_gone
            })

        # Update corrected content in local database
        if urls_to_update_content:
            for content_url, corrected_content in urls_to_update_content:
                cur.execute("""
                    UPDATE pa.content_urls_joep
                    SET content = %s
                    WHERE url = %s
                """, (corrected_content, content_url))
            print(f"[VALIDATE-LINKS] Updated content for {len(urls_to_update_content)} URLs with corrected links")

        # Delete/reset operations for gone products only
        if urls_with_gone_products:
            placeholders = ','.join(['%s'] * len(urls_with_gone_products))

            # Delete from content table
            cur.execute(f"""
                DELETE FROM pa.content_urls_joep
                WHERE url IN ({placeholders})
            """, urls_with_gone_products)

            # Delete from tracking table
            cur.execute(f"""
                DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check
                WHERE url IN ({placeholders})
            """, urls_with_gone_products)

            # Reset kopteksten flags - ONLY for gone products
            cur.execute(f"""
                UPDATE pa.jvs_seo_werkvoorraad_shopping_season
                SET kopteksten = 0
                WHERE url IN ({placeholders})
            """, urls_with_gone_products)

            print(f"[VALIDATE-LINKS] Reset {len(urls_with_gone_products)} URLs with gone products to pending")

        conn.commit()
        cur.close()
        return_db_connection(conn)

        return {
            "status": "success",
            "validated": len(rows),
            "urls_corrected": urls_corrected,
            "moved_to_pending": moved_to_pending,
            "results": results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/validation-history")
async def get_validation_history(limit: int = 20):
    """Get history of link validation results"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                content_url,
                total_links,
                broken_links,
                valid_links,
                broken_link_details,
                validated_at
            FROM pa.link_validation_results
            ORDER BY validated_at DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()

        cur.close()
        return_db_connection(conn)

        return {
            "status": "success",
            "results": rows
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/validation-history/reset")
async def reset_validation_history():
    """Reset all validation history - allows re-validation of all URLs"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get count before deletion
        cur.execute("SELECT COUNT(*) as count FROM pa.link_validation_results")
        count = cur.fetchone()['count']

        # Delete all validation history
        cur.execute("DELETE FROM pa.link_validation_results")
        conn.commit()

        cur.close()
        return_db_connection(conn)

        return {
            "status": "success",
            "message": f"Reset validation history for {count} URLs",
            "cleared_count": count
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
