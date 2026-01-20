from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, RedirectResponse
from datetime import datetime
from io import StringIO, BytesIO
import csv
import json
import os
import asyncio
import tempfile
import time
import re
from functools import partial, wraps
from concurrent.futures import ThreadPoolExecutor
from backend.database import get_db_connection, get_output_connection, return_db_connection, return_output_connection
from backend.scraper_service import scrape_product_page, scrape_product_page_api, sanitize_content
from backend.gpt_service import generate_product_content, check_content_has_valid_links
from backend.link_validator import validate_content_links, validate_and_fix_content_links
from backend.faq_service import process_single_url_faq
from backend.thema_ads_router import router as thema_ads_router, cleanup_stale_jobs as cleanup_thema_ads_jobs
from backend.content_publisher import (
    get_total_content_count,
    get_content_batch,
    publish_all_content,
    generate_curl_command,
    start_publish_task,
    get_publish_task_status
)
import psycopg2

app = FastAPI(title="SEO Tools - Unified Platform", version="1.0.0")

# Include thema_ads router
app.include_router(thema_ads_router)

@app.on_event("startup")
async def startup_event():
    """Run startup tasks for all services."""
    await cleanup_thema_ads_jobs()

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
    """Redirect to the unified dashboard"""
    return RedirectResponse(url="/static/dashboard.html")

@app.get("/api/health")
def health_check():
    return {"status": "healthy", "service": "dm_tools"}

@app.get("/api/debug/test-scraper")
def debug_test_scraper(url: str):
    """Debug endpoint to test scraper on a single URL"""
    import time
    start = time.time()
    try:
        result = scrape_product_page_api(url)
        elapsed = time.time() - start
        if result is None:
            return {"status": "failed", "reason": "api_returned_none", "elapsed_seconds": elapsed}
        if result.get('error'):
            return {"status": "failed", "reason": result.get('error'), "elapsed_seconds": elapsed}
        return {
            "status": "success",
            "elapsed_seconds": elapsed,
            "product_count": len(result.get('products', [])),
            "h1_title": result.get('h1_title'),
            "product_subject": result.get('product_subject')
        }
    except Exception as e:
        elapsed = time.time() - start
        return {"status": "error", "reason": str(e), "elapsed_seconds": elapsed}

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
    conn = None
    final_status = None
    final_reason = None

    try:
        # Use API-based scraper for better product subject extraction
        scraped_data = scrape_product_page_api(url)

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
            final_reason = 'api_failed'
            result["status"] = "failed"
            result["reason"] = "api_failed"
        elif not scraped_data['products'] or len(scraped_data['products']) == 0:
            final_status = 'skipped'
            final_reason = 'no_products_found'
            result["status"] = "skipped"
            result["reason"] = "no_products_found"
        else:
            # Generate AI content using product_subject from selected facets
            try:
                # Use product_subject if available (from API), otherwise fall back to h1_title
                content_topic = scraped_data.get('product_subject') or scraped_data['h1_title']
                print(f"[DEBUG] Generating AI content for {url[:80]}... with topic '{content_topic}' and {len(scraped_data['products'])} products")
                ai_content = generate_product_content(
                    content_topic,
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
                else:
                    # Save content to local PostgreSQL immediately
                    content_conn = None
                    try:
                        content_conn = get_db_connection()
                        content_cur = content_conn.cursor()
                        content_cur.execute("""
                            INSERT INTO pa.content_urls_joep (url, content)
                            VALUES (%s, %s)
                        """, (url, sanitized))
                        content_conn.commit()
                        content_cur.close()
                    finally:
                        if content_conn:
                            return_db_connection(content_conn)

                    final_status = 'success'
                    result["status"] = "success"
                    # Strip HTML tags from preview to avoid broken tags
                    preview_text = re.sub(r'<[^>]+>', '', ai_content)
                    result["content_preview"] = preview_text[:100] + "..."

            except Exception as e:
                final_status = 'failed'
                final_reason = f"ai_generation_error: {str(e)}"
                result["status"] = "failed"
                result["reason"] = f"ai_generation_error: {str(e)}"

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
        return result

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
        return result
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

    try:
        # Validate parameters
        if batch_size < 1:
            raise HTTPException(status_code=400, detail="Batch size must be at least 1")

        if parallel_workers < 1 or parallel_workers > 20:
            raise HTTPException(status_code=400, detail="Parallel workers must be between 1 and 20")

        # Conservative mode always uses 1 worker for maximum safety
        if conservative_mode:
            parallel_workers = 1

        print(f"[ENDPOINT] Getting local connection...")
        # Get unprocessed URLs from local PostgreSQL
        local_conn = get_db_connection()
        print(f"[ENDPOINT] Got local connection, creating cursor...")
        local_cur = local_conn.cursor()

        # Fetch unprocessed URLs from local werkvoorraad (URLs not yet in tracking table)
        try:
            print(f"[ENDPOINT] Querying for {batch_size} pending URLs...")
            local_cur.execute("""
                SELECT w.url
                FROM pa.jvs_seo_werkvoorraad w
                LEFT JOIN pa.jvs_seo_werkvoorraad_kopteksten_check t ON w.url = t.url
                WHERE t.url IS NULL
                LIMIT %s
            """, (batch_size,))

            rows = local_cur.fetchall()
            print(f"[ENDPOINT] Got {len(rows)} URLs from local PostgreSQL")
        finally:
            print(f"[ENDPOINT] Closing cursor and returning connection...")
            local_cur.close()
            return_db_connection(local_conn)
            local_conn = None
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
            results = list(executor.map(process_func, urls))

        # Check for rate limiting
        rate_limited = False
        for result in results:
            if result['status'] == 'failed' and result.get('reason') == 'rate_limited_503':
                rate_limited = True
                print(f"[RATE LIMIT DETECTED] 503 error detected - stopping batch immediately")
                break

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

@app.get("/api/status")
def get_status():
    """Get processing status and counts (LOCAL PostgreSQL only)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get total content URLs (processed with content) - use DISTINCT to avoid counting duplicates
        cur.execute("SELECT COUNT(DISTINCT url) as processed FROM pa.content_urls_joep")
        processed = cur.fetchone()['processed']

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

        # Get total unique URLs across all tables (werkvoorraad + content)
        cur.execute("""
            SELECT COUNT(DISTINCT url) as total FROM (
                SELECT url FROM pa.jvs_seo_werkvoorraad
                UNION
                SELECT url FROM pa.content_urls_joep
            ) all_urls
        """)
        total = cur.fetchone()['total']

        # Pending = URLs in werkvoorraad that haven't been tracked yet (using LEFT JOIN)
        cur.execute("""
            SELECT COUNT(*) as pending
            FROM pa.jvs_seo_werkvoorraad w
            LEFT JOIN pa.jvs_seo_werkvoorraad_kopteksten_check t ON w.url = t.url
            WHERE t.url IS NULL
        """)
        pending = cur.fetchone()['pending']

        # Get recent results from local PostgreSQL
        try:
            cur.execute("""
                SELECT url, content, created_at
                FROM pa.content_urls_joep
                ORDER BY created_at DESC NULLS LAST
                LIMIT 5
            """)
            recent_rows = cur.fetchall()
            recent = [{'url': r['url'], 'content': r['content'], 'created_at': r['created_at'].isoformat() if r.get('created_at') else None} for r in recent_rows]
        except Exception as e:
            print(f"[DEBUG] Failed to get recent results: {e}")
            recent = []

        cur.close()
        return_db_connection(conn)

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

@app.get("/api/failure-reasons")
def get_failure_reasons():
    """Get breakdown of failure reasons"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT status, skip_reason, COUNT(*) as count
            FROM pa.jvs_seo_werkvoorraad_kopteksten_check
            GROUP BY status, skip_reason
            ORDER BY count DESC
        """)
        rows = cur.fetchall()

        cur.close()
        return_db_connection(conn)

        return {
            "breakdown": [{"status": r["status"], "reason": r["skip_reason"], "count": r["count"]} for r in rows]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/export/xlsx")
async def export_xlsx():
    """Export all generated content as Excel XLSX (from local PostgreSQL)"""
    from openpyxl import Workbook

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT url, content
            FROM pa.content_urls_joep
        """)
        rows = cur.fetchall()

        cur.close()
        return_db_connection(conn)

        # Create Excel workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Content Export"

        # Add headers
        ws.append(['url', 'content'])

        # Add data rows
        import re
        # Remove control characters that Excel doesn't allow (except tab, newline, carriage return)
        illegal_chars = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')

        for row in rows:
            content = row['content'] if row['content'] else ''
            # Remove illegal control characters
            content = illegal_chars.sub('', content)
            ws.append([row['url'], content])

        # Save to BytesIO
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=content_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"}
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
        return_output_connection(conn)

        # Convert to JSON-serializable format
        data = []
        for row in rows:
            data.append({
                'url': row['url'],
                'content': row['content']
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

        # Try multiple encodings (UTF-16, UTF-8 with BOM, UTF-8, Windows-1252, Latin-1)
        text_content = None
        for encoding in ['utf-16', 'utf-16-le', 'utf-8-sig', 'utf-8', 'windows-1252', 'latin-1']:
            try:
                text_content = content.decode(encoding)
                # Verify no replacement characters
                if '�' not in text_content:
                    break
            except (UnicodeDecodeError, UnicodeError):
                continue

        if text_content is None:
            text_content = content.decode('latin-1')  # Latin-1 never fails

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

        if parallel_workers < 1 or parallel_workers > 20:
            raise HTTPException(status_code=400, detail="Parallel workers must be between 1 and 20")

        # Use local PostgreSQL for all operations
        conn = get_db_connection()
        cur = conn.cursor()

        # Fetch unvalidated content URLs using LEFT JOIN for efficiency
        cur.execute("""
            SELECT c.url, c.content
            FROM pa.content_urls_joep c
            LEFT JOIN pa.link_validation_results v ON c.url = v.content_url
            WHERE v.content_url IS NULL
            LIMIT %s
        """, (batch_size,))

        rows = cur.fetchall()

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

            # Delete from content table (local PostgreSQL)
            cur.execute(f"""
                DELETE FROM pa.content_urls_joep
                WHERE url IN ({placeholders})
            """, urls_with_gone_products)

            # Delete from tracking table (local PostgreSQL)
            cur.execute(f"""
                DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check
                WHERE url IN ({placeholders})
            """, urls_with_gone_products)

            # Add URLs to werkvoorraad for reprocessing (if not already there)
            for url in urls_with_gone_products:
                cur.execute("""
                    INSERT INTO pa.jvs_seo_werkvoorraad (url, kopteksten)
                    VALUES (%s, 0)
                    ON CONFLICT (url) DO UPDATE SET kopteksten = 0
                """, (url,))

            print(f"[VALIDATE-LINKS] Deleted content for {len(urls_with_gone_products)} URLs with gone products")

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

@app.post("/api/validate-all-links")
def validate_all_links(parallel_workers: int = 3):
    """
    Validate ALL content URLs that haven't been validated yet.

    This runs until all URLs are validated or an error occurs.
    Returns summary of all validations performed.

    Args:
        parallel_workers: Number of parallel workers (1-10)
    """
    try:
        if parallel_workers < 1 or parallel_workers > 20:
            raise HTTPException(status_code=400, detail="Parallel workers must be between 1 and 20")

        total_validated = 0
        total_urls_corrected = 0
        total_moved_to_pending = 0
        batch_size = 100  # Process in batches of 100

        while True:
            # Use local PostgreSQL for all operations
            conn = get_db_connection()
            cur = conn.cursor()

            # Fetch unvalidated content URLs using LEFT JOIN for efficiency
            cur.execute("""
                SELECT c.url, c.content
                FROM pa.content_urls_joep c
                LEFT JOIN pa.link_validation_results v ON c.url = v.content_url
                WHERE v.content_url IS NULL
                LIMIT %s
            """, (batch_size,))

            rows = cur.fetchall()

            if not rows:
                cur.close()
                return_db_connection(conn)
                break  # No more URLs to validate

            # Prepare content items for parallel validation
            content_items = [(row['url'], row['content']) for row in rows]

            # Process validations in parallel using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                validation_results = list(executor.map(validate_single_content_es, content_items))

            urls_corrected = 0
            moved_to_pending = 0
            urls_with_gone_products = []
            urls_to_update_content = []

            # Process validation results
            for validation_result in validation_results:
                content_url = validation_result['content_url']
                has_replaced = len(validation_result['replaced_urls']) > 0
                has_gone = len(validation_result['gone_urls']) > 0

                total_links = len(validation_result['valid_urls']) + len(validation_result['replaced_urls']) + len(validation_result['gone_urls'])

                # Save validation results to local tracking table
                cur.execute("""
                    INSERT INTO pa.link_validation_results
                    (content_url, total_links, broken_links, valid_links, broken_link_details)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    content_url,
                    total_links,
                    len(validation_result['gone_urls']),
                    len(validation_result['valid_urls']) + len(validation_result['replaced_urls']),
                    json.dumps({
                        'gone_urls': validation_result['gone_urls'],
                        'replaced_urls': validation_result['replaced_urls']
                    })
                ))

                if has_replaced and not has_gone:
                    urls_to_update_content.append((content_url, validation_result['corrected_content']))
                    urls_corrected += 1

                if has_gone:
                    urls_with_gone_products.append(content_url)
                    moved_to_pending += 1

            # Update corrected content in local database
            if urls_to_update_content:
                for content_url, corrected_content in urls_to_update_content:
                    cur.execute("""
                        UPDATE pa.content_urls_joep
                        SET content = %s
                        WHERE url = %s
                    """, (corrected_content, content_url))

            # Delete/reset operations for gone products only
            if urls_with_gone_products:
                placeholders = ','.join(['%s'] * len(urls_with_gone_products))
                cur.execute(f"""
                    DELETE FROM pa.content_urls_joep
                    WHERE url IN ({placeholders})
                """, urls_with_gone_products)
                cur.execute(f"""
                    DELETE FROM pa.jvs_seo_werkvoorraad_kopteksten_check
                    WHERE url IN ({placeholders})
                """, urls_with_gone_products)
                # Add URLs to werkvoorraad for reprocessing (if not already there)
                for url in urls_with_gone_products:
                    cur.execute("""
                        INSERT INTO pa.jvs_seo_werkvoorraad (url, kopteksten)
                        VALUES (%s, 0)
                        ON CONFLICT (url) DO UPDATE SET kopteksten = 0
                    """, (url,))

            conn.commit()
            cur.close()
            return_db_connection(conn)

            total_validated += len(rows)
            total_urls_corrected += urls_corrected
            total_moved_to_pending += moved_to_pending

            print(f"[VALIDATE-ALL] Batch complete: {len(rows)} validated, {urls_corrected} corrected, {moved_to_pending} moved to pending. Total so far: {total_validated}")

        return {
            "status": "success",
            "message": f"Validated all {total_validated} content URLs",
            "validated": total_validated,
            "urls_corrected": total_urls_corrected,
            "moved_to_pending": total_moved_to_pending
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


# ============================================================================
# FAQ GENERATION ENDPOINTS
# ============================================================================

def process_single_url_faq_wrapper(args: tuple) -> dict:
    """Wrapper for process_single_url_faq to work with ThreadPoolExecutor"""
    url, num_faqs = args
    return process_single_url_faq(url, num_faqs)


@app.get("/api/faq/status")
def get_faq_status():
    """Get FAQ processing status and counts"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get total FAQ content (processed with FAQs)
        cur.execute("SELECT COUNT(*) as processed FROM pa.faq_content")
        processed = cur.fetchone()['processed']

        # Get skipped URLs
        cur.execute("""
            SELECT COUNT(*) as skipped
            FROM pa.faq_tracking
            WHERE status = 'skipped'
        """)
        skipped = cur.fetchone()['skipped']

        # Get failed URLs
        cur.execute("""
            SELECT COUNT(*) as failed
            FROM pa.faq_tracking
            WHERE status = 'failed'
        """)
        failed = cur.fetchone()['failed']

        # Get total unique URLs from werkvoorraad (same as content status)
        cur.execute("""
            SELECT COUNT(*) as total FROM pa.jvs_seo_werkvoorraad
        """)
        total = cur.fetchone()['total']

        # Pending = werkvoorraad URLs that don't have FAQs yet OR have status='pending' in tracking
        cur.execute("""
            SELECT COUNT(*) as pending
            FROM pa.jvs_seo_werkvoorraad w
            LEFT JOIN pa.faq_tracking t ON w.url = t.url
            WHERE t.url IS NULL OR t.status = 'pending'
        """)
        pending = cur.fetchone()['pending']

        # Get recent FAQ results
        try:
            cur.execute("""
                SELECT url, page_title, faq_json, schema_org, created_at
                FROM pa.faq_content
                ORDER BY created_at DESC NULLS LAST
                LIMIT 5
            """)
            recent_rows = cur.fetchall()
            recent = [{
                'url': r['url'],
                'page_title': r['page_title'],
                'faq_json': r['faq_json'],
                'schema_org': r['schema_org'],
                'created_at': r['created_at'].isoformat() if r.get('created_at') else None
            } for r in recent_rows]
        except Exception as e:
            print(f"[FAQ] Failed to get recent results: {e}")
            recent = []

        cur.close()
        return_db_connection(conn)

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


@app.post("/api/faq/process-urls")
def process_faq_urls(batch_size: int = 10, parallel_workers: int = 3, num_faqs: int = 6):
    """
    Process batch of URLs for FAQ generation.

    Args:
        batch_size: Number of URLs to process
        parallel_workers: Number of parallel workers (1-10)
        num_faqs: Number of FAQ items to generate per page (default 5)
    """
    print(f"[FAQ] process_faq_urls called - batch_size={batch_size}, workers={parallel_workers}, num_faqs={num_faqs}")

    try:
        # Validate parameters
        if batch_size < 1:
            raise HTTPException(status_code=400, detail="Batch size must be at least 1")

        if parallel_workers < 1 or parallel_workers > 20:
            raise HTTPException(status_code=400, detail="Parallel workers must be between 1 and 20")

        if num_faqs < 1 or num_faqs > 10:
            raise HTTPException(status_code=400, detail="Number of FAQs must be between 1 and 10")

        # Get unprocessed URLs from local PostgreSQL
        conn = get_db_connection()
        cur = conn.cursor()

        # Fetch unprocessed URLs (URLs not yet in FAQ tracking table OR with status='pending')
        cur.execute("""
            SELECT w.url
            FROM pa.jvs_seo_werkvoorraad w
            LEFT JOIN pa.faq_tracking t ON w.url = t.url
            WHERE t.url IS NULL OR t.status = 'pending'
            LIMIT %s
        """, (batch_size,))

        rows = cur.fetchall()
        cur.close()
        return_db_connection(conn)

        if not rows:
            return {
                "status": "complete",
                "message": "No URLs to process",
                "processed": 0
            }

        urls = [row['url'] for row in rows]

        # Process URLs in parallel using ThreadPoolExecutor
        url_args = [(url, num_faqs) for url in urls]
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            results = list(executor.map(process_single_url_faq_wrapper, url_args))

        # Save results to database using batch inserts for better performance
        conn = get_db_connection()
        cur = conn.cursor()

        # Prepare batch data
        tracking_data = []
        content_data = []

        for result in results:
            url = result['url']
            status = result['status']
            reason = result.get('reason')
            truncated_reason = reason[:255] if reason and len(reason) > 255 else reason

            tracking_data.append((url, status, truncated_reason))

            if status == 'success':
                content_data.append((
                    url,
                    result.get('page_title', ''),
                    result.get('faq_json', ''),
                    result.get('schema_org', '')
                ))

            print(f"[FAQ] {url} - Status: {status}" + (f" - Reason: {reason}" if reason else f" - {result.get('faq_count', 0)} FAQs"))

        # Batch insert tracking data
        if tracking_data:
            cur.executemany("""
                INSERT INTO pa.faq_tracking (url, status, skip_reason)
                VALUES (%s, %s, %s)
                ON CONFLICT (url) DO UPDATE SET status = EXCLUDED.status, skip_reason = EXCLUDED.skip_reason
            """, tracking_data)

        # Batch insert content data
        if content_data:
            cur.executemany("""
                INSERT INTO pa.faq_content (url, page_title, faq_json, schema_org)
                VALUES (%s, %s, %s, %s)
            """, content_data)

        conn.commit()
        cur.close()
        return_db_connection(conn)

        processed_count = sum(1 for r in results if r['status'] == 'success')
        skipped_count = sum(1 for r in results if r['status'] == 'skipped')
        failed_count = sum(1 for r in results if r['status'] == 'failed')

        print(f"[FAQ BATCH COMPLETE] Processed: {processed_count}/{len(urls)} | Skipped: {skipped_count} | Failed: {failed_count}")

        return {
            "status": "success",
            "processed": processed_count,
            "skipped": skipped_count,
            "failed": failed_count,
            "total_attempted": len(results),
            "results": results
        }

    except Exception as e:
        print(f"[FAQ ERROR] process_faq_urls failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


def faq_json_to_html(faq_json_str: str) -> str:
    """Convert FAQ JSON to HTML with bold questions and regular answers."""
    if not faq_json_str:
        return ''
    try:
        faqs = json.loads(faq_json_str)
        html_parts = []
        for faq in faqs:
            question = faq.get('question', '')
            answer = faq.get('answer', '')
            html_parts.append(f'<b>{question}</b><br>{answer}')
        content = '<br><br>'.join(html_parts)
        return f'<br />{content}<br />' if content else ''
    except (json.JSONDecodeError, TypeError):
        return ''


@app.get("/api/faq/export/xlsx")
async def export_faq_xlsx():
    """Export all generated FAQs as Excel XLSX"""
    from openpyxl import Workbook

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT url, page_title, faq_json, schema_org
            FROM pa.faq_content
        """)
        rows = cur.fetchall()

        cur.close()
        return_db_connection(conn)

        # Create Excel workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "FAQ Export"

        # Add headers (matching the format user requested: url, content_faq, content_bottom)
        ws.append(['url', 'content_faq', 'content_bottom'])

        # Add data rows
        import re
        # Remove control characters that Excel doesn't allow
        illegal_chars = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')

        for row in rows:
            # Build JSON-LD script tag for content_faq column
            schema_org = row['schema_org'] if row['schema_org'] else '{}'
            content_faq = f'<script type="application/ld+json">\n{schema_org}\n</script>'
            content_faq = illegal_chars.sub('', content_faq)

            # Build HTML for content_bottom column
            content_bottom = faq_json_to_html(row['faq_json'])
            content_bottom = illegal_chars.sub('', content_bottom)

            ws.append([row['url'], content_faq, content_bottom])

        # Auto-adjust column widths
        ws.column_dimensions["A"].width = 80
        ws.column_dimensions["B"].width = 100
        ws.column_dimensions["C"].width = 100

        # Save to BytesIO
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=faq_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/faq/export/json")
async def export_faq_json():
    """Export all generated FAQs as JSON"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT url, page_title, faq_json, schema_org
            FROM pa.faq_content
        """)
        rows = cur.fetchall()

        cur.close()
        return_db_connection(conn)

        # Convert to JSON-serializable format
        data = []
        for row in rows:
            data.append({
                'url': row['url'],
                'page_title': row['page_title'],
                'faqs': json.loads(row['faq_json']) if row['faq_json'] else [],
                'schema_org': json.loads(row['schema_org']) if row['schema_org'] else {}
            })

        # Return as downloadable file
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        return StreamingResponse(
            iter([json_str]),
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename=faq_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/faq/validate-links")
def validate_faq_links_endpoint(batch_size: int = 100, parallel_workers: int = 3):
    """
    Validate hyperlinks in FAQ content using Elasticsearch lookup.

    - Only validates FAQs that haven't been validated yet
    - Checks all product links (/p/) in FAQ answers
    - If product is gone, resets FAQ URL to pending for regeneration
    - Records validation results to avoid re-validating

    Args:
        batch_size: Number of FAQs to validate per batch (default: 100)
        parallel_workers: Number of parallel workers (default: 3)
    """
    from backend.link_validator import validate_faq_links, reset_faq_to_pending
    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get FAQs that haven't been validated yet (LEFT JOIN)
        cur.execute("""
            SELECT c.url, c.faq_json
            FROM pa.faq_content c
            LEFT JOIN pa.faq_validation_results v ON c.url = v.url
            WHERE c.faq_json IS NOT NULL
              AND v.url IS NULL
            LIMIT %s
        """, (batch_size,))
        rows = cur.fetchall()

        cur.close()
        return_db_connection(conn)

        if not rows:
            return {
                "status": "success",
                "message": "No unvalidated FAQs found",
                "validated": 0,
                "reset_to_pending": 0
            }

        # Validate each FAQ and collect results
        all_urls_with_gone = []
        total_links = 0
        total_valid = 0
        total_gone = 0
        validation_records = []

        def validate_single(row):
            url = row['url']
            faq_json = row['faq_json']
            result = validate_faq_links(faq_json)
            return {
                'url': url,
                'total_links': result['total_links'],
                'valid_links': result['valid_links'],
                'gone_links': result['gone_links'],
                'has_gone': result['has_gone_links']
            }

        # Process in parallel
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            futures = {executor.submit(validate_single, row): row for row in rows}

            for future in as_completed(futures):
                result = future.result()
                total_links += result['total_links']
                total_valid += result['valid_links']
                total_gone += len(result['gone_links']) if isinstance(result['gone_links'], list) else result['gone_links']

                validation_records.append(result)

                if result['has_gone']:
                    all_urls_with_gone.append(result['url'])

        # Record validation results (for URLs that passed - no gone products)
        conn = get_db_connection()
        cur = conn.cursor()
        for record in validation_records:
            if not record['has_gone']:  # Only record if no gone products
                cur.execute("""
                    INSERT INTO pa.faq_validation_results (url, total_links, valid_links, gone_links)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (url) DO UPDATE SET
                        total_links = EXCLUDED.total_links,
                        valid_links = EXCLUDED.valid_links,
                        gone_links = EXCLUDED.gone_links,
                        validated_at = CURRENT_TIMESTAMP
                """, (record['url'], record['total_links'], record['valid_links'], 0))
        conn.commit()
        cur.close()
        return_db_connection(conn)

        # Reset FAQs with gone products to pending
        reset_count = 0
        if all_urls_with_gone:
            reset_count = reset_faq_to_pending(all_urls_with_gone)

        return {
            "status": "success",
            "validated": len(rows),
            "total_links_checked": total_links,
            "valid_links": total_valid,
            "gone_links": total_gone,
            "faqs_with_gone_products": len(all_urls_with_gone),
            "reset_to_pending": reset_count,
            "urls_reset": all_urls_with_gone[:20]  # Show first 20 for debugging
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/faq/validate-all-links")
def validate_all_faq_links(parallel_workers: int = 3):
    """
    Validate ALL unvalidated FAQ links until complete.

    - Only validates FAQs that haven't been validated yet
    - Processes in batches, resetting any with gone products to pending
    - Records validation results to avoid re-validating
    """
    from backend.link_validator import validate_faq_links, reset_faq_to_pending
    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        batch_size = 500
        total_validated = 0
        total_reset = 0
        total_links_checked = 0
        total_gone_links = 0

        while True:
            conn = get_db_connection()
            cur = conn.cursor()

            # Get next batch of unvalidated FAQs (LEFT JOIN)
            cur.execute("""
                SELECT c.url, c.faq_json
                FROM pa.faq_content c
                LEFT JOIN pa.faq_validation_results v ON c.url = v.url
                WHERE c.faq_json IS NOT NULL
                  AND v.url IS NULL
                LIMIT %s
            """, (batch_size,))
            rows = cur.fetchall()

            cur.close()
            return_db_connection(conn)

            if not rows:
                break

            # Validate each FAQ
            batch_urls_with_gone = []
            validation_records = []

            def validate_single(row):
                url = row['url']
                faq_json = row['faq_json']
                result = validate_faq_links(faq_json)
                return {
                    'url': url,
                    'total_links': result['total_links'],
                    'valid_links': result['valid_links'],
                    'gone_links': result['gone_links'],
                    'has_gone': result['has_gone_links']
                }

            with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                futures = {executor.submit(validate_single, row): row for row in rows}

                for future in as_completed(futures):
                    result = future.result()
                    total_links_checked += result['total_links']
                    gone_count = len(result['gone_links']) if isinstance(result['gone_links'], list) else result['gone_links']
                    total_gone_links += gone_count

                    validation_records.append(result)

                    if result['has_gone']:
                        batch_urls_with_gone.append(result['url'])

            # Record validation results (for URLs that passed)
            conn = get_db_connection()
            cur = conn.cursor()
            for record in validation_records:
                if not record['has_gone']:
                    cur.execute("""
                        INSERT INTO pa.faq_validation_results (url, total_links, valid_links, gone_links)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (url) DO UPDATE SET
                            total_links = EXCLUDED.total_links,
                            valid_links = EXCLUDED.valid_links,
                            gone_links = EXCLUDED.gone_links,
                            validated_at = CURRENT_TIMESTAMP
                    """, (record['url'], record['total_links'], record['valid_links'], 0))
            conn.commit()
            cur.close()
            return_db_connection(conn)

            # Reset FAQs with gone products
            if batch_urls_with_gone:
                reset_count = reset_faq_to_pending(batch_urls_with_gone)
                total_reset += reset_count

            total_validated += len(rows)
            print(f"[FAQ-VALIDATE] Batch complete: {len(rows)} validated, {len(batch_urls_with_gone)} reset. Total: {total_validated}")

        return {
            "status": "success",
            "message": f"Validated all {total_validated} unvalidated FAQs",
            "validated": total_validated,
            "total_links_checked": total_links_checked,
            "gone_links": total_gone_links,
            "reset_to_pending": total_reset
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/faq/validation-history/reset")
def reset_faq_validation_history():
    """
    Reset all FAQ validation history to allow re-validation of all FAQs.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("DELETE FROM pa.faq_validation_results")
        deleted = cur.rowcount

        conn.commit()
        cur.close()
        return_db_connection(conn)

        return {
            "status": "success",
            "message": f"Reset validation history for {deleted} FAQs",
            "deleted": deleted
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export/combined/xlsx")
async def export_combined_xlsx():
    """
    Export combined FAQ and content_top results as Excel XLSX.
    Columns: url, content_faq, content_top, content_bottom, country_language
    Includes ALL URLs from both tables (empty cells where data is missing).
    """
    from openpyxl import Workbook

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Full outer join to get ALL URLs from both tables
        cur.execute("""
            SELECT
                COALESCE(f.url, c.url) as url,
                f.schema_org as content_faq,
                f.faq_json,
                c.content as content_top
            FROM pa.faq_content f
            FULL OUTER JOIN pa.content_urls_joep c ON f.url = c.url
            ORDER BY COALESCE(f.url, c.url)
        """)
        rows = cur.fetchall()

        cur.close()
        return_db_connection(conn)

        # Create Excel workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Combined Export"

        # Add headers
        ws.append(['url', 'content_faq', 'content_top', 'content_bottom', 'country_language'])

        # Add data rows
        import re
        # Remove control characters that Excel doesn't allow
        illegal_chars = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')

        for row in rows:
            # Build JSON-LD script tag for content_faq column (empty if no FAQ data)
            if row['content_faq']:
                content_faq = f'<script type="application/ld+json">\n{row["content_faq"]}\n</script>'
                content_faq = illegal_chars.sub('', content_faq)
            else:
                content_faq = ''

            content_top = row['content_top'] if row['content_top'] else ''
            if content_top:
                content_top = illegal_chars.sub('', content_top)

            # Build HTML for content_bottom column (empty if no FAQ data)
            content_bottom = faq_json_to_html(row['faq_json']) if row['faq_json'] else ''
            if content_bottom:
                content_bottom = illegal_chars.sub('', content_bottom)

            ws.append([row['url'], content_faq, content_top, content_bottom, 'nl-nl'])

        # Auto-adjust column widths
        ws.column_dimensions["A"].width = 80
        ws.column_dimensions["B"].width = 100
        ws.column_dimensions["C"].width = 100
        ws.column_dimensions["D"].width = 100
        ws.column_dimensions["E"].width = 15

        # Save to BytesIO
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=combined_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/faq/result/{url:path}")
async def delete_faq_result(url: str):
    """Delete a FAQ result and reset the URL back to pending state"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Delete from FAQ content table
        cur.execute("""
            DELETE FROM pa.faq_content
            WHERE url = %s
        """, (url,))

        # Delete from FAQ tracking table
        cur.execute("""
            DELETE FROM pa.faq_tracking
            WHERE url = %s
        """, (url,))

        conn.commit()
        cur.close()
        return_db_connection(conn)

        return {
            "status": "success",
            "message": f"FAQ result deleted and URL reset to pending",
            "url": url
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Content Publishing API
# ============================================================================

@app.get("/api/content-publish/stats")
async def get_content_publish_stats():
    """Get statistics about content available for publishing."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get counts
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM pa.content_urls_joep WHERE content IS NOT NULL) as content_top_count,
                (SELECT COUNT(*) FROM pa.faq_content WHERE faq_json IS NOT NULL) as faq_count,
                (SELECT COUNT(DISTINCT COALESCE(c.url, f.url))
                 FROM pa.content_urls_joep c
                 FULL OUTER JOIN pa.faq_content f ON c.url = f.url
                 WHERE c.content IS NOT NULL OR f.faq_json IS NOT NULL) as total_unique_urls
        """)
        row = cur.fetchone()

        cur.close()
        return_db_connection(conn)

        return {
            "content_top_count": row['content_top_count'],
            "faq_count": row['faq_count'],
            "total_unique_urls": row['total_unique_urls']
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/content-publish/preview")
async def preview_content_publish(offset: int = 0, limit: int = 10):
    """Preview content that will be published."""
    try:
        items = get_content_batch(offset, min(limit, 100))
        total = get_total_content_count()

        return {
            "total": total,
            "offset": offset,
            "limit": limit,
            "items": items
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/content-publish/curl")
async def get_content_publish_curl(limit: int = 10, environment: str = "dev"):
    """
    Generate a curl command for publishing content.

    Args:
        limit: Number of items to include (default: 10, max: 100)
        environment: Target environment (dev, staging, production)
    """
    try:
        if environment not in ("dev", "staging", "production"):
            raise HTTPException(status_code=400, detail="Invalid environment. Use: dev, staging, production")
        curl_cmd = generate_curl_command(limit=min(limit, 100), environment=environment)
        return {
            "environment": environment,
            "curl_command": curl_cmd
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/content-publish")
async def publish_content(environment: str = "dev", content_type: str = "all"):
    """
    Publish content to the website-configuration API in a single call.

    Args:
        environment: Target environment (dev, staging, production)
        content_type: What to publish - "all", "seo_only", or "faq_only"
    """
    try:
        if environment not in ("dev", "staging", "production"):
            raise HTTPException(status_code=400, detail="Invalid environment. Use: dev, staging, production")

        if content_type not in ("all", "seo_only", "faq_only"):
            raise HTTPException(status_code=400, detail="Invalid content_type. Use: all, seo_only, faq_only")

        # Start background task for publishing
        task_id = start_publish_task(environment=environment, content_type=content_type)
        return {
            "status": "started",
            "task_id": task_id,
            "environment": environment,
            "content_type": content_type,
            "message": "Publishing started in background. Use /api/content-publish/status/{task_id} to check progress."
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/content-publish/status/{task_id}")
async def get_publish_status(task_id: str):
    """
    Get the status of a content publishing task.
    """
    try:
        status = get_publish_task_status(task_id)
        if "error" in status and status["error"] == "Task not found":
            raise HTTPException(status_code=404, detail="Task not found")
        return status
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
