#!/usr/bin/env python3
"""
Redirect Checker - Check HTTP status codes, redirects, and canonical URLs.
Uses async requests with rate limiting and parallel workers.

Usage:
    python redirect_checker.py input.xlsx output.xlsx
    python redirect_checker.py input.csv output.csv

    Options:
        --workers N     Number of parallel workers (default: 20)
        --rate N        Requests per second (default: 2)
        --timeout N     Request timeout in seconds (default: 15)
        --url-column X  Column name containing URLs (default: 'url')
"""
import pandas as pd
import asyncio
import httpx
import time
import sys
import argparse
import re
from urllib.parse import urljoin

USER_AGENT = "Beslist script voor SEO"
BASE_URL = "https://www.beslist.nl"
DEFAULT_WORKERS = 20
DEFAULT_RATE = 2  # 2 requests per second total
DEFAULT_TIMEOUT = 15

def normalize_url(url: str) -> str:
    """Normalize URL by adding base URL for relative paths."""
    url = url.strip()
    if not url:
        return url
    if url.startswith(('http://', 'https://')):
        return url
    if url.startswith('/'):
        return BASE_URL + url
    return BASE_URL + '/' + url

def log(msg):
    print(msg, flush=True)

def extract_canonical(html, base_url):
    """Extract canonical URL from HTML using regex."""
    try:
        # Match <link rel="canonical" href="..."> or <link href="..." rel="canonical">
        patterns = [
            r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']',
            r'<link[^>]+href=["\']([^"\']+)["\'][^>]+rel=["\']canonical["\']',
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                canonical = match.group(1)
                # Handle relative URLs
                if not canonical.startswith(('http://', 'https://')):
                    canonical = urljoin(base_url, canonical)
                return canonical
    except Exception:
        pass
    return None

async def check_url(client, url, semaphore, rate_limiter, timeout):
    """Check a single URL for status code, redirect, and canonical."""
    async with semaphore:
        async with rate_limiter:
            result = {
                'input_url': url,
                'status_code': None,
                'final_url': None,
                'redirect_url': None,
                'canonical_url': None,
                'error': None
            }

            try:
                # First request without following redirects to capture redirect
                response = await client.get(url, follow_redirects=False, timeout=timeout)
                initial_status = response.status_code

                # Check for redirect
                if initial_status in (301, 302, 303, 307, 308):
                    redirect_location = response.headers.get('Location')
                    if redirect_location:
                        # Handle relative redirect URLs
                        if not redirect_location.startswith(('http://', 'https://')):
                            redirect_location = urljoin(url, redirect_location)
                        result['redirect_url'] = redirect_location

                # Second request following redirects to get final URL and canonical
                response = await client.get(url, follow_redirects=True, timeout=timeout)
                result['status_code'] = response.status_code
                result['final_url'] = str(response.url)

                # Only extract canonical for successful responses
                if response.status_code == 200:
                    try:
                        html = response.text
                        result['canonical_url'] = extract_canonical(html, str(response.url))
                    except Exception:
                        pass

                # Set redirect_url to final_url if different from input
                if result['final_url'] != url and not result['redirect_url']:
                    result['redirect_url'] = result['final_url']

            except httpx.TimeoutException:
                result['status_code'] = 'TIMEOUT'
                result['error'] = 'Request timed out'
            except httpx.RequestError as e:
                result['status_code'] = 'ERROR'
                result['error'] = str(e)[:100]
            except Exception as e:
                result['status_code'] = 'ERROR'
                result['error'] = str(e)[:100]

            return result

class RateLimiter:
    """Token bucket rate limiter."""
    def __init__(self, rate):
        self.rate = rate
        self.tokens = rate
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()

    async def __aenter__(self):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_update = now

            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1

    async def __aexit__(self, *args):
        pass

async def process_urls(urls, num_workers, rate_limit, timeout):
    """Process all URLs and return results."""
    semaphore = asyncio.Semaphore(num_workers)
    rate_limiter = RateLimiter(rate_limit)

    headers = {"User-Agent": USER_AGENT}
    limits = httpx.Limits(max_connections=num_workers, max_keepalive_connections=num_workers)

    results = []
    completed = 0
    total = len(urls)
    start_time = time.time()

    async with httpx.AsyncClient(headers=headers, limits=limits) as client:
        tasks = [check_url(client, url, semaphore, rate_limiter, timeout) for url in urls]

        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            completed += 1

            # Progress logging
            if completed % 100 == 0 or completed == total:
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (total - completed) / rate if rate > 0 else 0
                log(f"Progress: {completed}/{total} ({100*completed/total:.1f}%) - {rate:.2f} req/sec - ETA: {eta:.0f}s")

    return results

def read_input(filepath, url_column):
    """Read URLs from Excel or CSV file."""
    if filepath.endswith('.xlsx') or filepath.endswith('.xls'):
        df = pd.read_excel(filepath)
    elif filepath.endswith('.csv'):
        df = pd.read_csv(filepath)
    else:
        # Try to auto-detect
        try:
            df = pd.read_excel(filepath)
        except Exception:
            df = pd.read_csv(filepath)

    if url_column not in df.columns:
        # Try to find a column with 'url' in the name
        url_cols = [c for c in df.columns if 'url' in c.lower()]
        if url_cols:
            url_column = url_cols[0]
            log(f"Using column '{url_column}' for URLs")
        else:
            raise ValueError(f"Column '{url_column}' not found. Available columns: {list(df.columns)}")

    return df, url_column

def save_output(df, results, filepath):
    """Save results to Excel or CSV file."""
    # Create results dataframe
    results_df = pd.DataFrame(results)

    # Merge with original dataframe
    df_result = df.merge(
        results_df,
        left_on=df.columns[df.columns.str.lower().str.contains('url')][0] if any(df.columns.str.lower().str.contains('url')) else df.columns[0],
        right_on='input_url',
        how='left'
    )

    # Reorder columns to put results first
    result_cols = ['input_url', 'status_code', 'redirect_url', 'canonical_url', 'final_url', 'error']
    other_cols = [c for c in df_result.columns if c not in result_cols]
    df_result = df_result[result_cols + other_cols]

    if filepath.endswith('.xlsx') or filepath.endswith('.xls'):
        df_result.to_excel(filepath, index=False)
    else:
        df_result.to_csv(filepath, index=False)

    return df_result

def main():
    parser = argparse.ArgumentParser(description='Check HTTP status codes, redirects, and canonical URLs')
    parser.add_argument('input', help='Input file (Excel or CSV)')
    parser.add_argument('output', nargs='?', help='Output file (Excel or CSV). If not specified, overwrites input.')
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS, help=f'Number of parallel workers (default: {DEFAULT_WORKERS})')
    parser.add_argument('--rate', type=float, default=DEFAULT_RATE, help=f'Requests per second (default: {DEFAULT_RATE})')
    parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT, help=f'Request timeout in seconds (default: {DEFAULT_TIMEOUT})')
    parser.add_argument('--url-column', default='url', help='Column name containing URLs (default: url)')

    args = parser.parse_args()

    input_file = args.input
    output_file = args.output or input_file

    log("=" * 80)
    log("REDIRECT CHECKER")
    log("=" * 80)
    log(f"Input:       {input_file}")
    log(f"Output:      {output_file}")
    log(f"Workers:     {args.workers}")
    log(f"Rate limit:  {args.rate} req/sec")
    log(f"Timeout:     {args.timeout}s")
    log(f"User-Agent:  {USER_AGENT}")
    log(f"Base URL:    {BASE_URL} (for relative URLs)")
    log("=" * 80)

    # Read input
    log(f"\nReading {input_file}...")
    df, url_column = read_input(input_file, args.url_column)
    urls = df[url_column].dropna().astype(str).tolist()

    # Normalize URLs (handle relative URLs)
    urls = [normalize_url(url) for url in urls if url.strip()]
    log(f"Found {len(urls)} URLs to check")

    # Process URLs
    log(f"\nProcessing URLs...")
    start_time = time.time()
    results = asyncio.run(process_urls(urls, args.workers, args.rate, args.timeout))
    elapsed = time.time() - start_time

    # Summary statistics
    log("\n" + "=" * 80)
    log("SUMMARY")
    log("=" * 80)

    status_counts = {}
    redirect_count = 0
    canonical_diff_count = 0

    for r in results:
        status = str(r['status_code'])
        status_counts[status] = status_counts.get(status, 0) + 1
        if r['redirect_url']:
            redirect_count += 1
        if r['canonical_url'] and r['canonical_url'] != r['final_url']:
            canonical_diff_count += 1

    log(f"\nStatus codes:")
    for status, count in sorted(status_counts.items()):
        log(f"  {status}: {count}")

    log(f"\nRedirects found: {redirect_count}")
    log(f"Canonical differs from final URL: {canonical_diff_count}")
    log(f"Total time: {elapsed:.1f}s ({len(urls)/elapsed:.2f} URLs/sec)")

    # Save results
    log(f"\nSaving to {output_file}...")
    df_result = save_output(df, results, output_file)
    log(f"Saved {len(df_result)} rows")

    log("\nDone!")

if __name__ == "__main__":
    main()
