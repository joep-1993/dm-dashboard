#!/usr/bin/env python3
"""
Rate limit testing script for Beslist.nl scraping
Uses exact same setup as scraper_service.py to test rate limits
"""

import requests
import time
import random
from typing import List, Dict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Exact same user agent as scraper
USER_AGENT = "Beslist script voor SEO"

# Test URLs from database
TEST_URLS = [
    "https://www.beslist.nl/products/accessoires/accessoires_2596345/c/kleur~2596368~~merk~2685973~~populaire_themas_accessoires~2803185",
    "https://www.beslist.nl/products/accessoires/accessoires_2596345/c/merk~2685977",
    "https://www.beslist.nl/products/accessoires/accessoires_2596345/c/merk~2686021",
    "https://www.beslist.nl/products/accessoires/accessoires_2596345/c/merk~2686037~~t_accu~7224001",
    "https://www.beslist.nl/products/accessoires/accessoires_2596345/c/populaire_themas_accessoires~17684137",
    "https://www.beslist.nl/products/accessoires/accessoires_2596345/c/populaire_themas_accessoires~19246726",
    "https://www.beslist.nl/products/accessoires/accessoires_2596345_3541068/c/merk~2686080",
    "https://www.beslist.nl/products/accessoires/accessoires_2596345_3541068/c/populaire_themas_accessoires~23541893",
    "https://www.beslist.nl/products/accessoires/accessoires_2596345_3541070/c/merk~2685973",
    "https://www.beslist.nl/products/accessoires/accessoires_2596345_3541070/c/merk~2685991",
    "https://www.beslist.nl/products/accessoires/accessoires_2596345_8384027/c/t_accu~7224003~~v_batterij~10557331",
    "https://www.beslist.nl/products/accessoires/accessoires_2596346/c/merk~2686086",
    "https://www.beslist.nl/products/accessoires/accessoires_2596346/c/populaire_themas_accessoires~19243799",
    "https://www.beslist.nl/products/accessoires/accessoires_2596346/c/type_converter~13755958",
    "https://www.beslist.nl/products/accessoires/accessoires_2596346_12024831/c/populaire_themas_accessoires~13516928",
    "https://www.beslist.nl/products/accessoires/accessoires_2596346_12024833/c/merk~2938886",
    "https://www.beslist.nl/products/accessoires/accessoires_2596346_2665500/c/merk~2767509",
    "https://www.beslist.nl/products/accessoires/accessoires_2596346_2665500/c/merk~6781090",
    "https://www.beslist.nl/products/accessoires/accessoires_2596346_2665501/c/merk~2961946",
    "https://www.beslist.nl/products/accessoires/accessoires_2596346_2665503/c/merk~2767512",
]

def create_session():
    """Create a requests session with retry logic (same as scraper)"""
    session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=1, pool_maxsize=1)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session

def clean_url(url: str) -> str:
    """Remove query parameters from URL"""
    return url.split("?")[0] if url else ""

def test_rate_with_delay(session: requests.Session, urls: List[str], base_delay: float, variance: float = 0.0, test_name: str = "") -> Dict:
    """
    Test scraping rate with given delay parameters

    Args:
        session: requests session
        urls: list of URLs to test
        base_delay: base delay in seconds
        variance: random variance to add (0 to variance seconds)
        test_name: name of the test for reporting

    Returns:
        dict with test results
    """
    print(f"\n{'='*60}")
    print(f"Test: {test_name}")
    print(f"Base delay: {base_delay}s, Variance: {variance}s")
    print(f"Effective delay range: {base_delay:.2f}-{base_delay + variance:.2f}s")
    print(f"{'='*60}")

    # Exact same headers as scraper
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none"
    }

    results = {
        "test_name": test_name,
        "base_delay": base_delay,
        "variance": variance,
        "status_codes": {},
        "success_count": 0,
        "rate_limited_count": 0,
        "error_count": 0,
        "total_requests": len(urls),
        "urls_tested": []
    }

    for idx, url in enumerate(urls, 1):
        clean = clean_url(url)

        # Apply delay before request (same as scraper)
        actual_delay = base_delay + random.uniform(0, variance)
        time.sleep(actual_delay)

        try:
            start_time = time.time()
            response = session.get(clean, headers=headers, timeout=30)
            elapsed = time.time() - start_time

            status = response.status_code

            # Track status codes
            if status not in results["status_codes"]:
                results["status_codes"][status] = 0
            results["status_codes"][status] += 1

            # Categorize results
            if status == 200:
                results["success_count"] += 1
                status_emoji = "✓"
            elif status in [202, 405, 429]:
                results["rate_limited_count"] += 1
                status_emoji = "✗"
            else:
                results["error_count"] += 1
                status_emoji = "?"

            print(f"{status_emoji} [{idx}/{len(urls)}] Status: {status} | Delay: {actual_delay:.2f}s | Response: {elapsed:.2f}s | URL: {clean[:80]}...")

            results["urls_tested"].append({
                "url": clean,
                "status": status,
                "delay": actual_delay,
                "response_time": elapsed
            })

        except requests.RequestException as e:
            print(f"✗ [{idx}/{len(urls)}] Request failed: {str(e)[:100]}")
            results["error_count"] += 1
            results["urls_tested"].append({
                "url": clean,
                "status": "error",
                "error": str(e)
            })

    # Print summary
    print(f"\n{'-'*60}")
    print(f"Summary for: {test_name}")
    print(f"Total requests: {results['total_requests']}")
    print(f"Success (200): {results['success_count']} ({results['success_count']/results['total_requests']*100:.1f}%)")
    print(f"Rate limited (202/405/429): {results['rate_limited_count']} ({results['rate_limited_count']/results['total_requests']*100:.1f}%)")
    print(f"Errors: {results['error_count']}")
    print(f"Status code breakdown: {results['status_codes']}")
    print(f"{'-'*60}")

    return results

def main():
    """Run progressive rate limit tests"""
    print("\n" + "="*60)
    print("RATE LIMIT TESTING FOR BESLIST.NL SCRAPER")
    print("="*60)
    print(f"User-Agent: {USER_AGENT}")
    print(f"URLs to test: {len(TEST_URLS)}")
    print("="*60)

    session = create_session()
    all_results = []

    # Test 1: Very conservative (1.0-1.3s) - should definitely work
    result1 = test_rate_with_delay(
        session,
        TEST_URLS[:10],
        base_delay=1.0,
        variance=0.3,
        test_name="Test 1: Very Conservative (1.0-1.3s)"
    )
    all_results.append(result1)

    # If Test 1 was successful, continue with faster rates
    if result1["success_count"] >= 8:  # At least 80% success
        print("\n✓ Test 1 successful, continuing with faster rate...")
        time.sleep(2)  # Cool-down period

        # Test 2: Current scraper rate (0.5-0.7s)
        result2 = test_rate_with_delay(
            session,
            TEST_URLS[:10],
            base_delay=0.5,
            variance=0.2,
            test_name="Test 2: Current Scraper Rate (0.5-0.7s)"
        )
        all_results.append(result2)

        if result2["success_count"] >= 8:
            print("\n✓ Test 2 successful, trying even faster...")
            time.sleep(2)

            # Test 3: Faster (0.3-0.5s)
            result3 = test_rate_with_delay(
                session,
                TEST_URLS[:10],
                base_delay=0.3,
                variance=0.2,
                test_name="Test 3: Faster (0.3-0.5s)"
            )
            all_results.append(result3)

            if result3["success_count"] >= 8:
                print("\n✓ Test 3 successful, pushing limits...")
                time.sleep(2)

                # Test 4: Very fast (0.1-0.3s)
                result4 = test_rate_with_delay(
                    session,
                    TEST_URLS[:10],
                    base_delay=0.1,
                    variance=0.2,
                    test_name="Test 4: Very Fast (0.1-0.3s)"
                )
                all_results.append(result4)

    # Final report
    print("\n" + "="*60)
    print("FINAL REPORT")
    print("="*60)

    for result in all_results:
        success_rate = result["success_count"] / result["total_requests"] * 100
        rate_limited_rate = result["rate_limited_count"] / result["total_requests"] * 100

        if success_rate >= 80:
            verdict = "✓ SAFE"
        elif success_rate >= 50:
            verdict = "⚠ BORDERLINE"
        else:
            verdict = "✗ TOO FAST"

        print(f"\n{result['test_name']}")
        print(f"  Delay: {result['base_delay']}-{result['base_delay'] + result['variance']}s")
        print(f"  Success: {success_rate:.1f}%")
        print(f"  Rate limited: {rate_limited_rate:.1f}%")
        print(f"  Verdict: {verdict}")

    print("\n" + "="*60)
    print("RECOMMENDATION")
    print("="*60)

    # Find optimal rate
    safe_results = [r for r in all_results if r["success_count"] / r["total_requests"] >= 0.8]
    if safe_results:
        fastest_safe = min(safe_results, key=lambda x: x["base_delay"])
        print(f"\nOptimal rate: {fastest_safe['base_delay']}-{fastest_safe['base_delay'] + fastest_safe['variance']}s delay")
        print(f"Success rate: {fastest_safe['success_count'] / fastest_safe['total_requests'] * 100:.1f}%")
    else:
        print("\nAll tested rates showed rate limiting. Recommend starting with 1.5s+ delay.")

    print("\n" + "="*60)

if __name__ == "__main__":
    main()
