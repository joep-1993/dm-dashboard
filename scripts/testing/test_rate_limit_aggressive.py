#!/usr/bin/env python3
"""
Aggressive rate limit testing to find the actual breaking point
"""

import requests
import time
import random
from typing import List, Dict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENT = "Beslist script voor SEO"

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
]

def create_session():
    """Create a requests session with retry logic"""
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

def test_burst_rate(session: requests.Session, urls: List[str], delay: float, test_name: str) -> Dict:
    """Test with a specific delay"""
    print(f"\n{'='*60}")
    print(f"{test_name} - Delay: {delay}s")
    print(f"{'='*60}")

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
        "delay": delay,
        "status_codes": {},
        "success": 0,
        "rate_limited": 0,
        "total": len(urls)
    }

    for idx, url in enumerate(urls, 1):
        if idx > 1:
            time.sleep(delay)

        try:
            response = session.get(url, headers=headers, timeout=30)
            status = response.status_code

            if status not in results["status_codes"]:
                results["status_codes"][status] = 0
            results["status_codes"][status] += 1

            if status == 200:
                results["success"] += 1
                print(f"✓ [{idx}/{len(urls)}] 200 OK")
            elif status in [202, 405, 429]:
                results["rate_limited"] += 1
                print(f"✗ [{idx}/{len(urls)}] {status} RATE LIMITED")
            else:
                print(f"? [{idx}/{len(urls)}] {status}")

        except Exception as e:
            print(f"✗ [{idx}/{len(urls)}] ERROR: {str(e)[:50]}")

    success_rate = results["success"] / results["total"] * 100
    print(f"\nSuccess: {results['success']}/{results['total']} ({success_rate:.1f}%)")
    print(f"Rate limited: {results['rate_limited']}/{results['total']}")
    print(f"Status codes: {results['status_codes']}")

    return results

def main():
    print("\n" + "="*60)
    print("AGGRESSIVE RATE LIMIT TESTING")
    print("="*60)

    session = create_session()
    all_results = []

    # Test extremely fast rates
    test_delays = [
        (0.05, "Test 1: 0.05s (50ms)"),
        (0.02, "Test 2: 0.02s (20ms)"),
        (0.01, "Test 3: 0.01s (10ms)"),
        (0.0, "Test 4: 0.0s (NO DELAY - Burst)"),
    ]

    for delay, test_name in test_delays:
        result = test_burst_rate(session, TEST_URLS, delay, test_name)
        all_results.append(result)

        # If we start getting rate limited, stop
        if result["rate_limited"] > result["total"] * 0.2:  # More than 20% rate limited
            print(f"\n⚠ Rate limiting detected at {delay}s delay. Stopping tests.")
            break

        time.sleep(3)  # Cool-down between tests

    # Final summary
    print("\n" + "="*60)
    print("AGGRESSIVE TEST SUMMARY")
    print("="*60)

    for result in all_results:
        success_rate = result["success"] / result["total"] * 100
        if success_rate >= 80:
            verdict = "✓ SAFE"
        elif success_rate >= 50:
            verdict = "⚠ BORDERLINE"
        else:
            verdict = "✗ TOO FAST"

        print(f"\n{result['test_name']}")
        print(f"  Delay: {result['delay']}s")
        print(f"  Success rate: {success_rate:.1f}%")
        print(f"  Verdict: {verdict}")

    print("\n" + "="*60)

if __name__ == "__main__":
    main()
