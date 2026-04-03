"""
SEO Index Checker Service

Checks Google Search Console URL Inspection API to determine if URLs are indexed.
Uses multiple service accounts for quota rotation (2,000 requests/day per account).
"""
import os
from typing import List, Dict
from google.oauth2 import service_account
from googleapiclient.discovery import build

_DIR = os.path.dirname(__file__)

SERVICE_ACCOUNT_DIR = os.path.join(_DIR, "service_accounts")
SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
SITE_URL = "sc-domain:beslist.nl"
MAX_URLS_PER_ACCOUNT = 2000


def _get_service_account_files() -> List[str]:
    """Get sorted list of service account JSON file paths."""
    if not os.path.isdir(SERVICE_ACCOUNT_DIR):
        return []
    return [
        os.path.join(SERVICE_ACCOUNT_DIR, f)
        for f in sorted(os.listdir(SERVICE_ACCOUNT_DIR))
        if f.endswith(".json")
    ]


def _get_service_client(service_account_file):
    """Create a Search Console API client from a service account file."""
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file, scopes=SCOPES
    )
    return build("searchconsole", "v1", credentials=credentials)


def _inspect_url(service, url: str) -> Dict:
    """Inspect a single URL. Returns dict with status details."""
    try:
        request = {"inspectionUrl": url, "siteUrl": SITE_URL}
        response = service.urlInspection().index().inspect(body=request).execute()

        if "inspectionResult" in response and "indexStatusResult" in response["inspectionResult"]:
            status = response["inspectionResult"]["indexStatusResult"]
            coverage = status.get("coverageState", "unknown")
            verdict = status.get("verdict", "unknown")
            robots = status.get("robotsTxtState", "unknown")
            indexing = status.get("indexingState", "unknown")
            is_indexed = "indexed" in coverage.lower() and "not" not in coverage.lower()

            return {
                "url": url,
                "indexed": is_indexed,
                "coverage_state": coverage,
                "verdict": verdict,
                "robots": robots,
                "indexing_state": indexing,
                "error": None,
            }

        return {
            "url": url,
            "indexed": False,
            "coverage_state": "unknown",
            "verdict": "unknown",
            "robots": "unknown",
            "indexing_state": "unknown",
            "error": None,
        }
    except Exception as e:
        error_msg = str(e)
        is_quota = "Quota exceeded" in error_msg or "rateLimitExceeded" in error_msg
        return {
            "url": url,
            "indexed": None,
            "coverage_state": None,
            "verdict": None,
            "robots": None,
            "indexing_state": None,
            "error": "QUOTA_EXCEEDED" if is_quota else error_msg[:200],
        }


def check_urls(urls: List[str], progress_callback=None) -> Dict:
    """
    Check index status for a list of URLs.
    Rotates through service accounts on quota exhaustion.

    Args:
        urls: List of URLs to check
        progress_callback: Optional callable(checked, total) for progress updates

    Returns:
        Dict with results list and stats
    """
    sa_files = _get_service_account_files()
    if not sa_files:
        return {
            "status": "error",
            "message": f"No service account files found in {SERVICE_ACCOUNT_DIR}",
            "results": [],
        }

    # Build service clients
    services = []
    for sa_file in sa_files:
        try:
            client = _get_service_client(sa_file)
            services.append((client, os.path.basename(sa_file)))
        except Exception as e:
            print(f"[INDEX_CHECKER] Failed to load {os.path.basename(sa_file)}: {e}")

    if not services:
        return {
            "status": "error",
            "message": "No valid service accounts could be loaded",
            "results": [],
        }

    results = []
    service_idx = 0
    urls_on_current_account = 0

    for i, url in enumerate(urls):
        if service_idx >= len(services):
            # All accounts exhausted, mark remaining as quota error
            for remaining_url in urls[i:]:
                results.append({
                    "url": remaining_url,
                    "indexed": None,
                    "coverage_state": None,
                    "verdict": None,
                    "robots": None,
                    "indexing_state": None,
                    "error": "QUOTA_EXCEEDED (all accounts exhausted)",
                })
            break

        service, account_name = services[service_idx]
        result = _inspect_url(service, url)
        results.append(result)

        if result["error"] == "QUOTA_EXCEEDED":
            print(f"[INDEX_CHECKER] Quota exhausted for {account_name}, rotating")
            service_idx += 1
            urls_on_current_account = 0
            continue

        urls_on_current_account += 1
        if urls_on_current_account >= MAX_URLS_PER_ACCOUNT:
            service_idx += 1
            urls_on_current_account = 0

        if progress_callback and (i + 1) % 10 == 0:
            progress_callback(i + 1, len(urls))

    indexed = sum(1 for r in results if r["indexed"] is True)
    not_indexed = sum(1 for r in results if r["indexed"] is False)
    errors = sum(1 for r in results if r["error"] is not None)

    return {
        "status": "success",
        "total": len(urls),
        "indexed": indexed,
        "not_indexed": not_indexed,
        "errors": errors,
        "service_accounts_available": len(services),
        "service_accounts_used": min(service_idx + 1, len(services)),
        "results": results,
    }


def get_quota_info() -> Dict:
    """Get info about available service accounts and estimated quota."""
    sa_files = _get_service_account_files()
    accounts = []
    for sa_file in sa_files:
        try:
            creds = service_account.Credentials.from_service_account_file(sa_file, scopes=SCOPES)
            accounts.append({
                "file": os.path.basename(sa_file),
                "project": creds.project_id,
                "status": "ok",
            })
        except Exception as e:
            accounts.append({
                "file": os.path.basename(sa_file),
                "project": None,
                "status": f"error: {str(e)[:100]}",
            })

    valid = sum(1 for a in accounts if a["status"] == "ok")
    return {
        "accounts": accounts,
        "total_accounts": len(accounts),
        "valid_accounts": valid,
        "daily_quota_per_account": MAX_URLS_PER_ACCOUNT,
        "estimated_daily_quota": valid * MAX_URLS_PER_ACCOUNT,
    }
