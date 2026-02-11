"""
Index Checker

Checks Google Search Console URL Inspection API to determine if URLs are indexed.
Uses multiple service accounts for quota rotation (2,000 requests/day per account).
Reads URLs from Excel, writes results back to the same file.
"""
import os
import sys
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from concurrent.futures import ThreadPoolExecutor

_DIR = os.path.dirname(__file__)

# Service account files for quota rotation
SERVICE_ACCOUNT_DIR = os.path.join(_DIR, "service_accounts")
SERVICE_ACCOUNT_FILES = [
    os.path.join(SERVICE_ACCOUNT_DIR, f)
    for f in sorted(os.listdir(SERVICE_ACCOUNT_DIR))
    if f.endswith(".json")
] if os.path.isdir(SERVICE_ACCOUNT_DIR) else []

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
SITE_URL = "sc-domain:beslist.nl"
MAX_URLS_PER_ACCOUNT = 1000

# Default Excel path (can be overridden via command line argument)
DEFAULT_EXCEL_PATH = "/app/backend/index_checker.xlsx"


def get_service_client(service_account_file):
    """Create a Search Console API client from a service account file."""
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file, scopes=SCOPES
    )
    return build("searchconsole", "v1", credentials=credentials)


def get_index_status(service, url):
    """Check the index status of a single URL via the URL Inspection API."""
    try:
        request = {"inspectionUrl": url, "siteUrl": SITE_URL}
        response = service.urlInspection().index().inspect(body=request).execute()
        if "inspectionResult" in response and "indexStatusResult" in response["inspectionResult"]:
            coverage_state = response["inspectionResult"]["indexStatusResult"].get("coverageState", "")
            if "indexed" in coverage_state.lower():
                return "indexed"
            else:
                return f"not indexed ({coverage_state})"
        return "not indexed"
    except Exception as e:
        error_msg = str(e)
        if "Quota exceeded" in error_msg or "rateLimitExceeded" in error_msg:
            return "QUOTA_EXCEEDED"
        return f"Error: {error_msg[:100]}"


def process_batch(service, urls, start_idx, account_name):
    """Process a batch of URLs using a specific service account."""
    results = []
    end_idx = min(start_idx + MAX_URLS_PER_ACCOUNT, len(urls))

    for i in range(start_idx, end_idx):
        result = get_index_status(service, urls[i])
        if result == "QUOTA_EXCEEDED":
            print(f"  Quota exceeded for {account_name} after {len(results)} URLs")
            return results, False
        results.append(result)

        if (len(results)) % 50 == 0:
            print(f"  [{account_name}] Checked {len(results)}/{end_idx - start_idx} URLs...")

    return results, True


def main():
    excel_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_EXCEL_PATH

    if not os.path.exists(excel_path):
        print(f"Excel file not found: {excel_path}")
        sys.exit(1)

    if not SERVICE_ACCOUNT_FILES:
        print(f"No service account JSON files found in {SERVICE_ACCOUNT_DIR}")
        sys.exit(1)

    print(f"Service accounts: {len(SERVICE_ACCOUNT_FILES)}")
    for f in SERVICE_ACCOUNT_FILES:
        print(f"  - {os.path.basename(f)}")

    # Load Excel
    df = pd.read_excel(excel_path)
    print(f"Loaded {len(df)} URLs from {excel_path}")

    # Find first empty cell in column B (index status column)
    if df.shape[1] < 2:
        df.insert(1, "index_status", None)

    start_index = df[df.iloc[:, 1].isnull()].index.min()
    if pd.isna(start_index):
        print("All URLs already checked. Nothing to do.")
        return

    urls = df.iloc[:, 0].astype(str).str.strip().tolist()
    remaining = len(urls) - start_index
    print(f"Starting from row {start_index + 1}, {remaining} URLs remaining")

    # Initialize service clients
    services = []
    for sa_file in SERVICE_ACCOUNT_FILES:
        try:
            services.append((get_service_client(sa_file), os.path.basename(sa_file)))
        except Exception as e:
            print(f"  Failed to load {os.path.basename(sa_file)}: {e}")

    if not services:
        print("No valid service accounts loaded. Exiting.")
        sys.exit(1)

    # Process URLs with service account rotation
    service_idx = 0
    current_index = start_index

    while current_index < len(urls) and service_idx < len(services):
        service, account_name = services[service_idx]
        print(f"\n[{service_idx + 1}/{len(services)}] Using {account_name} (from row {current_index + 1})")

        results, success = process_batch(service, urls, current_index, account_name)

        if results:
            df.iloc[current_index:current_index + len(results), 1] = results
            df.to_excel(excel_path, index=False)
            current_index += len(results)
            print(f"  Saved {len(results)} results. Progress: {current_index}/{len(urls)}")

        if not success:
            service_idx += 1
            if service_idx >= len(services):
                print("\nAll service accounts exhausted.")

    print(f"\nDone. Checked {current_index - start_index} URLs. Results saved to {excel_path}")


if __name__ == "__main__":
    main()
