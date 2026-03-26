"""
Download CloudFront log files from an S3 bucket.

Usage from PyCharm / Python console:
    from download_cloudfront_logs import main
    main(list_only=True)
    main(date="2026-03-26")
    main(from_date="2026-03-20")
    main(days=7)
    main()  # download all
"""

import os
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# AWS credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")

# S3 bucket
BUCKET_NAME = "production-projectstack-1hts6sh41-logbucketbucket-10tf48d8lt2pt"
PREFIX = ""

# Default download directory
DEFAULT_DOWNLOAD_DIR = r"C:\Users\JoepvanSchagen\Downloads\Cloudfront"


def create_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def list_log_files(s3_client, date_filter: str = None) -> list:
    """List all .gz log files in the bucket/prefix, optionally filtered by date."""
    files = []
    paginator = s3_client.get_paginator("list_objects_v2")

    print(f"Listing files in s3://{BUCKET_NAME}/{PREFIX} ...")

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".gz") or key.endswith(".log"):
                if date_filter:
                    if date_filter in key:
                        files.append(obj)
                else:
                    files.append(obj)

    return files


def download_files(s3_client, files: list, download_dir: str) -> int:
    """Download log files to the local directory."""
    os.makedirs(download_dir, exist_ok=True)

    downloaded = 0
    skipped = 0
    total = len(files)

    for i, obj in enumerate(files, 1):
        key = obj["Key"]
        filename = os.path.basename(key)
        local_path = os.path.join(download_dir, filename)

        # Skip if already downloaded (same size)
        if os.path.exists(local_path) and os.path.getsize(local_path) == obj["Size"]:
            skipped += 1
            continue

        size_kb = obj["Size"] / 1024
        print(f"  [{i}/{total}] {filename} ({size_kb:.1f} KB)")

        try:
            s3_client.download_file(BUCKET_NAME, key, local_path)
            downloaded += 1
        except ClientError as e:
            print(f"    Error downloading {filename}: {e}")

    if skipped > 0:
        print(f"Skipped {skipped} already-downloaded files.")

    return downloaded


def main(list_only: bool = False, date: str = None, from_date: str = None,
         days: int = None, download_dir: str = DEFAULT_DOWNLOAD_DIR):
    """
    Download CloudFront logs from S3.

    Args:
        list_only: If True, only list files without downloading.
        date: Filter logs for a specific date (e.g. "2026-03-26").
        from_date: Download all logs from this date onwards (e.g. "2026-03-20").
        days: Download logs from the last N days.
        download_dir: Download directory (default: C:\\Users\\JoepvanSchagen\\Downloads\\Cloudfront).
    """
    try:
        s3_client = create_s3_client()
    except NoCredentialsError:
        print("Error: Invalid AWS credentials.")
        return

    # Build date filters
    date_filters = []
    if date:
        date_filters.append(date)
    elif from_date:
        start = datetime.strptime(from_date, "%Y-%m-%d")
        today = datetime.now()
        d = start
        while d <= today:
            date_filters.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
    elif days:
        for i in range(days):
            d = datetime.now() - timedelta(days=i)
            date_filters.append(d.strftime("%Y-%m-%d"))

    # Collect files
    all_files = []
    if date_filters:
        for date_str in date_filters:
            files = list_log_files(s3_client, date_filter=date_str)
            all_files.extend(files)
        print(f"Found {len(all_files)} log files for {len(date_filters)} date(s).")
    else:
        all_files = list_log_files(s3_client)
        print(f"Found {len(all_files)} log files.")

    if not all_files:
        print("No log files found.")
        return

    # Show total size
    total_size_mb = sum(f["Size"] for f in all_files) / (1024 * 1024)
    print(f"Total size: {total_size_mb:.1f} MB")

    if list_only:
        for f in all_files:
            print(f"  {f['Key']} ({f['Size'] / 1024:.1f} KB)")
        return

    # Download
    print(f"\nDownloading to: {download_dir}")
    downloaded = download_files(s3_client, all_files, download_dir)
    print(f"\nDone! Downloaded {downloaded} new files.")


if __name__ == "__main__":
    main(list_only=False, from_date="2026-02-14")
