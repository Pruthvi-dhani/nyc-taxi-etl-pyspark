"""
s3_data_copy.py
===============
Downloads NYC TLC trip-data Parquet files for a given year from the TLC
CloudFront CDN and uploads them directly to an S3 bucket.

Designed to run in AWS CloudShell — streams each file directly from the
CDN into S3 using boto3, so no local disk space is consumed.

Usage
-----
    python s3_data_copy.py              # defaults to year=2025
    python s3_data_copy.py --year 2024
    python s3_data_copy.py --year 2025 --trip-types yellow green

S3 layout produced
------------------
    s3://nyc-taxi-etl-spark-raw/raw/yellow/yellow_tripdata_YYYY-MM.parquet
    s3://nyc-taxi-etl-spark-raw/raw/green/green_tripdata_YYYY-MM.parquet
    s3://nyc-taxi-etl-spark-raw/raw/fhv/fhv_tripdata_YYYY-MM.parquet
    s3://nyc-taxi-etl-spark-raw/raw/fhvhv/fhvhv_tripdata_YYYY-MM.parquet
"""

from __future__ import annotations

import argparse
import logging
import sys
import urllib.error
import urllib.request

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BUCKET = "nyc-taxi-etl-spark-raw"
S3_PREFIX = "raw"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ALL_TRIP_TYPES = ["yellow", "green", "fhv", "fhvhv"]
DEFAULT_YEAR = 2025

# boto3 multipart upload threshold — 100 MB chunks keep memory usage low
CHUNK_SIZE = 100 * 1024 * 1024  # 100 MB

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    format="[%(asctime)s] %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _s3_key(trip_type: str, filename: str) -> str:
    return f"{S3_PREFIX}/{trip_type}/{filename}"


def _file_exists_in_s3(s3_client, key: str) -> bool:
    """Return True if the object already exists in the bucket."""
    try:
        s3_client.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "404":
            return False
        raise


def _stream_url_to_s3(s3_client, url: str, key: str) -> None:
    """
    Stream *url* directly into S3 via a multipart upload.
    No data is written to local disk.
    """
    mpu = s3_client.create_multipart_upload(Bucket=BUCKET, Key=key)
    upload_id = mpu["UploadId"]
    parts: list[dict] = []
    part_number = 1

    try:
        with urllib.request.urlopen(url) as response:
            while True:
                chunk = response.read(CHUNK_SIZE)
                if not chunk:
                    break
                part = s3_client.upload_part(
                    Bucket=BUCKET,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk,
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                part_number += 1

        s3_client.complete_multipart_upload(
            Bucket=BUCKET,
            Key=key,
            MultipartUpload={"Parts": parts},
            UploadId=upload_id,
        )
    except Exception:
        s3_client.abort_multipart_upload(Bucket=BUCKET, Key=key, UploadId=upload_id)
        raise


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------
def copy_year(year: int, trip_types: list[str]) -> None:
    s3 = boto3.client("s3")

    log.info("Starting NYC TLC data copy  →  year=%d  bucket=s3://%s/%s/", year, BUCKET, S3_PREFIX)
    log.info("Trip types: %s", ", ".join(trip_types))

    total = skipped = failed = 0

    for trip_type in trip_types:
        for month in range(1, 13):
            filename = f"{trip_type}_tripdata_{year}-{month:02d}.parquet"
            url = f"{BASE_URL}/{filename}"
            key = _s3_key(trip_type, filename)

            if _file_exists_in_s3(s3, key):
                log.warning("Already exists, skipping: s3://%s/%s", BUCKET, key)
                skipped += 1
                continue

            log.info("Copying  %s", url)
            log.info("      →  s3://%s/%s", BUCKET, key)

            try:
                _stream_url_to_s3(s3, url, key)
                log.info("✓  Done: %s", filename)
                total += 1
            except urllib.error.HTTPError as exc:
                log.warning("FAILED: %s — HTTP %s (%s)", filename, exc.code, exc.reason)
                failed += 1
            except Exception as exc:  # noqa: BLE001
                log.warning("FAILED: %s — %s", filename, exc)
                failed += 1

    # Summary
    print("=" * 60)
    log.info("Copy complete.")
    log.info("  Uploaded : %d", total)
    log.info("  Skipped  : %d  (already existed in S3)", skipped)
    log.info("  Failed   : %d  (file not available on TLC CDN)", failed)
    print("=" * 60)

    if failed:
        log.warning(
            "Some files could not be copied. "
            "This is normal for future months not yet published by the TLC."
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy NYC TLC Parquet files from the CDN into S3."
    )
    parser.add_argument(
        "--year",
        type=int,
        default=DEFAULT_YEAR,
        help=f"Year to ingest (default: {DEFAULT_YEAR})",
    )
    parser.add_argument(
        "--trip-types",
        nargs="+",
        choices=ALL_TRIP_TYPES,
        default=ALL_TRIP_TYPES,
        metavar="TYPE",
        help=f"Trip types to copy. Choices: {ALL_TRIP_TYPES}. Default: all.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    copy_year(year=args.year, trip_types=args.trip_types)


