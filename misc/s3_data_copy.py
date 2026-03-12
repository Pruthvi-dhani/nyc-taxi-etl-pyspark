"""
Downloads NYC TLC trip-data Parquet files for a given year from the TLC
CloudFront CDN and streams them to S3 bucket specified.

Requires AWS credentials to be setup before running the script

Usage
-----
    python s3_data_copy.py --year 2025 --bucket nyc-taxi-etl-spark-raw
    python s3_data_copy.py --year 2024 --bucket nyc-taxi-etl-spark-raw --s3-prefix raw

S3 layout produced
------------------
    s3://nyc-taxi-etl-spark-raw/raw/yellow/yellow_tripdata_YYYY-MM.parquet
    s3://nyc-taxi-etl-spark-raw/raw/green/green_tripdata_YYYY-MM.parquet
    s3://nyc-taxi-etl-spark-raw/raw/fhv/fhv_tripdata_YYYY-MM.parquet
    s3://nyc-taxi-etl-spark-raw/raw/fhvhv/fhvhv_tripdata_YYYY-MM.parquet

    these are the 4 types of datasets present in nyc tlc data
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import urllib.error
import urllib.request

import boto3
from botocore.exceptions import ClientError

# keep this a constant as we always use the official url for downloads
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# trip types are also fixed as per the data dictionary
ALL_TRIP_TYPES = ["yellow", "green", "fhv", "fhvhv"]

# boto3 multipart upload threshold — 100 MB chunks keep memory usage low
CHUNK_SIZE = 100 * 1024 * 1024  # 100 MB

# Logging
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
def _s3_full_path(s3_prefix: str, trip_type: str, filename: str) -> str:
    return f"{s3_prefix}/{trip_type}/{filename}"


def _file_exists_in_s3(s3_client, bucket: str, key: str) -> bool:
    """Return True if the object already exists in the bucket."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "404":
            return False
        raise


def _stream_url_to_s3(s3_client, bucket: str, url: str, key: str) -> None:
    """
    Stream *url* directly into S3 via a multipart upload.
    No data is written to local disk.
    """
    mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
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
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk,
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                part_number += 1

        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            MultipartUpload={"Parts": parts},
            UploadId=upload_id,
        )
    except Exception:
        s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------
def copy_year(year: int, bucket: str, s3_prefix: str) -> None:
    s3 = boto3.client("s3")

    log.info("Starting NYC TLC data copy  →  year=%d  bucket=s3://%s/%s/", year, bucket, s3_prefix)
    log.info("Trip types: %s", ", ".join(ALL_TRIP_TYPES))

    total = 0
    skipped = 0
    failed = 0

    for trip_type in ALL_TRIP_TYPES:
        for month in range(1, 13):
            filename = f"{trip_type}_tripdata_{year}-{month:02d}.parquet"
            url = f"{BASE_URL}/{filename}"
            key = _s3_full_path(s3_prefix, trip_type, filename)

            if _file_exists_in_s3(s3, bucket, key):
                log.warning("Already exists, skipping: s3://%s/%s", bucket, key)
                skipped += 1
                continue

            log.info("Copying  %s", url)
            log.info("      to  s3://%s/%s", bucket, key)

            try:
                _stream_url_to_s3(s3, bucket, url, key)
                log.info("Done: %s", filename)
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
        required=True,
        help="Year to ingest (e.g. 2025). Required.",
    )

    parser.add_argument(
        "--bucket",
        default=os.environ.get("TLC_RAW_BUCKET"),
        required=True,
        help="S3 bucket name. Defaults to TLC_RAW_BUCKET env var.",
    )

    parser.add_argument(
        "--s3-prefix",
        default=os.environ.get("TLC_S3_PREFIX", "raw"),
        required=True,
        help="Key prefix inside the bucket (default: 'raw'). Overrides TLC_S3_PREFIX env var.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    copy_year(year=args.year, bucket=args.bucket, s3_prefix=args.s3_prefix)


