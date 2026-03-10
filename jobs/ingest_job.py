"""
Ingestion Job — EMR Serverless entrypoint.

Reads raw TLC Parquet files from S3, applies Bronze-layer transformations,
and writes partitioned output to the Bronze S3 prefix.

Usage (local)::

    python jobs/ingest_job.py --trip-type yellow --start-year 2022 --end-year 2023

Usage (EMR Serverless) — configured via emr_serverless_job.json.
"""

from __future__ import annotations

import argparse
import sys

from src import config
from src.ingestion.reader import read_trip_data
from src.transformation.bronze import transform_to_bronze, write_bronze
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NYC Taxi – Ingestion Job (Raw → Bronze)")
    parser.add_argument(
        "--trip-type",
        choices=config.TRIP_TYPES,
        default="yellow",
        help="TLC trip type to ingest (default: yellow)",
    )
    parser.add_argument("--start-year", type=int, default=config.DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=config.DEFAULT_END_YEAR)
    parser.add_argument("--start-month", type=int, default=config.DEFAULT_START_MONTH)
    parser.add_argument("--end-month", type=int, default=config.DEFAULT_END_MONTH)
    parser.add_argument("--mode", default="append", choices=["append", "overwrite"])
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> None:
    logger.info(
        "Starting ingestion job | trip_type=%s | %d-%02d → %d-%02d",
        args.trip_type,
        args.start_year,
        args.start_month,
        args.end_year,
        args.end_month,
    )

    spark = get_spark_session(app_name=f"nyc-taxi-ingest-{args.trip_type}")

    # 1. Read raw data from TLC public S3
    raw_df = read_trip_data(
        spark,
        trip_type=args.trip_type,
        start_year=args.start_year,
        end_year=args.end_year,
        start_month=args.start_month,
        end_month=args.end_month,
    )

    # 2. Apply Bronze transformations
    bronze_df = transform_to_bronze(raw_df, trip_type=args.trip_type)

    # 3. Write to Bronze S3 prefix
    write_bronze(bronze_df, trip_type=args.trip_type, mode=args.mode)

    logger.info("Ingestion job completed successfully for trip_type='%s'", args.trip_type)
    spark.stop()


if __name__ == "__main__":
    run(parse_args(sys.argv[1:]))

