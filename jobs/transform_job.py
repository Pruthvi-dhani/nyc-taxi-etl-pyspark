"""
Transformation Job — EMR Serverless entrypoint.

Reads Bronze-layer data, applies Silver-layer cleaning/enrichment,
then builds Gold-layer KPI aggregation tables.

Usage (local)::

    python jobs/transform_job.py --trip-type yellow

Usage (EMR Serverless) — configured via emr_serverless_job.json.
"""

from __future__ import annotations

import argparse
import sys

from src import config
from src.transformation.gold import transform_to_gold, write_gold
from src.transformation.silver import transform_to_silver, write_silver
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NYC Taxi – Transformation Job (Bronze → Silver → Gold)")
    parser.add_argument("--trip-type", choices=config.TRIP_TYPES, default="yellow")
    parser.add_argument("--mode", default="append", choices=["append", "overwrite"])
    parser.add_argument(
        "--skip-gold",
        action="store_true",
        help="Write Silver only, skip Gold aggregations",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> None:
    logger.info("Starting transformation job | trip_type=%s", args.trip_type)

    spark = get_spark_session(app_name=f"nyc-taxi-transform-{args.trip_type}")

    # 1. Read Bronze
    bronze_path = f"{config.BRONZE_PREFIX}/{args.trip_type}"
    logger.info("Reading Bronze data from %s", bronze_path)
    bronze_df = spark.read.parquet(bronze_path)

    # 2. Bronze → Silver
    silver_df = transform_to_silver(bronze_df, spark=spark, trip_type=args.trip_type)
    write_silver(silver_df, trip_type=args.trip_type, mode=args.mode)

    # 3. Silver → Gold
    if not args.skip_gold:
        silver_df = spark.read.parquet(f"{config.SILVER_PREFIX}/{args.trip_type}")
        gold_tables = transform_to_gold(silver_df, trip_type=args.trip_type)
        write_gold(gold_tables, trip_type=args.trip_type, mode=args.mode)

    logger.info("Transformation job completed successfully for trip_type='%s'", args.trip_type)
    spark.stop()


if __name__ == "__main__":
    run(parse_args(sys.argv[1:]))

