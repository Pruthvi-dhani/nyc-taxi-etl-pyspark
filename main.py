"""
Local development runner.

Chains all three pipeline stages (ingest → transform → analytics) for a
single trip type and date range.  On EMR Serverless each stage is submitted
as a separate job via ``scripts/submit_job.sh``.

Usage::

    python main.py --trip-type yellow --start-year 2022 --end-year 2022 \\
                   --start-month 1 --end-month 3
"""

from __future__ import annotations

import argparse
import sys

from src import config
from src.utils.logger import get_logger

logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NYC Taxi ETL – local runner")
    parser.add_argument("--trip-type", choices=config.TRIP_TYPES, default="yellow")
    parser.add_argument("--start-year", type=int, default=config.DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=config.DEFAULT_END_YEAR)
    parser.add_argument("--start-month", type=int, default=config.DEFAULT_START_MONTH)
    parser.add_argument("--end-month", type=int, default=config.DEFAULT_END_MONTH)
    parser.add_argument("--mode", default="overwrite", choices=["append", "overwrite"])
    parser.add_argument(
        "--stage",
        choices=["all", "ingest", "transform", "analytics"],
        default="all",
        help="Which pipeline stage to run (default: all)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logger.info("=== NYC Taxi ETL Pipeline — Local Runner ===")
    logger.info("trip_type=%s | %d-%02d → %d-%02d | mode=%s | stage=%s",
                args.trip_type, args.start_year, args.start_month,
                args.end_year, args.end_month, args.mode, args.stage)

    if args.stage in ("all", "ingest"):
        from jobs.ingest_job import parse_args as ingest_args
        from jobs.ingest_job import run as ingest_run
        logger.info("--- Stage 1: Ingestion (Raw → Bronze) ---")
        ingest_run(ingest_args([
            "--trip-type", args.trip_type,
            "--start-year", str(args.start_year),
            "--end-year", str(args.end_year),
            "--start-month", str(args.start_month),
            "--end-month", str(args.end_month),
            "--mode", args.mode,
        ]))

    if args.stage in ("all", "transform"):
        from jobs.transform_job import parse_args as transform_args
        from jobs.transform_job import run as transform_run
        logger.info("--- Stage 2: Transformation (Bronze → Silver → Gold) ---")
        transform_run(transform_args([
            "--trip-type", args.trip_type,
            "--mode", args.mode,
        ]))

    if args.stage in ("all", "analytics"):
        from jobs.analytics_job import parse_args as analytics_args
        from jobs.analytics_job import run as analytics_run
        logger.info("--- Stage 3: Analytics ---")
        analytics_run(analytics_args([
            "--trip-type", args.trip_type,
            "--module", "all",
        ]))

    logger.info("=== Pipeline complete ===")


if __name__ == "__main__":
    main(sys.argv[1:])
