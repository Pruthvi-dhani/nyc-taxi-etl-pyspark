"""
Analytics Job — EMR Serverless entrypoint.

Reads Gold-layer tables and Silver data to produce all analytics outputs.

Usage (local)::

    python jobs/analytics_job.py --trip-type yellow

Usage (EMR Serverless) — configured via emr_serverless_job.json.
"""

from __future__ import annotations

import argparse
import sys

from src import config
from src.analytics import demand_analysis, fare_analysis, geo_analysis, time_series, trip_patterns
from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NYC Taxi – Analytics Job")
    parser.add_argument("--trip-type", choices=config.TRIP_TYPES, default="yellow")
    parser.add_argument(
        "--module",
        choices=["all", "demand", "fare", "trip_patterns", "geo", "time_series"],
        default="all",
        help="Which analytics module to run (default: all)",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> None:
    logger.info("Starting analytics job | trip_type=%s | module=%s", args.trip_type, args.module)

    spark = get_spark_session(app_name=f"nyc-taxi-analytics-{args.trip_type}")
    t = args.trip_type

    def run_demand():
        logger.info("Running demand analysis...")
        demand_analysis.hourly_demand_profile(spark, t)
        demand_analysis.daily_trip_trend(spark, t)
        demand_analysis.borough_demand_breakdown(spark, t)
        demand_analysis.mom_trip_volume(spark, t)

    def run_fare():
        logger.info("Running fare analysis...")
        fare_analysis.fare_by_distance_bucket(spark, t)
        fare_analysis.tip_percentage_distribution(spark, t)
        fare_analysis.payment_type_trend(spark, t)
        fare_analysis.top_revenue_zones(spark, t)
        fare_analysis.fare_anomaly_detection(spark, t)

    def run_trip_patterns():
        logger.info("Running trip patterns analysis...")
        trip_patterns.top_corridors(spark, t)
        trip_patterns.airport_trip_share(spark, t)
        trip_patterns.distance_distribution_by_borough(spark, t)
        trip_patterns.busiest_pickup_zones(spark, t)
        trip_patterns.hourly_pattern_by_day_type(spark, t)

    def run_geo():
        logger.info("Running geo analysis...")
        geo_analysis.zone_trip_volume(spark, t)
        geo_analysis.zone_revenue_per_mile(spark, t)
        geo_analysis.inter_borough_flow(spark, t)
        geo_analysis.zone_net_flow(spark, t)

    def run_time_series():
        logger.info("Running time-series analysis...")
        time_series.monthly_volume_trend(spark, t)
        time_series.covid_impact_recovery(spark, t)
        time_series.seasonal_decomposition_proxy(spark, t)
        time_series.fare_evolution_yoy(spark, t)

    module_map = {
        "demand": run_demand,
        "fare": run_fare,
        "trip_patterns": run_trip_patterns,
        "geo": run_geo,
        "time_series": run_time_series,
    }

    if args.module == "all":
        for fn in module_map.values():
            fn()
    else:
        module_map[args.module]()

    logger.info("Analytics job completed successfully.")
    spark.stop()


if __name__ == "__main__":
    run(parse_args(sys.argv[1:]))

