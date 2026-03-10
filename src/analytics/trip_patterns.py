"""
Trip Patterns module.

Answers
-------
* What are the most popular pickup → dropoff corridors?
* What share of trips are airport trips?
* How do trip distances differ between boroughs?
* What are the busiest pickup zones?

Inputs:  Silver data
Outputs: Parquet summaries to ``s3://<bucket>/analytics/trip_patterns/``
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src.utils.logger import get_logger

logger = get_logger(__name__)

_OUTPUT_PREFIX = f"{config.ANALYTICS_PREFIX}/trip_patterns"


def _read_silver(spark: SparkSession, trip_type: str) -> DataFrame:
    return spark.read.parquet(f"{config.SILVER_PREFIX}/{trip_type}")


def top_corridors(
    spark: SparkSession,
    trip_type: str = "yellow",
    top_n: int = 25,
) -> DataFrame:
    """
    Most popular pickup → dropoff zone pairs ranked by trip count.
    """
    df = _read_silver(spark, trip_type)

    result = (
        df.groupBy("pickup_zone", "dropoff_zone", "pickup_borough", "dropoff_borough")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare"),
        )
        .orderBy("trip_count", ascending=False)
        .limit(top_n)
    )

    _write(result, f"top_{top_n}_corridors")
    return result


def airport_trip_share(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Monthly share of airport trips vs. non-airport trips.
    """
    df = _read_silver(spark, trip_type)

    result = (
        df.groupBy("pickup_year", "pickup_month", "is_airport_trip")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
        )
        .orderBy("pickup_year", "pickup_month", "is_airport_trip")
    )

    _write(result, "airport_trip_share")
    return result


def distance_distribution_by_borough(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Trip distance percentiles broken down by pickup borough.
    """
    df = _read_silver(spark, trip_type)

    result = (
        df.groupBy("pickup_borough")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_distance"),
            F.percentile_approx("trip_distance", 0.25).alias("p25_distance"),
            F.percentile_approx("trip_distance", 0.50).alias("p50_distance"),
            F.percentile_approx("trip_distance", 0.75).alias("p75_distance"),
            F.percentile_approx("trip_distance", 0.95).alias("p95_distance"),
        )
        .orderBy("avg_distance", ascending=False)
    )

    _write(result, "distance_distribution_by_borough")
    return result


def busiest_pickup_zones(
    spark: SparkSession,
    trip_type: str = "yellow",
    top_n: int = 30,
) -> DataFrame:
    """
    Top N pickup zones by total trip count.
    """
    df = _read_silver(spark, trip_type)

    result = (
        df.groupBy("pickup_zone", "pickup_borough")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
        )
        .orderBy("trip_count", ascending=False)
        .limit(top_n)
    )

    _write(result, f"top_{top_n}_pickup_zones")
    return result


def hourly_pattern_by_day_type(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Average trip count by hour, split between weekday and weekend.
    """
    df = _read_silver(spark, trip_type)

    result = (
        df.groupBy("pickup_hour", "is_weekend")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare"),
        )
        .orderBy("is_weekend", "pickup_hour")
    )

    _write(result, "hourly_pattern_weekday_vs_weekend")
    return result


def _write(df: DataFrame, name: str) -> None:
    path = f"{_OUTPUT_PREFIX}/{name}"
    logger.info("Writing trip_patterns '%s' to %s", name, path)
    df.coalesce(1).write.mode("overwrite").parquet(path)

