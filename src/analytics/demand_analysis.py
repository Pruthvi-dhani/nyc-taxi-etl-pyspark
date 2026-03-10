"""
Demand Analysis module.

Answers
-------
* What are the peak hours of the day for taxi demand?
* Which days of the week see the highest ride volume?
* How does demand vary by borough?
* What is the month-over-month trend in trip volume?

Inputs:  Gold tables ``trips_by_hour``, ``trips_by_date``, ``trips_by_borough``
Outputs: Parquet + CSV summaries written to ``s3://<bucket>/analytics/demand/``
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src import config
from src.utils.logger import get_logger

logger = get_logger(__name__)

_OUTPUT_PREFIX = f"{config.ANALYTICS_PREFIX}/demand"


def _read_gold(spark: SparkSession, table: str, trip_type: str) -> DataFrame:
    path = f"{config.GOLD_PREFIX}/{table}/{trip_type}"
    logger.info("Reading Gold table from %s", path)
    return spark.read.parquet(path)


def hourly_demand_profile(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Compute average trips per hour-of-day across the entire dataset.

    Returns a 24-row DataFrame with columns:
    ``pickup_hour``, ``avg_trips``, ``pct_of_daily_total``.
    """
    df = _read_gold(spark, "trips_by_hour", trip_type)

    hourly = (
        df.groupBy("pickup_hour")
        .agg(F.avg("trip_count").alias("avg_trips"), F.avg("avg_fare").alias("avg_fare"))
        .orderBy("pickup_hour")
    )

    total_window = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    hourly = hourly.withColumn(
        "pct_of_daily_total",
        F.round(F.col("avg_trips") / F.sum("avg_trips").over(total_window) * 100, 2),
    )

    _write(hourly, "hourly_demand_profile")
    return hourly


def daily_trip_trend(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Daily trip counts with a 7-day rolling average.

    Returns columns: ``pickup_date``, ``trip_count``, ``rolling_7d_avg``.
    """
    df = _read_gold(spark, "trips_by_date", trip_type)

    w7 = (
        Window.orderBy(F.col("pickup_date").cast("timestamp").cast("long"))
        .rowsBetween(-6, 0)
    )

    result = df.select(
        "pickup_date", "trip_count", "total_revenue", "avg_fare"
    ).withColumn(
        "rolling_7d_avg", F.round(F.avg("trip_count").over(w7), 0)
    ).orderBy("pickup_date")

    _write(result, "daily_trip_trend")
    return result


def borough_demand_breakdown(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Trip volume and revenue share by pickup borough.
    """
    df = _read_gold(spark, "trips_by_borough", trip_type)

    borough_total = df.groupBy("pickup_borough").agg(
        F.sum("trip_count").alias("total_trips"),
        F.sum("total_revenue").alias("total_revenue"),
        F.avg("avg_fare").alias("avg_fare"),
    )

    total_trips = borough_total.agg(F.sum("total_trips")).collect()[0][0]
    result = borough_total.withColumn(
        "trip_share_pct", F.round(F.col("total_trips") / total_trips * 100, 2)
    ).orderBy("total_trips", ascending=False)

    _write(result, "borough_demand_breakdown")
    return result


def mom_trip_volume(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Month-over-month trip volume with percentage change.
    """
    df = _read_gold(spark, "trips_by_date", trip_type)

    monthly = df.groupBy("pickup_year", "pickup_month").agg(
        F.sum("trip_count").alias("monthly_trips"),
        F.sum("total_revenue").alias("monthly_revenue"),
    ).orderBy("pickup_year", "pickup_month")

    lag_w = Window.orderBy("pickup_year", "pickup_month")
    result = monthly.withColumn(
        "prev_month_trips", F.lag("monthly_trips", 1).over(lag_w)
    ).withColumn(
        "mom_change_pct",
        F.round(
            (F.col("monthly_trips") - F.col("prev_month_trips")) / F.col("prev_month_trips") * 100,
            2,
        ),
    )

    _write(result, "mom_trip_volume")
    return result


def _write(df: DataFrame, name: str) -> None:
    path = f"{_OUTPUT_PREFIX}/{name}"
    logger.info("Writing demand analysis '%s' to %s", name, path)
    df.coalesce(1).write.mode("overwrite").option("header", "true").parquet(path)

