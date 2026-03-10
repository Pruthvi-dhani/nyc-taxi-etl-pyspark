"""
Time-Series Analysis module.

Answers
-------
* What is the long-term trend in monthly trip volumes (2019–2024)?
* What was the impact of COVID-19 on trip volumes and when did recovery occur?
* Are there seasonal patterns in trip demand?
* How have average fares evolved year-over-year?

Inputs:  Gold table ``trips_by_date``
Outputs: Parquet summaries to ``s3://<bucket>/analytics/time_series/``
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src import config
from src.utils.logger import get_logger

logger = get_logger(__name__)

_OUTPUT_PREFIX = f"{config.ANALYTICS_PREFIX}/time_series"


def _read_gold_dates(spark: SparkSession, trip_type: str) -> DataFrame:
    return spark.read.parquet(f"{config.GOLD_PREFIX}/trips_by_date/{trip_type}")


def monthly_volume_trend(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Monthly aggregated trip count and revenue with YoY comparison.

    Columns: ``year``, ``month``, ``monthly_trips``, ``monthly_revenue``,
    ``yoy_trips_change_pct``, ``yoy_revenue_change_pct``.
    """
    df = _read_gold_dates(spark, trip_type)

    monthly = (
        df.groupBy("pickup_year", "pickup_month")
        .agg(
            F.sum("trip_count").alias("monthly_trips"),
            F.sum("total_revenue").alias("monthly_revenue"),
            F.avg("avg_fare").alias("avg_fare"),
        )
        .orderBy("pickup_year", "pickup_month")
    )

    # Year-over-year comparison (lag 12 months)
    yoy_w = Window.orderBy("pickup_year", "pickup_month")
    result = (
        monthly
        .withColumn("prev_year_trips", F.lag("monthly_trips", 12).over(yoy_w))
        .withColumn("prev_year_revenue", F.lag("monthly_revenue", 12).over(yoy_w))
        .withColumn(
            "yoy_trips_change_pct",
            F.round(
                (F.col("monthly_trips") - F.col("prev_year_trips")) / F.col("prev_year_trips") * 100,
                2,
            ),
        )
        .withColumn(
            "yoy_revenue_change_pct",
            F.round(
                (F.col("monthly_revenue") - F.col("prev_year_revenue")) / F.col("prev_year_revenue") * 100,
                2,
            ),
        )
    )

    _write(result, "monthly_volume_trend")
    return result


def covid_impact_recovery(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Highlights the COVID-19 demand dip (Mar–May 2020) and subsequent recovery.

    Uses Jan–Feb 2020 as a baseline and computes trip volume as a
    percentage of that baseline for every subsequent month.
    """
    df = _read_gold_dates(spark, trip_type)

    monthly = (
        df.groupBy("pickup_year", "pickup_month")
        .agg(F.sum("trip_count").alias("monthly_trips"))
        .orderBy("pickup_year", "pickup_month")
    )

    # Baseline: average of Jan & Feb 2020
    baseline_df = monthly.filter(
        (F.col("pickup_year") == 2020) & (F.col("pickup_month").isin(1, 2))
    )
    baseline_row = baseline_df.agg(F.avg("monthly_trips").alias("baseline")).collect()
    baseline = baseline_row[0]["baseline"] if baseline_row else None

    if baseline:
        result = monthly.withColumn(
            "pct_of_baseline",
            F.round(F.col("monthly_trips") / baseline * 100, 2),
        ).withColumn(
            "covid_period",
            F.when(
                (F.col("pickup_year") == 2020) & (F.col("pickup_month").between(3, 5)),
                "Impact",
            )
            .when(
                (F.col("pickup_year") == 2020) & (F.col("pickup_month").between(6, 12)),
                "Early recovery",
            )
            .when(F.col("pickup_year") == 2021, "Recovery")
            .when(F.col("pickup_year") >= 2022, "Post-COVID")
            .otherwise("Pre-COVID"),
        )
    else:
        result = monthly.withColumn("pct_of_baseline", F.lit(None)).withColumn("covid_period", F.lit(None))

    _write(result, "covid_impact_recovery")
    return result


def seasonal_decomposition_proxy(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Compute a proxy seasonal index by calculating each month's average
    trip count relative to the overall monthly average.

    A seasonal index > 1 indicates above-average demand for that month.
    """
    df = _read_gold_dates(spark, trip_type)

    monthly = (
        df.groupBy("pickup_year", "pickup_month")
        .agg(F.sum("trip_count").alias("monthly_trips"))
    )

    # Overall average per month-of-year across all years
    month_avg = (
        monthly.groupBy("pickup_month")
        .agg(F.avg("monthly_trips").alias("avg_monthly_trips"))
    )

    overall_avg = monthly.agg(F.avg("monthly_trips").alias("grand_avg")).collect()[0]["grand_avg"]

    result = month_avg.withColumn(
        "seasonal_index",
        F.round(F.col("avg_monthly_trips") / overall_avg, 4),
    ).orderBy("pickup_month")

    _write(result, "seasonal_index")
    return result


def fare_evolution_yoy(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Year-over-year average fare and total revenue trend.
    """
    df = _read_gold_dates(spark, trip_type)

    result = (
        df.groupBy("pickup_year")
        .agg(
            F.sum("trip_count").alias("annual_trips"),
            F.sum("total_revenue").alias("annual_revenue"),
            F.avg("avg_fare").alias("avg_fare"),
            F.avg("avg_distance").alias("avg_distance"),
        )
        .orderBy("pickup_year")
    )

    yoy_w = Window.orderBy("pickup_year")
    result = result.withColumn(
        "yoy_fare_change_pct",
        F.round(
            (F.col("avg_fare") - F.lag("avg_fare", 1).over(yoy_w))
            / F.lag("avg_fare", 1).over(yoy_w)
            * 100,
            2,
        ),
    )

    _write(result, "fare_evolution_yoy")
    return result


def _write(df: DataFrame, name: str) -> None:
    path = f"{_OUTPUT_PREFIX}/{name}"
    logger.info("Writing time_series analysis '%s' to %s", name, path)
    df.coalesce(1).write.mode("overwrite").parquet(path)

