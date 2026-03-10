"""
Fare Analysis module.

Answers
-------
* How does average fare vary by trip distance bucket?
* What is the distribution of tip percentages?
* How does payment type split change over time?
* Which zones generate the highest revenue per trip?
* Are there anomalous fares (IQR-based outlier detection)?

Inputs:  Gold tables ``fare_summary``, Silver data (for zone-level)
Outputs: Parquet summaries to ``s3://<bucket>/analytics/fare/``
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src.utils.logger import get_logger

logger = get_logger(__name__)

_OUTPUT_PREFIX = f"{config.ANALYTICS_PREFIX}/fare"


def _read_silver(spark: SparkSession, trip_type: str) -> DataFrame:
    path = f"{config.SILVER_PREFIX}/{trip_type}"
    return spark.read.parquet(path)


def _read_gold(spark: SparkSession, table: str, trip_type: str) -> DataFrame:
    return spark.read.parquet(f"{config.GOLD_PREFIX}/{table}/{trip_type}")


def fare_by_distance_bucket(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Average fare grouped by trip distance bucket (0-2, 2-5, 5-10, 10-20, 20+ miles).
    """
    df = _read_silver(spark, trip_type)

    bucketised = df.withColumn(
        "distance_bucket",
        F.when(F.col("trip_distance") < 2, "0-2 mi")
        .when(F.col("trip_distance") < 5, "2-5 mi")
        .when(F.col("trip_distance") < 10, "5-10 mi")
        .when(F.col("trip_distance") < 20, "10-20 mi")
        .otherwise("20+ mi"),
    )

    result = bucketised.groupBy("distance_bucket").agg(
        F.count("*").alias("trip_count"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_amount").alias("avg_tip"),
        F.avg("total_amount").alias("avg_total"),
        F.avg("trip_duration_mins").alias("avg_duration_mins"),
    ).orderBy("distance_bucket")

    _write(result, "fare_by_distance_bucket")
    return result


def tip_percentage_distribution(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Distribution of tip percentage (tip / fare) in 5-point buckets.
    Only credit-card trips (payment_type=1) are included.
    """
    df = _read_silver(spark, trip_type).filter(F.col("payment_type") == 1)

    df = df.withColumn(
        "tip_pct",
        F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount") * 100).otherwise(None),
    )

    bucketised = df.withColumn(
        "tip_pct_bucket",
        F.when(F.col("tip_pct").isNull(), "no_tip")
        .when(F.col("tip_pct") == 0, "0%")
        .when(F.col("tip_pct") < 10, "1-10%")
        .when(F.col("tip_pct") < 15, "10-15%")
        .when(F.col("tip_pct") < 20, "15-20%")
        .when(F.col("tip_pct") < 25, "20-25%")
        .otherwise("25%+"),
    )

    result = bucketised.groupBy("tip_pct_bucket").agg(
        F.count("*").alias("trip_count"),
        F.avg("tip_pct").alias("avg_tip_pct"),
    ).orderBy("avg_tip_pct")

    _write(result, "tip_pct_distribution")
    return result


def payment_type_trend(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Monthly payment type split (cash vs. card vs. other).
    """
    df = _read_gold(spark, "fare_summary", trip_type)
    result = (
        df.groupBy("pickup_year", "pickup_month", "payment_label")
        .agg(F.sum("trip_count").alias("trip_count"))
        .orderBy("pickup_year", "pickup_month", "payment_label")
    )
    _write(result, "payment_type_trend")
    return result


def top_revenue_zones(spark: SparkSession, trip_type: str = "yellow", top_n: int = 20) -> DataFrame:
    """
    Top N pickup zones by total revenue.
    """
    df = _read_silver(spark, trip_type)
    result = (
        df.groupBy("pickup_zone", "pickup_borough")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("tip_amount").alias("avg_tip"),
        )
        .orderBy("total_revenue", ascending=False)
        .limit(top_n)
    )
    _write(result, f"top_{top_n}_revenue_zones")
    return result


def fare_anomaly_detection(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    IQR-based fare anomaly flagging.

    Trips with fare_amount outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR] are
    flagged as anomalous.  Returns a summary of anomaly counts by month.
    """
    df = _read_silver(spark, trip_type)

    q1, q3 = df.approxQuantile("fare_amount", [0.25, 0.75], 0.01)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    logger.info("Fare IQR bounds: lower=%.2f, upper=%.2f", lower_bound, upper_bound)

    flagged = df.withColumn(
        "is_fare_anomaly",
        (F.col("fare_amount") < lower_bound) | (F.col("fare_amount") > upper_bound),
    )

    result = (
        flagged.groupBy("pickup_year", "pickup_month")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum(F.col("is_fare_anomaly").cast("int")).alias("anomaly_count"),
        )
        .withColumn(
            "anomaly_pct",
            F.round(F.col("anomaly_count") / F.col("total_trips") * 100, 2),
        )
        .orderBy("pickup_year", "pickup_month")
    )

    _write(result, "fare_anomaly_by_month")
    return result


def _write(df: DataFrame, name: str) -> None:
    path = f"{_OUTPUT_PREFIX}/{name}"
    logger.info("Writing fare analysis '%s' to %s", name, path)
    df.coalesce(1).write.mode("overwrite").parquet(path)

