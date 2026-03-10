"""
Gold layer transformation.

Produces pre-aggregated KPI tables consumed by the analytics modules.
Each table is written as partitioned Parquet to the Gold S3 prefix.

Tables produced
---------------
* ``trips_by_hour``          – trip count & avg fare per hour-of-day
* ``trips_by_date``          – daily trip count, revenue, avg distance
* ``trips_by_borough``       – pickup borough breakdown
* ``fare_summary``           – fare statistics by payment type
* ``trip_duration_summary``  – duration percentiles by trip type
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import config
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Individual aggregation helpers
# ---------------------------------------------------------------------------

def build_trips_by_hour(df: DataFrame) -> DataFrame:
    """Aggregate trip count and avg fare by hour-of-day."""
    return (
        df.groupBy("pickup_year", "pickup_month", "pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.sum("total_amount").alias("total_revenue"),
        )
        .orderBy("pickup_year", "pickup_month", "pickup_hour")
    )


def build_trips_by_date(df: DataFrame) -> DataFrame:
    """Aggregate daily trip count, revenue, and distance."""
    return (
        df.withColumn("pickup_date", F.to_date("pickup_datetime_local"))
        .groupBy("pickup_date", "pickup_year", "pickup_month")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_mins").alias("avg_duration_mins"),
        )
        .orderBy("pickup_date")
    )


def build_trips_by_borough(df: DataFrame) -> DataFrame:
    """Aggregate trip count and revenue by pickup borough."""
    return (
        df.groupBy("pickup_year", "pickup_month", "pickup_borough")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
        )
        .orderBy("pickup_year", "pickup_month", "trip_count", ascending=[True, True, False])
    )


def build_fare_summary(df: DataFrame) -> DataFrame:
    """Fare statistics broken down by payment type."""
    payment_labels = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided"}
    payment_map_expr = F.create_map(
        *[item for kv in payment_labels.items() for item in (F.lit(kv[0]), F.lit(kv[1]))]
    )
    return (
        df.withColumn("payment_label", payment_map_expr[F.col("payment_type").cast("int")])
        .groupBy("pickup_year", "pickup_month", "payment_label")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("tip_amount").alias("avg_tip"),
            F.expr("avg(tip_amount / nullif(fare_amount, 0)) * 100").alias("avg_tip_pct"),
            F.percentile_approx("fare_amount", 0.5).alias("median_fare"),
            F.percentile_approx("fare_amount", 0.95).alias("p95_fare"),
        )
    )


def build_trip_duration_summary(df: DataFrame) -> DataFrame:
    """Trip duration percentile summary."""
    return (
        df.groupBy("pickup_year", "pickup_month")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_duration_mins").alias("avg_duration_mins"),
            F.percentile_approx("trip_duration_mins", 0.25).alias("p25_duration"),
            F.percentile_approx("trip_duration_mins", 0.50).alias("p50_duration"),
            F.percentile_approx("trip_duration_mins", 0.75).alias("p75_duration"),
            F.percentile_approx("trip_duration_mins", 0.95).alias("p95_duration"),
        )
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def transform_to_gold(df: DataFrame, trip_type: str) -> dict[str, DataFrame]:
    """
    Build all Gold-layer aggregation tables from a Silver DataFrame.

    Parameters
    ----------
    df:
        Silver DataFrame for *trip_type*.
    trip_type:
        Used for logging only.

    Returns
    -------
    dict[str, DataFrame]
        Mapping of table name → aggregated DataFrame.
    """
    logger.info("Building Gold aggregations for trip_type='%s'", trip_type)

    tables: dict[str, DataFrame] = {
        "trips_by_hour": build_trips_by_hour(df),
        "trips_by_date": build_trips_by_date(df),
        "trips_by_borough": build_trips_by_borough(df),
        "trip_duration_summary": build_trip_duration_summary(df),
    }

    # Fare & tip stats only available for yellow / green
    if trip_type in ("yellow", "green"):
        tables["fare_summary"] = build_fare_summary(df)

    return tables


def write_gold(tables: dict[str, DataFrame], trip_type: str, mode: str = "append") -> None:
    """
    Write each Gold table to its own S3 prefix partitioned by year/month.

    Parameters
    ----------
    tables:
        Output of :func:`transform_to_gold`.
    trip_type:
        Used as part of the S3 path.
    mode:
        Spark write mode.
    """
    for table_name, df in tables.items():
        output_path = f"{config.GOLD_PREFIX}/{table_name}/{trip_type}"
        logger.info("Writing Gold table '%s' to %s", table_name, output_path)

        partition_cols = []
        if "pickup_year" in df.columns:
            partition_cols.append("pickup_year")
        if "pickup_month" in df.columns:
            partition_cols.append("pickup_month")

        writer = df.write.format("parquet").mode(mode)
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(output_path)

    logger.info("Gold write complete for trip_type='%s'", trip_type)

