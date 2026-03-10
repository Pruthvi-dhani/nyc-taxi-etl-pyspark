"""
Silver layer transformation.

Responsibilities
----------------
* Drop rows with null primary keys (pickup/dropoff datetime, location IDs).
* Filter out physically impossible values (negative fares, zero distances,
  trips > 200 miles, fares > $500, durations < 1 min or > 12 hrs).
* Enrich with derived datetime features (hour, day_of_week, is_weekend).
* Join with taxi zone lookup to resolve zone names and boroughs.
* Convert pickup_datetime to US/Eastern timezone.
* Write partitioned Parquet to the Silver S3 prefix.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src import config
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Quality thresholds
# ---------------------------------------------------------------------------
MIN_TRIP_DISTANCE: float = 0.01       # miles
MAX_TRIP_DISTANCE: float = 200.0      # miles
MIN_FARE_AMOUNT: float = 0.0
MAX_FARE_AMOUNT: float = 500.0
MIN_TRIP_DURATION_SECS: int = 60      # 1 minute
MAX_TRIP_DURATION_SECS: int = 43200   # 12 hours


def _load_zone_lookup(spark: SparkSession) -> DataFrame:
    """Load the TLC taxi zone lookup CSV from S3."""
    logger.info("Loading taxi zone lookup from %s", config.TAXI_ZONE_LOOKUP_PATH)
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(config.TAXI_ZONE_LOOKUP_PATH)
        .select(
            F.col("LocationID").cast(IntegerType()).alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone_name"),
            F.col("service_zone"),
        )
    )


def _drop_nulls(df: DataFrame, trip_type: str) -> DataFrame:
    """Drop rows missing critical columns."""
    required = ["pickup_datetime", "dropoff_datetime"]
    if trip_type in ("yellow", "green"):
        required += ["pu_location_id", "do_location_id", "fare_amount", "trip_distance"]
    return df.dropna(subset=required)


def _filter_outliers(df: DataFrame, trip_type: str) -> DataFrame:
    """Remove rows with physically impossible or extreme values."""
    if trip_type == "fhv":
        return df  # FHV data has no fare / distance columns

    df = df.withColumn("_duration_secs", F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"))

    df = df.filter(
        (F.col("trip_distance").between(MIN_TRIP_DISTANCE, MAX_TRIP_DISTANCE))
        & (F.col("fare_amount").between(MIN_FARE_AMOUNT, MAX_FARE_AMOUNT))
        & (F.col("_duration_secs").between(MIN_TRIP_DURATION_SECS, MAX_TRIP_DURATION_SECS))
        & (F.col("passenger_count") > 0)
    ).drop("_duration_secs")

    return df


def _enrich_datetime(df: DataFrame) -> DataFrame:
    """Add derived datetime feature columns."""
    return (
        df
        .withColumn(
            "pickup_datetime_local",
            F.from_utc_timestamp("pickup_datetime", "America/New_York"),
        )
        .withColumn("pickup_hour", F.hour("pickup_datetime_local"))
        .withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime_local"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("pickup_datetime_local").isin(1, 7), True).otherwise(False),
        )
        .withColumn("pickup_month", F.month("pickup_datetime_local"))
        .withColumn("pickup_year", F.year("pickup_datetime_local"))
    )


def _join_zones(df: DataFrame, zone_df: DataFrame, trip_type: str) -> DataFrame:
    """Enrich pickup and dropoff locations with borough and zone name."""
    df = df.join(
        zone_df.select(
            F.col("location_id").alias("pu_location_id"),
            F.col("borough").alias("pickup_borough"),
            F.col("zone_name").alias("pickup_zone"),
        ),
        on="pu_location_id",
        how="left",
    )

    # Dropoff zone
    df = df.join(
        zone_df.select(
            F.col("location_id").alias("do_location_id"),
            F.col("borough").alias("dropoff_borough"),
            F.col("zone_name").alias("dropoff_zone"),
        ),
        on="do_location_id",
        how="left",
    )
    return df


def transform_to_silver(
    df: DataFrame,
    spark: SparkSession,
    trip_type: str,
) -> DataFrame:
    """
    Apply Silver-layer transformations.

    Parameters
    ----------
    df:
        Bronze DataFrame for *trip_type*.
    spark:
        Active SparkSession (needed to load zone lookup).
    trip_type:
        One of ``"yellow"``, ``"green"``, ``"fhv"``.

    Returns
    -------
    DataFrame
        Clean, enriched Silver DataFrame.
    """
    logger.info("Applying Silver transformations for trip_type='%s'", trip_type)
    df = _drop_nulls(df, trip_type)
    df = _filter_outliers(df, trip_type)
    df = _enrich_datetime(df)

    zone_df = _load_zone_lookup(spark)
    df = _join_zones(df, zone_df, trip_type)

    # Add trip duration in minutes
    if "dropoff_datetime" in df.columns and "pickup_datetime" in df.columns:
        df = df.withColumn(
            "trip_duration_mins",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,
        )

    # Flag airport trips (JFK=132, LGA=138, EWR=1)
    airport_ids = [1, 132, 138]
    df = df.withColumn(
        "is_airport_trip",
        F.col("pu_location_id").isin(airport_ids) | F.col("do_location_id").isin(airport_ids)
        if "pu_location_id" in df.columns else F.lit(False),
    )

    return df


def write_silver(
    df: DataFrame,
    trip_type: str,
    mode: str = "append",
) -> None:
    """Write Silver DataFrame to S3 as partitioned Parquet."""
    output_path = f"{config.SILVER_PREFIX}/{trip_type}"
    logger.info("Writing Silver data to %s (mode=%s)", output_path, mode)

    partition_cols = ["pickup_year", "pickup_month"] if "pickup_year" in df.columns else []

    writer = df.write.format("parquet").mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(output_path)
    logger.info("Silver write complete for trip_type='%s'", trip_type)



