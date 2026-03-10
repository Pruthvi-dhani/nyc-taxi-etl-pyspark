"""
Geo Analysis module.

Answers
-------
* Which taxi zones have the highest trip volume? (choropleth-ready)
* What is revenue-per-zone?
* What is the average fare per mile by zone?
* Which zones are net exporters vs. importers of trips?

Inputs:  Silver data (zone names and boroughs already joined in)
Outputs: Parquet summaries to ``s3://<bucket>/analytics/geo/``

Note: The outputs are structured so they can be directly joined with the
TLC taxi zone shapefile (GeoJSON available at
https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc)
for choropleth visualisation in tools like Kepler.gl or Plotly.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src.utils.logger import get_logger

logger = get_logger(__name__)

_OUTPUT_PREFIX = f"{config.ANALYTICS_PREFIX}/geo"


def _read_silver(spark: SparkSession, trip_type: str) -> DataFrame:
    return spark.read.parquet(f"{config.SILVER_PREFIX}/{trip_type}")


def zone_trip_volume(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Total pickups and dropoffs per zone — the primary choropleth input.
    """
    df = _read_silver(spark, trip_type)

    pickups = df.groupBy("pu_location_id", "pickup_zone", "pickup_borough").agg(
        F.count("*").alias("pickup_count"),
        F.sum("total_amount").alias("pickup_revenue"),
    )

    dropoffs = df.groupBy("do_location_id", "dropoff_zone", "dropoff_borough").agg(
        F.count("*").alias("dropoff_count"),
    ).withColumnRenamed("do_location_id", "pu_location_id") \
     .withColumnRenamed("dropoff_zone", "pickup_zone") \
     .withColumnRenamed("dropoff_borough", "pickup_borough")

    result = pickups.join(dropoffs, on=["pu_location_id", "pickup_zone", "pickup_borough"], how="outer") \
        .withColumnRenamed("pu_location_id", "location_id") \
        .withColumnRenamed("pickup_zone", "zone") \
        .withColumnRenamed("pickup_borough", "borough") \
        .withColumn("net_flow", F.col("pickup_count") - F.col("dropoff_count")) \
        .orderBy("pickup_count", ascending=False)

    _write(result, "zone_trip_volume")
    return result


def zone_revenue_per_mile(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Average fare per mile by pickup zone — a proxy for pricing density.
    """
    df = _read_silver(spark, trip_type)

    result = (
        df.filter(F.col("trip_distance") > 0)
        .withColumn("fare_per_mile", F.col("fare_amount") / F.col("trip_distance"))
        .groupBy("pu_location_id", "pickup_zone", "pickup_borough")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_per_mile").alias("avg_fare_per_mile"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
        )
        .orderBy("avg_fare_per_mile", ascending=False)
    )

    _write(result, "zone_revenue_per_mile")
    return result


def inter_borough_flow(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Origin-destination matrix at the borough level.
    Useful for understanding cross-borough trip patterns.
    """
    df = _read_silver(spark, trip_type)

    result = (
        df.groupBy("pickup_borough", "dropoff_borough")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare"),
        )
        .orderBy("trip_count", ascending=False)
    )

    _write(result, "inter_borough_flow")
    return result


def zone_net_flow(spark: SparkSession, trip_type: str = "yellow") -> DataFrame:
    """
    Net flow (pickups - dropoffs) per zone.  Negative values indicate
    zones that receive more trips than they originate (e.g. airports,
    business districts in the morning).
    """
    df = _read_silver(spark, trip_type)

    pickups = df.groupBy("pu_location_id").agg(F.count("*").alias("pickups"))
    dropoffs = df.groupBy("do_location_id").agg(F.count("*").alias("dropoffs"))
    zones = df.select("pu_location_id", "pickup_zone", "pickup_borough").distinct()

    result = (
        zones.join(pickups, on="pu_location_id", how="left")
        .join(dropoffs.withColumnRenamed("do_location_id", "pu_location_id"), on="pu_location_id", how="left")
        .fillna(0, subset=["pickups", "dropoffs"])
        .withColumn("net_flow", F.col("pickups") - F.col("dropoffs"))
        .withColumnRenamed("pu_location_id", "location_id")
        .withColumnRenamed("pickup_zone", "zone")
        .withColumnRenamed("pickup_borough", "borough")
        .orderBy("net_flow", ascending=False)
    )

    _write(result, "zone_net_flow")
    return result


def _write(df: DataFrame, name: str) -> None:
    path = f"{_OUTPUT_PREFIX}/{name}"
    logger.info("Writing geo analysis '%s' to %s", name, path)
    df.coalesce(1).write.mode("overwrite").parquet(path)

