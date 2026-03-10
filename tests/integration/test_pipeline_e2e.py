"""
Integration test — runs the full Bronze → Silver → Gold pipeline locally
using the in-memory sample fixtures.  Does not write to S3.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.transformation.bronze import transform_to_bronze
from src.transformation.gold import transform_to_gold
from src.transformation.silver import _drop_nulls, _enrich_datetime, _filter_outliers


class TestEndToEndPipeline:
    def test_yellow_pipeline(self, spark: SparkSession, yellow_raw_df):
        """Full pipeline should produce non-empty Gold tables for Yellow trips."""
        # Bronze
        bronze = transform_to_bronze(yellow_raw_df, trip_type="yellow")
        assert bronze.count() == yellow_raw_df.count(), "Bronze must not drop rows"

        # Silver
        silver = _drop_nulls(bronze, "yellow")
        silver = _filter_outliers(silver, "yellow")
        silver = _enrich_datetime(silver)
        silver = silver.withColumn(
            "trip_duration_mins",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,
        )
        silver = silver.withColumn("pickup_borough", F.lit("Manhattan"))
        silver = silver.withColumn("dropoff_borough", F.lit("Brooklyn"))
        silver = silver.withColumn("pickup_zone", F.lit("Zone A"))
        silver = silver.withColumn("dropoff_zone", F.lit("Zone B"))
        silver = silver.withColumn("is_airport_trip", F.lit(False))
        silver = silver.withColumn("pickup_date", F.to_date("pickup_datetime_local"))

        assert silver.count() > 0, "Silver must retain at least some rows after filtering"

        # Gold
        gold_tables = transform_to_gold(silver, trip_type="yellow")
        assert len(gold_tables) == 5
        for name, df in gold_tables.items():
            assert df.count() > 0, f"Gold table '{name}' must not be empty"

    def test_green_pipeline(self, spark: SparkSession, green_raw_df):
        """Full pipeline should work for Green trips too."""
        bronze = transform_to_bronze(green_raw_df, trip_type="green")
        silver = _drop_nulls(bronze, "green")
        silver = _filter_outliers(silver, "green")
        silver = _enrich_datetime(silver)
        silver = silver.withColumn(
            "trip_duration_mins",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,
        )
        silver = silver.withColumn("pickup_borough", F.lit("Queens"))
        silver = silver.withColumn("dropoff_borough", F.lit("Manhattan"))
        silver = silver.withColumn("pickup_zone", F.lit("LGA Airport"))
        silver = silver.withColumn("dropoff_zone", F.lit("Midtown"))
        silver = silver.withColumn("is_airport_trip", F.lit(True))
        silver = silver.withColumn("pickup_date", F.to_date("pickup_datetime_local"))

        gold_tables = transform_to_gold(silver, trip_type="green")
        assert "fare_summary" in gold_tables

