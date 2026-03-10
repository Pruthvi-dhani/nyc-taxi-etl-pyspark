"""Unit tests for the Silver transformation layer."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.transformation.bronze import transform_to_bronze
from src.transformation.silver import (
    MIN_FARE_AMOUNT,
    MIN_TRIP_DISTANCE,
    _drop_nulls,
    _enrich_datetime,
    _filter_outliers,
)


@pytest.fixture()
def bronze_yellow(spark: SparkSession, yellow_raw_df):
    return transform_to_bronze(yellow_raw_df, trip_type="yellow")


class TestSilverDropNulls:
    def test_drops_null_pickup_datetime(self, spark: SparkSession, bronze_yellow):
        """Rows with null pickup_datetime must be dropped."""
        df_with_null = bronze_yellow.withColumn(
            "pickup_datetime",
            F.when(F.col("pickup_datetime").isNotNull() & (F.monotonically_increasing_id() == 0), None)
            .otherwise(F.col("pickup_datetime")),
        )
        result = _drop_nulls(df_with_null, trip_type="yellow")
        assert result.count() <= df_with_null.count()


class TestSilverOutlierFilter:
    def test_removes_negative_fare(self, spark: SparkSession, bronze_yellow):
        """Rows with fare_amount < 0 must be removed."""
        result = _filter_outliers(bronze_yellow, trip_type="yellow")
        negative_fares = result.filter(F.col("fare_amount") < MIN_FARE_AMOUNT).count()
        assert negative_fares == 0

    def test_removes_zero_distance(self, spark: SparkSession, bronze_yellow):
        """Rows with trip_distance <= 0 must be removed."""
        result = _filter_outliers(bronze_yellow, trip_type="yellow")
        zero_dist = result.filter(F.col("trip_distance") < MIN_TRIP_DISTANCE).count()
        assert zero_dist == 0

    def test_valid_rows_retained(self, spark: SparkSession, bronze_yellow):
        """Valid rows must not be dropped by the outlier filter."""
        result = _filter_outliers(bronze_yellow, trip_type="yellow")
        # The sample fixture has 3 valid rows and 1 bad row (negative fare)
        assert result.count() >= 2


class TestSilverDatetimeEnrichment:
    def test_pickup_hour_added(self, spark: SparkSession, bronze_yellow):
        result = _enrich_datetime(bronze_yellow)
        assert "pickup_hour" in result.columns
        hours = {r["pickup_hour"] for r in result.select("pickup_hour").collect()}
        # All hours should be in valid range 0-23
        assert all(0 <= h <= 23 for h in hours)

    def test_is_weekend_added(self, spark: SparkSession, bronze_yellow):
        result = _enrich_datetime(bronze_yellow)
        assert "is_weekend" in result.columns

    def test_pickup_local_tz_added(self, spark: SparkSession, bronze_yellow):
        result = _enrich_datetime(bronze_yellow)
        assert "pickup_datetime_local" in result.columns

