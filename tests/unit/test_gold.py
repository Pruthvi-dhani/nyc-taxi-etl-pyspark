"""Unit tests for the Gold transformation layer."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.transformation.bronze import transform_to_bronze
from src.transformation.gold import (
    build_fare_summary,
    build_trips_by_borough,
    build_trips_by_date,
    build_trips_by_hour,
    transform_to_gold,
)
from src.transformation.silver import _drop_nulls, _enrich_datetime, _filter_outliers


@pytest.fixture()
def silver_yellow(spark: SparkSession, yellow_raw_df):
    """Produce a minimal Silver-like DataFrame from the sample fixture."""
    df = transform_to_bronze(yellow_raw_df, trip_type="yellow")
    df = _drop_nulls(df, trip_type="yellow")
    df = _filter_outliers(df, trip_type="yellow")
    df = _enrich_datetime(df)
    # Add trip duration and required columns not yet in the test fixture
    df = df.withColumn(
        "trip_duration_mins",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,
    )
    df = df.withColumn("pickup_borough", F.lit("Manhattan"))
    df = df.withColumn("dropoff_borough", F.lit("Brooklyn"))
    df = df.withColumn("pickup_zone", F.lit("Midtown Center"))
    df = df.withColumn("dropoff_zone", F.lit("Park Slope"))
    df = df.withColumn("is_airport_trip", F.lit(False))
    df = df.withColumn("pickup_date", F.to_date("pickup_datetime_local"))
    return df


class TestGoldTripsHour:
    def test_has_trip_count(self, silver_yellow):
        result = build_trips_by_hour(silver_yellow)
        assert "trip_count" in result.columns
        assert result.count() > 0

    def test_hours_in_valid_range(self, silver_yellow):
        result = build_trips_by_hour(silver_yellow)
        hours = {r["pickup_hour"] for r in result.select("pickup_hour").collect()}
        assert all(0 <= h <= 23 for h in hours)


class TestGoldTripsDate:
    def test_has_required_columns(self, silver_yellow):
        result = build_trips_by_date(silver_yellow)
        for col in ("pickup_date", "trip_count", "total_revenue", "avg_fare"):
            assert col in result.columns

    def test_no_negative_trip_counts(self, silver_yellow):
        result = build_trips_by_date(silver_yellow)
        assert result.filter(F.col("trip_count") < 0).count() == 0


class TestGoldBorough:
    def test_borough_aggregation(self, silver_yellow):
        result = build_trips_by_borough(silver_yellow)
        assert "pickup_borough" in result.columns
        boroughs = {r["pickup_borough"] for r in result.select("pickup_borough").collect()}
        assert "Manhattan" in boroughs


class TestGoldFareSummary:
    def test_tip_pct_column(self, silver_yellow):
        result = build_fare_summary(silver_yellow)
        assert "avg_tip_pct" in result.columns

    def test_median_fare_positive(self, silver_yellow):
        result = build_fare_summary(silver_yellow)
        medians = [r["median_fare"] for r in result.select("median_fare").collect()]
        assert all(m > 0 for m in medians if m is not None)


class TestTransformToGold:
    def test_returns_all_tables(self, silver_yellow):
        tables = transform_to_gold(silver_yellow, trip_type="yellow")
        expected = {"trips_by_hour", "trips_by_date", "trips_by_borough", "fare_summary", "trip_duration_summary"}
        assert expected == set(tables.keys())

    def test_fhv_excludes_fare_summary(self, spark: SparkSession, silver_yellow):
        tables = transform_to_gold(silver_yellow, trip_type="fhv")
        assert "fare_summary" not in tables

