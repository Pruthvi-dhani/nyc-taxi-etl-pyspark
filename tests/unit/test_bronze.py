"""Unit tests for the Bronze transformation layer."""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.transformation.bronze import transform_to_bronze


class TestBronzeTransformation:
    def test_column_rename_yellow(self, spark: SparkSession, yellow_raw_df):
        """Vendor ID and datetime columns should be renamed to snake_case."""
        result = transform_to_bronze(yellow_raw_df, trip_type="yellow")
        assert "vendor_id" in result.columns
        assert "pickup_datetime" in result.columns
        assert "dropoff_datetime" in result.columns
        assert "pu_location_id" in result.columns
        assert "do_location_id" in result.columns
        # Original names should be gone
        assert "VendorID" not in result.columns
        assert "tpep_pickup_datetime" not in result.columns

    def test_audit_columns_added(self, spark: SparkSession, yellow_raw_df):
        """Bronze layer must add _ingested_at and _trip_type audit columns."""
        result = transform_to_bronze(yellow_raw_df, trip_type="yellow")
        assert "_ingested_at" in result.columns
        assert "_trip_type" in result.columns
        trip_types = [r["_trip_type"] for r in result.select("_trip_type").collect()]
        assert all(t == "yellow" for t in trip_types)

    def test_partition_columns_derived(self, spark: SparkSession, yellow_raw_df):
        """year and month partition columns must be derived from pickup_datetime."""
        result = transform_to_bronze(yellow_raw_df, trip_type="yellow")
        assert "year" in result.columns
        assert "month" in result.columns
        years = {r["year"] for r in result.select("year").collect()}
        assert years == {2022}

    def test_row_count_unchanged(self, spark: SparkSession, yellow_raw_df):
        """Bronze layer must not drop any rows."""
        result = transform_to_bronze(yellow_raw_df, trip_type="yellow")
        assert result.count() == yellow_raw_df.count()

    def test_green_rename(self, spark: SparkSession, green_raw_df):
        """Green taxi datetime columns should be renamed correctly."""
        result = transform_to_bronze(green_raw_df, trip_type="green")
        assert "pickup_datetime" in result.columns
        assert "dropoff_datetime" in result.columns
        assert "lpep_pickup_datetime" not in result.columns

