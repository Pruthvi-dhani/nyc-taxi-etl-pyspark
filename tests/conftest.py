"""
Shared pytest fixtures for the test suite.

Provides:
- A local SparkSession (``spark``) reused across all tests in the session.
- Sample DataFrames for Yellow, Green, and FHV trips (small, in-memory).
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a local SparkSession for the test suite."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("nyc-taxi-etl-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def yellow_raw_df(spark: SparkSession):
    """Minimal raw Yellow taxi DataFrame matching the TLC schema."""
    data = [
        (1, "2022-03-01 08:00:00", "2022-03-01 08:20:00", 1.0, 3.5, 1.0, "N", 161, 236, 1, 14.0, 0.5, 0.5, 3.0, 0.0, 0.3, 18.3, 2.5, 0.0),
        (2, "2022-03-01 09:00:00", "2022-03-01 09:35:00", 2.0, 8.2, 1.0, "N", 132, 48,  1, 28.0, 0.0, 0.5, 5.0, 0.0, 0.3, 33.8, 2.5, 1.75),
        (1, "2022-03-02 22:00:00", "2022-03-02 22:10:00", 1.0, 1.1, 1.0, "N", 236, 161, 2,  7.5, 0.5, 0.5, 0.0, 0.0, 0.3,  8.8, 0.0, 0.0),
        # Bad row — negative fare (should be filtered in Silver)
        (1, "2022-03-03 10:00:00", "2022-03-03 10:15:00", 1.0, 2.0, 1.0, "N", 100, 200, 1, -5.0, 0.5, 0.5, 0.0, 0.0, 0.3,  0.0, 0.0, 0.0),
    ]
    columns = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
        "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra",
        "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge", "airport_fee",
    ]
    return spark.createDataFrame(data, schema=columns).select(
        F.col("VendorID"),
        F.to_timestamp("tpep_pickup_datetime").alias("tpep_pickup_datetime"),
        F.to_timestamp("tpep_dropoff_datetime").alias("tpep_dropoff_datetime"),
        *[F.col(c) for c in columns[3:]],
    )


@pytest.fixture(scope="session")
def green_raw_df(spark: SparkSession):
    """Minimal raw Green taxi DataFrame."""
    data = [
        (2, "2022-04-01 07:00:00", "2022-04-01 07:25:00", "N", 1.0, 74, 41, 1.0, 4.5, 15.0, 0.5, 0.5, 2.5, 0.0, 0.0, 0.3, 18.8, 1.0, 1.0, 2.5),
        (1, "2022-04-02 18:00:00", "2022-04-02 18:40:00", "N", 1.0, 41, 74, 2.0, 9.3, 30.0, 1.0, 0.5, 5.0, 0.0, 0.0, 0.3, 36.8, 1.0, 1.0, 2.5),
    ]
    columns = [
        "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
        "store_and_fwd_flag", "RatecodeID", "PULocationID", "DOLocationID",
        "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge",
        "total_amount", "payment_type", "trip_type", "congestion_surcharge",
    ]
    return spark.createDataFrame(data, schema=columns).select(
        F.col("VendorID"),
        F.to_timestamp("lpep_pickup_datetime").alias("lpep_pickup_datetime"),
        F.to_timestamp("lpep_dropoff_datetime").alias("lpep_dropoff_datetime"),
        *[F.col(c) for c in columns[3:]],
    )


