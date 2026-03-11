"""
Explicit PySpark StructType schemas for each TLC trip type.

Defining schemas up-front avoids the schema-inference overhead on large
Parquet datasets and guards against year-on-year column additions that
would otherwise cause silent type-widening or missing-column errors.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Yellow taxi  (2019-present column layout)
# ---------------------------------------------------------------------------
YELLOW_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
    ]
)

# ---------------------------------------------------------------------------
# Green taxi
# ---------------------------------------------------------------------------
GREEN_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("lpep_pickup_datetime", TimestampType(), True),
        StructField("lpep_dropoff_datetime", TimestampType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("ehail_fee", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_type", DoubleType(), True),
        StructField("trip_type", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
    ]
)

# ---------------------------------------------------------------------------
# FHV (For-Hire Vehicle)
# ---------------------------------------------------------------------------
FHV_SCHEMA = StructType(
    [
        StructField("dispatching_base_num", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropOff_datetime", TimestampType(), True),
        StructField("PUlocationID", DoubleType(), True),
        StructField("DOlocationID", DoubleType(), True),
        StructField("SR_Flag", IntegerType(), True),
        StructField("Affiliated_base_number", StringType(), True),
    ]
)

# ---------------------------------------------------------------------------
# FHVHV (High-Volume For-Hire Vehicle — Uber, Lyft, Via, etc.)
# ---------------------------------------------------------------------------
FHVHV_SCHEMA = StructType(
    [
        StructField("hvfhs_license_num", StringType(), True),
        StructField("dispatching_base_num", StringType(), True),
        StructField("originating_base_num", StringType(), True),
        StructField("request_datetime", TimestampType(), True),
        StructField("on_scene_datetime", TimestampType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("trip_miles", DoubleType(), True),
        StructField("trip_time", LongType(), True),
        StructField("base_passenger_fare", DoubleType(), True),
        StructField("tolls", DoubleType(), True),
        StructField("bcf", DoubleType(), True),
        StructField("sales_tax", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("tips", DoubleType(), True),
        StructField("driver_pay", DoubleType(), True),
        StructField("shared_request_flag", StringType(), True),
        StructField("shared_match_flag", StringType(), True),
        StructField("access_a_ride_flag", StringType(), True),
        StructField("wav_request_flag", StringType(), True),
        StructField("wav_match_flag", StringType(), True),
    ]
)

# Convenience mapping
SCHEMAS: dict = {
    "yellow": YELLOW_SCHEMA,
    "green": GREEN_SCHEMA,
    "fhv": FHV_SCHEMA,
    "fhvhv": FHVHV_SCHEMA,
}

