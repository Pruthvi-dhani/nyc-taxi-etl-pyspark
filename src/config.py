"""
Central configuration for the NYC Taxi ETL pipeline.

All S3 paths, date ranges, and tuning knobs live here so that
job scripts never need hard-coded strings.
"""

import os

# ---------------------------------------------------------------------------
# S3 bucket layout
# ---------------------------------------------------------------------------
RAW_BUCKET: str = os.getenv("RAW_BUCKET", "nyc-taxi-etl-spark-raw")
PIPELINE_BUCKET: str = os.getenv("PIPELINE_BUCKET", "my-nyc-taxi-bucket")

# TLC public data prefix  (s3://nyc-taxi-etl-spark-raw/raw/<trip_type>/)
TLC_S3_PREFIX: str = f"s3://{RAW_BUCKET}/raw"

# Medallion layer prefixes on the pipeline bucket
BRONZE_PREFIX: str = f"s3://{PIPELINE_BUCKET}/bronze"
SILVER_PREFIX: str = f"s3://{PIPELINE_BUCKET}/silver"
GOLD_PREFIX: str = f"s3://{PIPELINE_BUCKET}/gold"
ANALYTICS_PREFIX: str = f"s3://{PIPELINE_BUCKET}/analytics"

# Taxi zone lookup table (CSV) stored in the pipeline bucket
TAXI_ZONE_LOOKUP_PATH: str = f"s3://{PIPELINE_BUCKET}/reference/taxi_zone_lookup.csv"

# ---------------------------------------------------------------------------
# Data range defaults  (override via env vars in EMR job configs)
# ---------------------------------------------------------------------------
DEFAULT_START_YEAR: int = int(os.getenv("START_YEAR", "2022"))
DEFAULT_END_YEAR: int = int(os.getenv("END_YEAR", "2023"))
DEFAULT_START_MONTH: int = int(os.getenv("START_MONTH", "1"))
DEFAULT_END_MONTH: int = int(os.getenv("END_MONTH", "12"))

# Supported trip types
TRIP_TYPES: list[str] = ["yellow", "green", "fhv", "fhvhv"]

# ---------------------------------------------------------------------------
# AWS / EMR settings
# ---------------------------------------------------------------------------
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
EMR_APP_ID: str = os.getenv("EMR_APP_ID", "")
EMR_EXEC_ROLE_ARN: str = os.getenv("EMR_EXEC_ROLE_ARN", "")
EMR_LOG_URI: str = f"s3://{PIPELINE_BUCKET}/logs/emr-serverless/"

# ---------------------------------------------------------------------------
# Spark tuning (referenced by spark_session.py and emr_serverless_job.json)
# ---------------------------------------------------------------------------
SPARK_EXECUTOR_MEMORY: str = os.getenv("SPARK_EXECUTOR_MEMORY", "8g")
SPARK_EXECUTOR_CORES: int = int(os.getenv("SPARK_EXECUTOR_CORES", "4"))
SPARK_DRIVER_MEMORY: str = os.getenv("SPARK_DRIVER_MEMORY", "4g")
SHUFFLE_PARTITIONS: int = int(os.getenv("SHUFFLE_PARTITIONS", "200"))

# ---------------------------------------------------------------------------
# Glue / Iceberg catalog
# ---------------------------------------------------------------------------
GLUE_CATALOG_ID: str = os.getenv("GLUE_CATALOG_ID", "")
ICEBERG_WAREHOUSE: str = f"s3://{PIPELINE_BUCKET}/iceberg-warehouse"
DATABASE_NAME: str = os.getenv("DATABASE_NAME", "nyc_taxi")

