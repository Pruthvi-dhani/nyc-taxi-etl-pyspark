"""
Spark tuning constants referenced by spark_session.py and emr_serverless_job.json.
These can be overridden via environment variables in EMR job configurations.
"""

import os

# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
DRIVER_MEMORY: str = os.getenv("SPARK_DRIVER_MEMORY", "8g")
DRIVER_CORES: int = int(os.getenv("SPARK_DRIVER_CORES", "4"))

# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------
EXECUTOR_MEMORY: str = os.getenv("SPARK_EXECUTOR_MEMORY", "16g")
EXECUTOR_CORES: int = int(os.getenv("SPARK_EXECUTOR_CORES", "4"))
EXECUTOR_INSTANCES: int = int(os.getenv("SPARK_EXECUTOR_INSTANCES", "10"))

# ---------------------------------------------------------------------------
# SQL / shuffle
# ---------------------------------------------------------------------------
SHUFFLE_PARTITIONS: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "400"))
BROADCAST_THRESHOLD_MB: int = int(os.getenv("SPARK_BROADCAST_THRESHOLD_MB", "100"))

# ---------------------------------------------------------------------------
# S3 / I-O
# ---------------------------------------------------------------------------
# Number of Parquet output files per partition write
OUTPUT_FILE_MAX_BYTES: int = 128 * 1024 * 1024  # 128 MB target file size

# Spark conf dict — can be unpacked directly into SparkSession.builder
SPARK_CONF: dict[str, str] = {
    "spark.driver.memory": DRIVER_MEMORY,
    "spark.driver.cores": str(DRIVER_CORES),
    "spark.executor.memory": EXECUTOR_MEMORY,
    "spark.executor.cores": str(EXECUTOR_CORES),
    "spark.executor.instances": str(EXECUTOR_INSTANCES),
    "spark.sql.shuffle.partitions": str(SHUFFLE_PARTITIONS),
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": str(BROADCAST_THRESHOLD_MB * 1024 * 1024),
    "spark.sql.parquet.mergeSchema": "false",
    "spark.sql.parquet.filterPushdown": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
}

