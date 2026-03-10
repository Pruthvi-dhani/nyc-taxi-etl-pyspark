"""
SparkSession factory.

On EMR Serverless the cluster is pre-configured; this factory adds
Iceberg/Glue catalog configuration and sensible defaults so that
local dev and EMR runs both work from the same codebase.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from src import config


def get_spark_session(app_name: str = "nyc-taxi-etl", local: bool = False) -> SparkSession:
    """
    Build and return a :class:`~pyspark.sql.SparkSession`.

    Parameters
    ----------
    app_name:
        Human-readable name surfaced in the Spark UI / EMR console.
    local:
        When ``True`` forces ``master("local[*]")`` for unit tests.
        On EMR Serverless this flag must be ``False`` (default).
    """
    builder = SparkSession.builder.appName(app_name)

    if local:
        builder = builder.master("local[*]")

    # -----------------------------------------------------------------------
    # Iceberg + AWS Glue catalog
    # -----------------------------------------------------------------------
    builder = (
        builder
        # Iceberg extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Use Glue as the Iceberg catalog
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", config.ICEBERG_WAREHOUSE)
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        # Default catalog
        .config("spark.sql.defaultCatalog", "glue_catalog")
        # -----------------------------------------------------------------------
        # Performance tuning
        # -----------------------------------------------------------------------
        .config("spark.sql.shuffle.partitions", str(config.SHUFFLE_PARTITIONS))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
        # -----------------------------------------------------------------------
        # S3 optimisations (EMR uses EMRFS; locally uses Hadoop S3A)
        # -----------------------------------------------------------------------
        .config("spark.hadoop.fs.s3.impl", "com.amazon.ws.emr.hadoop.fs.EmrFileSystem")
    )

    if local:
        # Switch to S3A for local runs (no EMRFS available)
        builder = builder.config(
            "spark.hadoop.fs.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark

