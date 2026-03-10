"""
Bronze layer transformation.

Responsibilities
----------------
* Normalise column names to snake_case.
* Add audit columns: ``_ingested_at``, ``_trip_type``, ``_source_file``.
* Write partitioned Parquet to the Bronze S3 prefix.

The Bronze layer is intentionally *light-touch* — no business logic,
no row drops.  It is a faithful, timestamped copy of the raw data.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import config
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Mapping of raw column names → normalised snake_case names per trip type
_YELLOW_RENAME: dict[str, str] = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
}

_GREEN_RENAME: dict[str, str] = {
    "VendorID": "vendor_id",
    "lpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
}

_FHV_RENAME: dict[str, str] = {
    "PUlocationID": "pu_location_id",
    "DOlocationID": "do_location_id",
    "SR_Flag": "sr_flag",
    "Affiliated_base_number": "affiliated_base_number",
}

_RENAME_MAPS: dict[str, dict[str, str]] = {
    "yellow": _YELLOW_RENAME,
    "green": _GREEN_RENAME,
    "fhv": _FHV_RENAME,
}


def _rename_columns(df: DataFrame, trip_type: str) -> DataFrame:
    rename_map = _RENAME_MAPS.get(trip_type, {})
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def transform_to_bronze(
    df: DataFrame,
    trip_type: str,
) -> DataFrame:
    """
    Apply Bronze-layer transformations to a raw ingestion DataFrame.

    Parameters
    ----------
    df:
        Raw DataFrame as produced by :func:`src.ingestion.reader.read_trip_data`.
    trip_type:
        One of ``"yellow"``, ``"green"``, ``"fhv"``.

    Returns
    -------
    DataFrame
        Bronze DataFrame with audit columns and normalised column names.
    """
    logger.info("Applying Bronze transformations for trip_type='%s'", trip_type)

    df = _rename_columns(df, trip_type)

    df = (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_trip_type", F.lit(trip_type))
    )

    # Derive partition columns from the pickup datetime
    if "pickup_datetime" in df.columns:
        df = (
            df.withColumn("year", F.year("pickup_datetime"))
            .withColumn("month", F.month("pickup_datetime"))
        )

    return df


def write_bronze(
    df: DataFrame,
    trip_type: str,
    mode: str = "append",
) -> None:
    """
    Write the Bronze DataFrame to S3 as partitioned Parquet.

    Parameters
    ----------
    df:
        Bronze-transformed DataFrame.
    trip_type:
        Used as the first partition level.
    mode:
        Spark write mode (``"append"`` or ``"overwrite"``).
    """
    output_path = f"{config.BRONZE_PREFIX}/{trip_type}"
    partition_cols = ["year", "month"] if "year" in df.columns else []

    logger.info("Writing Bronze data to %s (mode=%s)", output_path, mode)

    writer = df.write.format("parquet").mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(output_path)
    logger.info("Bronze write complete for trip_type='%s'", trip_type)

