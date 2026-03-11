"""
S3 reader for NYC TLC Parquet files.

Builds S3 URIs from the public TLC bucket layout::

    s3://nyc-tlc/trip data/<type>_tripdata_<YYYY-MM>.parquet

and reads them directly into a Spark DataFrame using the schema
defined in :mod:`src.ingestion.schema`.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src import config
from src.ingestion.schema import SCHEMAS
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _build_s3_paths(
    trip_type: str,
    start_year: int,
    end_year: int,
    start_month: int = 1,
    end_month: int = 12,
) -> list[str]:
    """Return a list of S3 URIs covering the requested date range.

    Layout: s3://<RAW_BUCKET>/raw/<trip_type>/<trip_type>_tripdata_YYYY-MM.parquet
    """
    paths: list[str] = []
    for year in range(start_year, end_year + 1):
        m_start = start_month if year == start_year else 1
        m_end = end_month if year == end_year else 12
        for month in range(m_start, m_end + 1):
            path = (
                f"{config.TLC_S3_PREFIX}/{trip_type}/"
                f"{trip_type}_tripdata_{year}-{month:02d}.parquet"
            )
            paths.append(path)
    return paths


def read_trip_data(
    spark: SparkSession,
    trip_type: str,
    start_year: int = config.DEFAULT_START_YEAR,
    end_year: int = config.DEFAULT_END_YEAR,
    start_month: int = config.DEFAULT_START_MONTH,
    end_month: int = config.DEFAULT_END_MONTH,
) -> DataFrame:
    """
    Read raw TLC Parquet files from S3 into a Spark DataFrame.

    Parameters
    ----------
    spark:
        Active :class:`~pyspark.sql.SparkSession`.
    trip_type:
        One of ``"yellow"``, ``"green"``, or ``"fhv"``.
    start_year / end_year:
        Inclusive year range to ingest.
    start_month / end_month:
        Inclusive month range (applied to first/last year respectively).

    Returns
    -------
    DataFrame
        Raw, schema-enforced DataFrame with an added ``_source_file`` column.
    """
    if trip_type not in SCHEMAS:
        raise ValueError(f"Unsupported trip_type '{trip_type}'. Choose from {list(SCHEMAS)}")

    paths = _build_s3_paths(trip_type, start_year, end_year, start_month, end_month)
    schema = SCHEMAS[trip_type]

    logger.info("Reading %d Parquet file(s) for trip_type='%s'", len(paths), trip_type)
    logger.info("Date range: %d-%02d → %d-%02d", start_year, start_month, end_year, end_month)

    from pyspark.sql import functions as F

    df = (
        spark.read.schema(schema)
        .option("mergeSchema", "false")
        .option("pathGlobFilter", "*.parquet")
        .parquet(*paths)
    )

    # Attach source file path for Bronze lineage tracking
    df = df.withColumn("_source_file", F.input_file_name())

    logger.info("Schema loaded successfully for trip_type='%s'", trip_type)
    return df


