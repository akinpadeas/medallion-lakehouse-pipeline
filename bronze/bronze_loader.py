"""
bronze/bronze_loader.py

Lands raw source data into the Bronze Delta layer.

Bronze is the immutable historical record — data is written as-is,
with no transformation beyond adding audit metadata. This lets us
replay any downstream layer from scratch without re-ingesting from source.

Design principles:
  - Append-only writes (no updates or deletes)
  - Schema-on-read: source schema is preserved
  - Every row gets: ingestion_ts, source_file, batch_id, pipeline_version
  - Partition by ingestion date for efficient downstream reads
"""

import uuid
from datetime import datetime, timezone
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
import logging

from utils.spark_session import get_spark_session

logger = logging.getLogger(__name__)

BRONZE_BASE_PATH = "data/bronze"
PIPELINE_VERSION = "1.0.0"


def add_audit_columns(df: DataFrame, source_file: str, batch_id: str) -> DataFrame:
    """
    Append standard Bronze audit columns to every row.

    These columns enable full lineage tracing — given any Gold row,
    you can trace back to the exact source file and ingestion batch.

    Args:
        df: Raw source DataFrame.
        source_file: Path or identifier of the source file/API endpoint.
        batch_id: Unique identifier for this pipeline run.

    Returns:
        DataFrame with audit columns added.
    """
    return (
        df
        .withColumn("_ingestion_ts", F.lit(datetime.now(timezone.utc)).cast(TimestampType()))
        .withColumn("_ingestion_date", F.to_date(F.lit(datetime.now(timezone.utc).date().isoformat())))
        .withColumn("_source_file", F.lit(source_file).cast(StringType()))
        .withColumn("_batch_id", F.lit(batch_id).cast(StringType()))
        .withColumn("_pipeline_version", F.lit(PIPELINE_VERSION).cast(StringType()))
    )


def write_to_bronze(
    df: DataFrame,
    table_name: str,
    source_file: str,
    batch_id: str = None,
    partition_cols: list[str] = None,
) -> dict:
    """
    Write a DataFrame to the Bronze Delta layer.

    Args:
        df: Raw source DataFrame.
        table_name: Logical name for the Bronze table (e.g., "trips", "zones").
        source_file: Source file path or API identifier.
        batch_id: Pipeline run ID. Auto-generated if not provided.
        partition_cols: Columns to partition the Delta table by.
                        Defaults to ["_ingestion_date"].

    Returns:
        Dictionary with run metadata (batch_id, row_count, table_path).
    """
    batch_id = batch_id or str(uuid.uuid4())
    partition_cols = partition_cols or ["_ingestion_date"]
    table_path = f"{BRONZE_BASE_PATH}/{table_name}"

    logger.info(f"Starting Bronze write | table={table_name} | batch={batch_id}")

    enriched_df = add_audit_columns(df, source_file, batch_id)
    row_count = enriched_df.count()

    (
        enriched_df
        .write
        .format("delta")
        .mode("append")
        .partitionBy(*partition_cols)
        .option("mergeSchema", "true")   # Allow additive schema changes
        .save(table_path)
    )

    logger.info(f"Bronze write complete | rows={row_count:,} | path={table_path}")

    return {
        "batch_id": batch_id,
        "table": table_name,
        "table_path": table_path,
        "rows_written": row_count,
        "ingestion_ts": datetime.now(timezone.utc).isoformat(),
    }


def load_nyc_taxi_to_bronze(
    spark: SparkSession,
    source_path: str,
    execution_date: str,
) -> dict:
    """
    Read NYC Taxi Parquet files and land them in Bronze.

    Args:
        spark: Active SparkSession.
        source_path: Path to source Parquet files.
        execution_date: Pipeline execution date (YYYY-MM-DD).

    Returns:
        Metadata dict from write_to_bronze.
    """
    logger.info(f"Reading NYC Taxi data from {source_path}")

    df = spark.read.parquet(source_path)

    logger.info(f"Source schema: {df.schema.simpleString()}")
    logger.info(f"Source row count: {df.count():,}")

    return write_to_bronze(
        df=df,
        table_name="trips",
        source_file=source_path,
        partition_cols=["_ingestion_date"],
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Bronze loader for NYC Taxi data")
    parser.add_argument("--source", required=True, help="Source Parquet path")
    parser.add_argument("--date", required=True, help="Execution date YYYY-MM-DD")
    args = parser.parse_args()

    spark = get_spark_session("MedallionBronzeLoader")
    result = load_nyc_taxi_to_bronze(spark, args.source, args.date)
    print(f"Bronze load complete: {result}")
