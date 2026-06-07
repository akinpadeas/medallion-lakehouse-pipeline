"""
utils/delta_utils.py

Helper functions for common Delta Lake operations:
  - OPTIMIZE (file compaction)
  - ZORDER (co-locate related data)
  - VACUUM (remove old files)
  - Time travel queries
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


def optimize_table(spark: SparkSession, table_path: str, zorder_cols: list[str] = None) -> None:
    """
    Run OPTIMIZE on a Delta table, optionally with ZORDER.

    OPTIMIZE compacts small files into larger ones, improving read performance.
    ZORDER co-locates rows with similar values in the same files — most useful
    on high-cardinality columns used in frequent filter predicates.

    Args:
        spark: Active SparkSession.
        table_path: Path or table name of the Delta table.
        zorder_cols: Columns to ZORDER by (e.g., ["pickup_zone_id", "vendor_id"]).
    """
    if zorder_cols:
        cols = ", ".join(zorder_cols)
        sql = f"OPTIMIZE delta.`{table_path}` ZORDER BY ({cols})"
        logger.info(f"Running OPTIMIZE with ZORDER BY ({cols}) on {table_path}")
    else:
        sql = f"OPTIMIZE delta.`{table_path}`"
        logger.info(f"Running OPTIMIZE on {table_path}")

    spark.sql(sql)
    logger.info("OPTIMIZE complete.")


def vacuum_table(spark: SparkSession, table_path: str, retention_hours: int = 168) -> None:
    """
    Run VACUUM to remove files older than retention_hours.

    Default retention is 168 hours (7 days) — Delta's minimum safe threshold.
    Never set below 168 unless you're certain no concurrent readers exist.

    Args:
        spark: Active SparkSession.
        table_path: Path or table name of the Delta table.
        retention_hours: How many hours of file history to retain.
    """
    logger.info(f"Running VACUUM on {table_path} (retain {retention_hours}h)")
    spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS")
    logger.info("VACUUM complete.")


def time_travel_query(spark: SparkSession, table_path: str, version: int = None, timestamp: str = None):
    """
    Read a Delta table at a specific version or timestamp.

    Args:
        spark: Active SparkSession.
        table_path: Path to the Delta table.
        version: Specific version number to read.
        timestamp: ISO timestamp string to read (e.g., "2024-01-15 00:00:00").

    Returns:
        DataFrame at the requested point in time.
    """
    if version is not None:
        logger.info(f"Time travel: reading {table_path} at version {version}")
        return spark.read.format("delta").option("versionAsOf", version).load(table_path)
    elif timestamp is not None:
        logger.info(f"Time travel: reading {table_path} at {timestamp}")
        return spark.read.format("delta").option("timestampAsOf", timestamp).load(table_path)
    else:
        raise ValueError("Provide either version or timestamp for time travel.")


def get_table_history(spark: SparkSession, table_path: str, limit: int = 10):
    """
    Return the Delta transaction log history for a table.

    Useful for auditing: who wrote what, when, and which operation.

    Args:
        spark: Active SparkSession.
        table_path: Path to the Delta table.
        limit: Number of history entries to return (most recent first).

    Returns:
        DataFrame with version, timestamp, operation, and operationParameters.
    """
    dt = DeltaTable.forPath(spark, table_path)
    history = dt.history(limit)
    return history.select("version", "timestamp", "operation", "operationParameters", "userMetadata")


def upsert_to_delta(
    spark: SparkSession,
    source_df,
    target_path: str,
    merge_keys: list[str],
    update_cols: list[str] = None,
) -> None:
    """
    Merge (upsert) a source DataFrame into a Delta table.

    Performs an INSERT when no match, UPDATE when match found.
    Used for Silver layer idempotent reloads and incremental updates.

    Args:
        spark: Active SparkSession.
        source_df: DataFrame containing new/updated records.
        target_path: Path to the target Delta table.
        merge_keys: Columns that uniquely identify a record (join keys).
        update_cols: Columns to update on match. If None, updates all columns.
    """
    target = DeltaTable.forPath(spark, target_path)

    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    merge_builder = (
        target.alias("target")
        .merge(source_df.alias("source"), merge_condition)
    )

    if update_cols:
        update_set = {col: f"source.{col}" for col in update_cols}
        merge_builder = merge_builder.whenMatchedUpdate(set=update_set)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    merge_builder.whenNotMatchedInsertAll().execute()

    logger.info(f"Upsert complete into {target_path} on keys: {merge_keys}")
