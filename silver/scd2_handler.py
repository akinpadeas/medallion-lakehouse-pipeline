"""
silver/scd2_handler.py

Implements SCD Type 2 (Slowly Changing Dimension Type 2) merge logic.

SCD Type 2 preserves the full history of a dimension by:
  - Closing the previous row when a tracked attribute changes
    (setting valid_to = effective_date - 1 day, is_current = False)
  - Inserting a new row with the updated attributes
    (valid_from = effective_date, valid_to = 9999-12-31, is_current = True)

This is contrasted with SCD Type 1 (overwrite, no history)
and SCD Type 3 (limited history via extra columns).

We use SCD Type 2 for dimensions where historical accuracy matters —
e.g., a zone that gets renamed should not retroactively rename all
historical trips that passed through it under the old name.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType
import logging

logger = logging.getLogger(__name__)

# Sentinel date: row is "current" until this date
SCD2_END_DATE = "9999-12-31"


def apply_scd2_merge(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    business_key_cols: list[str],
    tracked_cols: list[str],
    effective_date_col: str = "effective_date",
) -> None:
    """
    Perform an SCD Type 2 merge into a Delta table.

    Algorithm:
      1. Identify rows in source where tracked_cols differ from current target row
      2. For changed rows: close the existing target row (set valid_to, is_current=False)
      3. Insert new rows for: changed records + net-new records

    Args:
        spark: Active SparkSession.
        source_df: Incoming dimension data (one row per business key, current values).
        target_path: Delta table path for the target dimension table.
        business_key_cols: Natural/business key columns that identify a unique entity.
        tracked_cols: Columns whose changes trigger a new SCD2 row.
        effective_date_col: Column in source_df that specifies when the change took effect.
    """
    target = DeltaTable.forPath(spark, target_path)
    target_df = target.toDF()

    # -----------------------------------------------------------------------
    # Step 1: Identify changed records
    # Source rows where ANY tracked column differs from current target row.
    # -----------------------------------------------------------------------
    join_condition = " AND ".join(
        [f"source.{k} = target.{k}" for k in business_key_cols]
    )

    change_condition = " OR ".join(
        [f"source.{c} <> target.{c}" for c in tracked_cols]
    )

    # -----------------------------------------------------------------------
    # Step 2: Close existing rows for changed records
    # Merge: when matched AND changed → update valid_to and is_current
    # -----------------------------------------------------------------------
    close_expr = {
        "valid_to": f"source.{effective_date_col}",
        "is_current": "false",
    }

    (
        target.alias("target")
        .merge(
            source_df.alias("source"),
            f"{join_condition} AND target.is_current = true AND ({change_condition})",
        )
        .whenMatchedUpdate(set=close_expr)
        .execute()
    )

    logger.info("SCD2: Closed expired rows for changed records.")

    # -----------------------------------------------------------------------
    # Step 3: Insert new rows
    # New rows are needed for:
    #   a) Changed records (the updated version)
    #   b) Net-new records (business key doesn't exist in target at all)
    # -----------------------------------------------------------------------
    current_target_df = target.toDF().filter(F.col("is_current") == True)

    existing_keys = current_target_df.select(*business_key_cols)

    # Records not present in current target at all
    new_records = source_df.join(existing_keys, on=business_key_cols, how="left_anti")

    # Records that existed but were just closed (they're now not current)
    # Re-join source against target after the close step
    updated_target_df = target.toDF()
    closed_keys = (
        updated_target_df
        .filter(F.col("is_current") == False)
        .select(*business_key_cols)
        .join(current_target_df.select(*business_key_cols), on=business_key_cols, how="left_anti")
    )
    changed_records = source_df.join(closed_keys, on=business_key_cols, how="inner")

    rows_to_insert = new_records.union(changed_records)

    rows_to_insert = (
        rows_to_insert
        .withColumn("valid_from", F.col(effective_date_col).cast(DateType()))
        .withColumn("valid_to", F.lit(SCD2_END_DATE).cast(DateType()))
        .withColumn("is_current", F.lit(True).cast(BooleanType()))
    )

    insert_count = rows_to_insert.count()
    logger.info(f"SCD2: Inserting {insert_count:,} new/updated rows.")

    rows_to_insert.write.format("delta").mode("append").save(target_path)

    logger.info("SCD2 merge complete.")


def read_current_dimension(spark: SparkSession, table_path: str) -> DataFrame:
    """
    Return only the current (is_current = True) rows from an SCD2 table.

    Use this for joining to fact tables in Gold models where you want
    the latest attribute values.
    """
    return (
        spark.read.format("delta").load(table_path)
        .filter(F.col("is_current") == True)
    )


def read_dimension_as_of(spark: SparkSession, table_path: str, as_of_date: str) -> DataFrame:
    """
    Return dimension rows that were current on a specific date.

    Use this for historical fact analysis where you want the attribute
    values that were in effect at the time of the event, not today.

    Example: a trip in 2022 should use the 2022 zone name, not today's.

    Args:
        spark: Active SparkSession.
        table_path: Delta table path for the SCD2 dimension.
        as_of_date: Date string (YYYY-MM-DD) to look up.

    Returns:
        DataFrame filtered to rows valid on as_of_date.
    """
    return (
        spark.read.format("delta").load(table_path)
        .filter(
            (F.col("valid_from") <= F.lit(as_of_date).cast(DateType()))
            & (F.col("valid_to") >= F.lit(as_of_date).cast(DateType()))
        )
    )
