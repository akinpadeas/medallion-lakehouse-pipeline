"""
silver/silver_transformer.py

Transforms Bronze raw data into clean, validated Silver records.

Silver responsibilities:
  - Type casting and null handling
  - Deduplication (using row_number over business keys)
  - Business rule validation (e.g., fare > 0, trip_distance >= 0)
  - Outlier flagging (not removal — flagged for downstream use)
  - Standardized column naming (snake_case, no vendor-specific abbreviations)

Silver writes use upsert (MERGE) — re-running the same date is safe.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, TimestampType, StringType
)
import logging

from utils.spark_session import get_spark_session
from utils.delta_utils import upsert_to_delta

logger = logging.getLogger(__name__)

BRONZE_BASE_PATH = "data/bronze"
SILVER_BASE_PATH = "data/silver"


# ---------------------------------------------------------------------------
# Type casting
# ---------------------------------------------------------------------------

def cast_trips_schema(df: DataFrame) -> DataFrame:
    """
    Enforce correct types on raw Bronze trip data.

    Bronze lands everything as source types (often strings from CSV,
    or loosely typed Parquet). Silver enforces the canonical schema.
    """
    return (
        df
        .withColumn("vendor_id", F.col("VendorID").cast(IntegerType()))
        .withColumn("pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("pickup_location_id", F.col("PULocationID").cast(IntegerType()))
        .withColumn("dropoff_location_id", F.col("DOLocationID").cast(IntegerType()))
        .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        # Carry audit columns forward
        .withColumn("_ingestion_ts", F.col("_ingestion_ts"))
        .withColumn("_ingestion_date", F.col("_ingestion_date"))
        .withColumn("_source_file", F.col("_source_file"))
        .withColumn("_batch_id", F.col("_batch_id"))
    )


# ---------------------------------------------------------------------------
# Null handling
# ---------------------------------------------------------------------------

def handle_nulls(df: DataFrame) -> DataFrame:
    """
    Apply null handling strategy per column type.

    Strategy:
      - Business key columns (vendor_id, location_ids): drop rows — nulls
        in keys make joins unreliable.
      - Metric columns (fare, tip, distance): fill with 0.0 and flag.
      - Categorical columns (payment_type): fill with -1 (unknown sentinel).
    """
    key_cols = ["vendor_id", "pickup_datetime", "dropoff_datetime",
                "pickup_location_id", "dropoff_location_id"]

    df = df.dropna(subset=key_cols)

    df = (
        df
        .withColumn("trip_distance", F.coalesce(F.col("trip_distance"), F.lit(0.0)))
        .withColumn("fare_amount", F.coalesce(F.col("fare_amount"), F.lit(0.0)))
        .withColumn("tip_amount", F.coalesce(F.col("tip_amount"), F.lit(0.0)))
        .withColumn("total_amount", F.coalesce(F.col("total_amount"), F.lit(0.0)))
        .withColumn("passenger_count", F.coalesce(F.col("passenger_count"), F.lit(0)))
        .withColumn("payment_type", F.coalesce(F.col("payment_type"), F.lit(-1)))
    )

    return df


# ---------------------------------------------------------------------------
# Business rule validation
# ---------------------------------------------------------------------------

def apply_business_rules(df: DataFrame) -> DataFrame:
    """
    Add a _dq_flag column encoding which business rules each row violates.

    Flagged rows are KEPT in Silver (not dropped) so analysts can
    investigate data quality issues at source. Gold layer filters them out.

    Flags (pipe-separated):
      - NEGATIVE_FARE: fare_amount < 0
      - ZERO_DISTANCE: trip_distance == 0
      - DURATION_ANOMALY: trip took < 1 min or > 5 hours
      - FUTURE_PICKUP: pickup_datetime is in the future
      - PASSENGER_ANOMALY: passenger_count > 8 or < 1
    """
    now = F.current_timestamp()

    df = df.withColumn(
        "trip_duration_minutes",
        (F.col("dropoff_datetime").cast(LongType()) - F.col("pickup_datetime").cast(LongType())) / 60,
    )

    flags = (
        F.when(F.col("fare_amount") < 0, F.lit("NEGATIVE_FARE|")).otherwise(F.lit(""))
        + F.when(F.col("trip_distance") == 0, F.lit("ZERO_DISTANCE|")).otherwise(F.lit(""))
        + F.when(
            (F.col("trip_duration_minutes") < 1) | (F.col("trip_duration_minutes") > 300),
            F.lit("DURATION_ANOMALY|"),
        ).otherwise(F.lit(""))
        + F.when(F.col("pickup_datetime") > now, F.lit("FUTURE_PICKUP|")).otherwise(F.lit(""))
        + F.when(
            (F.col("passenger_count") < 1) | (F.col("passenger_count") > 8),
            F.lit("PASSENGER_ANOMALY|"),
        ).otherwise(F.lit(""))
    )

    return df.withColumn("_dq_flags", F.rtrim(F.lit("|"), flags))


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicate rows using row_number over business keys.

    Within a set of rows sharing the same business key, we keep the
    most recently ingested one (_ingestion_ts DESC). This handles
    cases where the same source file is accidentally re-ingested.
    """
    window = Window.partitionBy(
        "vendor_id", "pickup_datetime", "pickup_location_id", "dropoff_location_id"
    ).orderBy(F.col("_ingestion_ts").desc())

    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ---------------------------------------------------------------------------
# Add derived columns
# ---------------------------------------------------------------------------

def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Add business-meaningful derived columns used in Gold models.

    Keeping these in Silver (rather than Gold) makes them available
    to multiple Gold consumers without re-computing.
    """
    return (
        df
        .withColumn("pickup_date", F.to_date(F.col("pickup_datetime")))
        .withColumn("pickup_hour", F.hour(F.col("pickup_datetime")))
        .withColumn("pickup_day_of_week", F.dayofweek(F.col("pickup_datetime")))
        .withColumn(
            "is_weekend",
            F.when(F.col("pickup_day_of_week").isin([1, 7]), True).otherwise(False),
        )
        .withColumn(
            "speed_mph",
            F.when(
                (F.col("trip_duration_minutes") > 0) & (F.col("trip_distance") > 0),
                F.round(F.col("trip_distance") / (F.col("trip_duration_minutes") / 60), 2),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
    )


# ---------------------------------------------------------------------------
# Full Silver transformation pipeline
# ---------------------------------------------------------------------------

def transform_trips_to_silver(
    spark: SparkSession,
    execution_date: str,
) -> dict:
    """
    Full Bronze → Silver pipeline for the trips table.

    Steps:
      1. Read Bronze partition for execution_date
      2. Cast schema
      3. Handle nulls
      4. Apply business rules (flag anomalies)
      5. Deduplicate
      6. Add derived columns
      7. Upsert into Silver Delta table

    Args:
        spark: Active SparkSession.
        execution_date: Date to process (YYYY-MM-DD).

    Returns:
        Dict with row counts and metadata.
    """
    bronze_path = f"{BRONZE_BASE_PATH}/trips"
    silver_path = f"{SILVER_BASE_PATH}/trips"

    logger.info(f"Reading Bronze trips for {execution_date}")
    bronze_df = (
        spark.read.format("delta").load(bronze_path)
        .filter(F.col("_ingestion_date") == execution_date)
    )
    bronze_count = bronze_df.count()
    logger.info(f"Bronze rows read: {bronze_count:,}")

    df = cast_trips_schema(bronze_df)
    df = handle_nulls(df)
    df = apply_business_rules(df)
    df = deduplicate(df)
    df = add_derived_columns(df)

    silver_count = df.count()
    dropped = bronze_count - silver_count
    logger.info(f"Silver rows after transforms: {silver_count:,} ({dropped:,} dropped as null key rows)")

    upsert_to_delta(
        spark=spark,
        source_df=df,
        target_path=silver_path,
        merge_keys=["vendor_id", "pickup_datetime", "pickup_location_id", "dropoff_location_id"],
    )

    return {
        "execution_date": execution_date,
        "bronze_rows": bronze_count,
        "silver_rows": silver_count,
        "dropped_rows": dropped,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Silver transformer for trips table")
    parser.add_argument("--date", required=True, help="Execution date YYYY-MM-DD")
    args = parser.parse_args()

    spark = get_spark_session("MedallionSilverTransformer")
    result = transform_trips_to_silver(spark, args.date)
    print(f"Silver transform complete: {result}")
