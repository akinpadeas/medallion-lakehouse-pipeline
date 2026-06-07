"""
tests/unit/test_silver_transformer.py

Unit tests for Silver layer transformation logic.

Uses chispa for PySpark DataFrame assertions — it gives clean diffs
when DataFrames don't match, unlike a raw assertEqual.

Run with: pytest tests/unit/ -v
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, TimestampType, StringType, BooleanType
)
from chispa.dataframe_comparer import assert_df_equality

from silver.silver_transformer import (
    apply_business_rules,
    deduplicate,
    add_derived_columns,
    handle_nulls,
)


@pytest.fixture(scope="session")
def spark():
    """Create a minimal local SparkSession for unit tests."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("medallion-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")  # Keep it fast for tests
        .getOrCreate()
    )


@pytest.fixture
def sample_trips_schema():
    return StructType([
        StructField("vendor_id", IntegerType()),
        StructField("pickup_datetime", TimestampType()),
        StructField("dropoff_datetime", TimestampType()),
        StructField("pickup_location_id", IntegerType()),
        StructField("dropoff_location_id", IntegerType()),
        StructField("passenger_count", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("fare_amount", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("payment_type", IntegerType()),
        StructField("_ingestion_ts", TimestampType()),
        StructField("_batch_id", StringType()),
    ])


class TestApplyBusinessRules:

    def test_flags_negative_fare(self, spark, sample_trips_schema):
        now = datetime(2024, 1, 15, 10, 0, 0)
        data = [(1, datetime(2024, 1, 15, 9, 0), datetime(2024, 1, 15, 9, 30),
                 100, 200, 2, 5.0, -10.0, 1.0, -8.0, 1, now, "batch-1")]
        df = spark.createDataFrame(data, sample_trips_schema)
        result = apply_business_rules(df)
        flags = result.select("_dq_flags").first()[0]
        assert "NEGATIVE_FARE" in flags

    def test_flags_zero_distance(self, spark, sample_trips_schema):
        now = datetime(2024, 1, 15, 10, 0, 0)
        data = [(1, datetime(2024, 1, 15, 9, 0), datetime(2024, 1, 15, 9, 30),
                 100, 200, 2, 0.0, 5.0, 1.0, 6.0, 1, now, "batch-1")]
        df = spark.createDataFrame(data, sample_trips_schema)
        result = apply_business_rules(df)
        flags = result.select("_dq_flags").first()[0]
        assert "ZERO_DISTANCE" in flags

    def test_no_flags_on_clean_row(self, spark, sample_trips_schema):
        now = datetime(2024, 1, 15, 10, 0, 0)
        data = [(1, datetime(2024, 1, 15, 9, 0), datetime(2024, 1, 15, 9, 30),
                 100, 200, 2, 5.0, 10.0, 2.0, 12.0, 1, now, "batch-1")]
        df = spark.createDataFrame(data, sample_trips_schema)
        result = apply_business_rules(df)
        flags = result.select("_dq_flags").first()[0]
        assert flags == "" or flags is None

    def test_flags_duration_anomaly(self, spark, sample_trips_schema):
        """A trip of 0 seconds should be flagged as DURATION_ANOMALY."""
        now = datetime(2024, 1, 15, 10, 0, 0)
        pickup = datetime(2024, 1, 15, 9, 0, 0)
        # Same pickup and dropoff time = 0 duration
        data = [(1, pickup, pickup, 100, 200, 2, 5.0, 10.0, 2.0, 12.0, 1, now, "batch-1")]
        df = spark.createDataFrame(data, sample_trips_schema)
        result = apply_business_rules(df)
        flags = result.select("_dq_flags").first()[0]
        assert "DURATION_ANOMALY" in flags


class TestDeduplicate:

    def test_keeps_most_recent_on_duplicate_keys(self, spark, sample_trips_schema):
        """When two rows share the same business key, keep the later-ingested one."""
        t1 = datetime(2024, 1, 15, 8, 0, 0)  # Older ingestion
        t2 = datetime(2024, 1, 15, 9, 0, 0)  # Newer ingestion
        pickup = datetime(2024, 1, 15, 7, 0, 0)
        dropoff = datetime(2024, 1, 15, 7, 30, 0)

        data = [
            (1, pickup, dropoff, 100, 200, 2, 5.0, 10.0, 2.0, 12.0, 1, t1, "batch-old"),
            (1, pickup, dropoff, 100, 200, 2, 5.0, 10.0, 2.0, 12.0, 1, t2, "batch-new"),
        ]
        df = spark.createDataFrame(data, sample_trips_schema)
        result = deduplicate(df)

        assert result.count() == 1
        assert result.first()["_batch_id"] == "batch-new"

    def test_preserves_distinct_keys(self, spark, sample_trips_schema):
        """Rows with different business keys should all be preserved."""
        now = datetime(2024, 1, 15, 10, 0, 0)
        data = [
            (1, datetime(2024, 1, 15, 9, 0), datetime(2024, 1, 15, 9, 30),
             100, 200, 2, 5.0, 10.0, 2.0, 12.0, 1, now, "batch-1"),
            (2, datetime(2024, 1, 15, 9, 0), datetime(2024, 1, 15, 9, 30),
             150, 250, 1, 3.0, 8.0, 1.5, 9.5, 1, now, "batch-1"),
        ]
        df = spark.createDataFrame(data, sample_trips_schema)
        result = deduplicate(df)
        assert result.count() == 2


class TestHandleNulls:

    def test_drops_rows_with_null_key(self, spark, sample_trips_schema):
        """Rows with null vendor_id should be dropped — can't join without a key."""
        now = datetime(2024, 1, 15, 10, 0, 0)
        data = [
            (None, datetime(2024, 1, 15, 9, 0), datetime(2024, 1, 15, 9, 30),
             100, 200, 2, 5.0, 10.0, 2.0, 12.0, 1, now, "batch-1"),
        ]
        df = spark.createDataFrame(data, sample_trips_schema)
        result = handle_nulls(df)
        assert result.count() == 0

    def test_fills_null_metric_with_zero(self, spark, sample_trips_schema):
        """Null fare_amount should be filled with 0.0, not dropped."""
        now = datetime(2024, 1, 15, 10, 0, 0)
        data = [
            (1, datetime(2024, 1, 15, 9, 0), datetime(2024, 1, 15, 9, 30),
             100, 200, 2, 5.0, None, 2.0, None, 1, now, "batch-1"),
        ]
        df = spark.createDataFrame(data, sample_trips_schema)
        result = handle_nulls(df)
        assert result.count() == 1
        row = result.first()
        assert row["fare_amount"] == 0.0
        assert row["total_amount"] == 0.0
