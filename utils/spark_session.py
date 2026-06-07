"""
utils/spark_session.py

Shared SparkSession factory. All pipeline stages import from here
so configuration is centralized and consistent.
"""

from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str, enable_delta: bool = True) -> SparkSession:
    """
    Create or retrieve an existing SparkSession.

    Args:
        app_name: Name shown in Spark UI and logs.
        enable_delta: Whether to configure Delta Lake extensions.

    Returns:
        Configured SparkSession.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Optimize small file writes
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        # Auto-compact Delta tables after writes
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        # Partition pruning
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        # Adaptive query execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    if not enable_delta:
        builder = SparkSession.builder.appName(app_name)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"SparkSession started: {app_name} | version {spark.version}")
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Gracefully stop the SparkSession."""
    if spark:
        logger.info("Stopping SparkSession.")
        spark.stop()
