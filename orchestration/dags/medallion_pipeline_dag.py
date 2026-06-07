"""
orchestration/dags/medallion_pipeline_dag.py

Airflow DAG: medallion_pipeline

Orchestrates the full Bronze → Silver → Gold pipeline using the TaskFlow API.
Runs daily at 03:00 UTC (after source data is typically available).

Task graph:
    ingest_bronze_trips
          │
    validate_bronze
          │
    transform_silver_trips
          │
    validate_silver
          │
    ┌─────┴──────┐
    │            │
 run_dbt    validate_gold
    │
 notify_success

Design decisions:
  - TaskFlow API (@task decorator) for clean Python task definitions
  - XCom used only for lightweight metadata (batch_id, row counts) — never DataFrames
  - Retries on transient failures (Spark submission, API timeouts)
  - Email alert on any task failure via on_failure_callback
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default args — applied to all tasks unless overridden
# ---------------------------------------------------------------------------

default_args = {
    "owner": "samson.akinpade",
    "depends_on_past": False,
    "email": ["your-alert-email@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="medallion_pipeline",
    description="Daily Bronze → Silver → Gold pipeline for NYC Taxi data",
    default_args=default_args,
    schedule_interval="0 3 * * *",   # 03:00 UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,               # Prevent concurrent runs overlapping
    tags=["medallion", "nyc-taxi", "daily"],
    doc_md="""
## Medallion Pipeline DAG

Processes NYC Taxi trip data through the three-layer medallion architecture.

**Layers:**
- **Bronze**: Raw data landed from source with audit metadata
- **Silver**: Cleansed, deduplicated, validated records
- **Gold**: Kimball star schema models (via dbt)

**Backfill example:**
```bash
airflow dags backfill medallion_pipeline \\
    --start-date 2024-01-01 --end-date 2024-01-31
```
    """,
)
def medallion_pipeline():

    # -----------------------------------------------------------------------
    # Task 1: Ingest raw data to Bronze
    # -----------------------------------------------------------------------
    @task(task_id="ingest_bronze_trips", retries=3)
    def ingest_bronze_trips(**context) -> dict:
        """
        Read NYC Taxi source data and write to Bronze Delta layer.
        Returns metadata dict pushed to XCom for downstream tasks.
        """
        from bronze.bronze_loader import load_nyc_taxi_to_bronze
        from utils.spark_session import get_spark_session

        execution_date = context["ds"]  # YYYY-MM-DD
        source_path = f"s3://your-bucket/raw/nyc_taxi/{execution_date}/"

        spark = get_spark_session("Medallion-Bronze-Ingest")
        try:
            result = load_nyc_taxi_to_bronze(spark, source_path, execution_date)
            logger.info(f"Bronze ingest complete: {result}")
            return result
        finally:
            spark.stop()

    # -----------------------------------------------------------------------
    # Task 2: Validate Bronze
    # -----------------------------------------------------------------------
    @task(task_id="validate_bronze")
    def validate_bronze(bronze_meta: dict, **context) -> bool:
        """
        Run Great Expectations suite against Bronze trips.
        Raises ValueError on critical expectation failures to halt the DAG.
        """
        import great_expectations as ge

        execution_date = context["ds"]
        logger.info(f"Running Bronze validation for {execution_date}")

        context_ge = ge.get_context()
        result = context_ge.run_checkpoint(
            checkpoint_name="bronze_trips_checkpoint",
            batch_request={
                "datasource_name": "delta_datasource",
                "data_asset_name": "bronze_trips",
                "options": {"partition": execution_date},
            },
        )

        if not result["success"]:
            failed = [k for k, v in result["run_results"].items() if not v["validation_result"]["success"]]
            raise ValueError(f"Bronze validation failed for expectations: {failed}")

        logger.info("Bronze validation passed.")
        return True

    # -----------------------------------------------------------------------
    # Task 3: Transform Bronze → Silver
    # -----------------------------------------------------------------------
    @task(task_id="transform_silver_trips")
    def transform_silver_trips(validation_passed: bool, **context) -> dict:
        """
        Apply PySpark transformations: cast, deduplicate, flag DQ issues, upsert Silver.
        """
        from silver.silver_transformer import transform_trips_to_silver
        from utils.spark_session import get_spark_session

        execution_date = context["ds"]
        spark = get_spark_session("Medallion-Silver-Transform")
        try:
            result = transform_trips_to_silver(spark, execution_date)
            logger.info(f"Silver transform complete: {result}")
            return result
        finally:
            spark.stop()

    # -----------------------------------------------------------------------
    # Task 4: Validate Silver
    # -----------------------------------------------------------------------
    @task(task_id="validate_silver")
    def validate_silver(silver_meta: dict, **context) -> bool:
        """Run Great Expectations suite against Silver trips."""
        import great_expectations as ge

        execution_date = context["ds"]
        context_ge = ge.get_context()
        result = context_ge.run_checkpoint(
            checkpoint_name="silver_trips_checkpoint",
            batch_request={
                "datasource_name": "delta_datasource",
                "data_asset_name": "silver_trips",
                "options": {"partition": execution_date},
            },
        )

        if not result["success"]:
            raise ValueError(f"Silver validation failed for {execution_date}")

        logger.info("Silver validation passed.")
        return True

    # -----------------------------------------------------------------------
    # Task 5: Run dbt (Silver → Gold)
    # -----------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="run_dbt_gold",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --select marts --vars "
            "'{\"start_date\": \"{{ ds }}\", \"end_date\": \"{{ ds }}\"}' "
            "--profiles-dir /opt/airflow/dbt_project && "
            "dbt test --select marts"
        ),
        retries=1,
    )

    # -----------------------------------------------------------------------
    # Task 6: Validate Gold
    # -----------------------------------------------------------------------
    @task(task_id="validate_gold")
    def validate_gold(**context) -> None:
        """
        Lightweight row count sanity check on Gold layer.
        Catches cases where dbt ran successfully but produced 0 rows.
        """
        from utils.spark_session import get_spark_session

        execution_date = context["ds"]
        spark = get_spark_session("Medallion-Gold-Validate")
        try:
            gold_count = (
                spark.read.format("delta")
                .load("data/gold/fact_trips")
                .filter(f"pickup_date = '{execution_date}'")
                .count()
            )
            if gold_count == 0:
                raise ValueError(f"Gold fact_trips has 0 rows for {execution_date}. Check dbt run.")
            logger.info(f"Gold validation passed: {gold_count:,} rows for {execution_date}")
        finally:
            spark.stop()

    # -----------------------------------------------------------------------
    # Task 7: Notify on success
    # -----------------------------------------------------------------------
    @task(task_id="notify_success")
    def notify_success(**context) -> None:
        """Log pipeline completion. Extend to Slack/PagerDuty as needed."""
        execution_date = context["ds"]
        logger.info(f"✅ Medallion pipeline completed successfully for {execution_date}")

    # -----------------------------------------------------------------------
    # Wire up the task graph
    # -----------------------------------------------------------------------
    bronze_meta = ingest_bronze_trips()
    bronze_valid = validate_bronze(bronze_meta)
    silver_meta = transform_silver_trips(bronze_valid)
    silver_valid = validate_silver(silver_meta)
    silver_valid >> dbt_run >> validate_gold() >> notify_success()


# Instantiate the DAG
medallion_pipeline_dag = medallion_pipeline()
