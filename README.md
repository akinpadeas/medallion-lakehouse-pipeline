# Medallion Lakehouse Pipeline

A production-grade **Bronze → Silver → Gold** data lakehouse pipeline built with PySpark, Delta Lake, dbt, and Apache Airflow. Demonstrates end-to-end data engineering across ingestion, transformation, data modeling, quality validation, and orchestration.

---

## Architecture overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│   NYC Taxi (Parquet)  ·  Weather API (JSON)  ·  Zones CSV           │
└────────────────────┬────────────────────────────────────────────────┘
                     │ Raw ingestion (PySpark)
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (Delta Lake)                                          │
│  • Raw data landed as-is, schema-on-read                            │
│  • Full audit trail: ingestion_ts, source_file, batch_id            │
│  • Append-only, immutable historical record                          │
└────────────────────┬────────────────────────────────────────────────┘
                     │ Cleanse + deduplicate (PySpark)
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (Delta Lake)                                          │
│  • Validated, typed, deduplicated records                            │
│  • SCD Type 2 for slowly changing dimensions                         │
│  • Business keys enforced, nulls handled, outliers flagged           │
└────────────────────┬────────────────────────────────────────────────┘
                     │ Model + aggregate (dbt)
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (Delta Lake / dbt marts)                                │
│  • Kimball star schema: fact_trips + dim_date, dim_zone, dim_vendor  │
│  • Pre-aggregated metrics: daily_zone_summary, hourly_demand         │
│  • Analytics-ready: BI tools, ML features, reporting                 │
└─────────────────────────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  DATA QUALITY  (Great Expectations)                                   │
│  • Expectation suites at Bronze, Silver, and Gold layers             │
│  • Automated validation on every pipeline run                         │
│  • Data docs generated to /docs/ge_data_docs/                        │
└─────────────────────────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  ORCHESTRATION  (Apache Airflow)                                      │
│  • DAG: medallion_pipeline_dag (daily schedule)                       │
│  • Task dependencies enforced; failure alerts via email               │
│  • Backfill support for historical loads                              │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Key engineering decisions

| Decision | Approach | Rationale |
|---|---|---|
| Storage format | Delta Lake | ACID transactions, schema evolution, time travel |
| Transformation engine | PySpark (Bronze→Silver) + dbt (Silver→Gold) | PySpark for heavy lifting; dbt for modular, testable SQL |
| Slowly Changing Dimensions | SCD Type 2 | Full history preservation for zone and vendor dimensions |
| Data quality | Great Expectations | Declarative, version-controlled expectation suites |
| Orchestration | Airflow with TaskFlow API | Clear task dependencies; easy backfill and retry |
| Data model | Kimball star schema | Optimized for analytical query patterns |

---

## Project structure

```
medallion-lakehouse-pipeline/
│
├── ingestion/                  # Source connectors and raw file readers
│   ├── nyc_taxi_reader.py      # Reads Parquet files from source
│   ├── weather_api_client.py   # REST ingestor for weather data
│   └── schema_registry.py      # Source schema definitions
│
├── bronze/                     # Bronze layer: raw landing
│   ├── bronze_loader.py        # Writes raw data to Delta with audit cols
│   └── bronze_schema.py        # Bronze table schemas
│
├── silver/                     # Silver layer: cleanse + conform
│   ├── silver_transformer.py   # PySpark transforms: clean, dedupe, cast
│   ├── scd2_handler.py         # SCD Type 2 merge logic
│   └── silver_schema.py        # Silver table schemas
│
├── gold/                       # Gold layer: business-ready models
│   └── (managed by dbt_project/)
│
├── dbt_project/                # dbt models for Gold layer
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── models/
│   │   ├── staging/            # 1:1 with Silver tables, light casting
│   │   │   ├── stg_trips.sql
│   │   │   ├── stg_zones.sql
│   │   │   └── stg_vendors.sql
│   │   ├── intermediate/       # Business logic, joins
│   │   │   └── int_trips_enriched.sql
│   │   └── marts/              # Star schema final layer
│   │       ├── fact_trips.sql
│   │       ├── dim_date.sql
│   │       ├── dim_zone.sql
│   │       ├── dim_vendor.sql
│   │       └── agg_daily_zone_summary.sql
│   ├── tests/                  # dbt singular + generic tests
│   └── macros/                 # Reusable SQL macros
│
├── orchestration/
│   └── dags/
│       └── medallion_pipeline_dag.py   # Airflow DAG definition
│
├── quality/
│   └── expectations/
│       ├── bronze_suite.json   # GE expectations for Bronze
│       ├── silver_suite.json   # GE expectations for Silver
│       └── gold_suite.json     # GE expectations for Gold
│
├── utils/
│   ├── spark_session.py        # Shared SparkSession factory
│   ├── delta_utils.py          # Delta Lake helpers (optimize, vacuum)
│   └── logging_config.py       # Structured logging setup
│
├── tests/
│   ├── unit/                   # Unit tests for transform logic
│   └── integration/            # End-to-end pipeline tests
│
├── docs/
│   └── architecture.md         # Detailed design decisions
│
├── .github/
│   └── workflows/
│       ├── ci.yml              # Run tests + dbt compile on every PR
│       └── dbt_docs.yml        # Auto-publish dbt docs to GitHub Pages
│
├── requirements.txt
├── docker-compose.yml          # Local Spark + Airflow environment
└── Makefile                    # Common dev commands
```

---

## Data model (Gold layer)

```
dim_date ──────────────────────┐
dim_zone ──────────────────────┤
dim_vendor ────────────────────┼──► fact_trips ──► agg_daily_zone_summary
                               │       │
                               └───────┘
```

**`fact_trips`** — one row per taxi trip
- `trip_sk` (surrogate key), `vendor_fk`, `pickup_zone_fk`, `dropoff_zone_fk`, `date_fk`
- `trip_distance`, `fare_amount`, `tip_amount`, `total_amount`, `passenger_count`, `duration_minutes`

**`dim_zone`** — NYC taxi zone dimension (SCD Type 2)
- `zone_sk`, `zone_id`, `zone_name`, `borough`, `service_zone`
- `valid_from`, `valid_to`, `is_current`

---

## Quickstart

### Prerequisites
- Docker & Docker Compose
- Python 3.10+
- Java 11 (for local Spark)

### 1. Clone and install

```bash
git clone https://github.com/akinpadeas/medallion-lakehouse-pipeline.git
cd medallion-lakehouse-pipeline
pip install -r requirements.txt
```

### 2. Start local environment

```bash
# Spin up local Spark + Airflow
make up

# Verify Airflow UI at http://localhost:8080 (admin/admin)
```

### 3. Run Bronze ingestion

```bash
python -m ingestion.nyc_taxi_reader --date 2024-01-01
python -m bronze.bronze_loader --source nyc_taxi --date 2024-01-01
```

### 4. Run Silver transformation

```bash
python -m silver.silver_transformer --layer bronze --table trips --date 2024-01-01
```

### 5. Run Gold (dbt)

```bash
cd dbt_project
dbt deps
dbt run --select marts
dbt test
dbt docs generate && dbt docs serve
```

### 6. Run full pipeline via Airflow

Trigger the `medallion_pipeline_dag` DAG from the Airflow UI or CLI:

```bash
airflow dags trigger medallion_pipeline_dag --conf '{"execution_date": "2024-01-01"}'
```

### 7. Validate data quality

```bash
python -m quality.run_expectations --suite bronze_suite --table bronze.trips
```

---

## CI/CD

Every pull request triggers the GitHub Actions workflow:

1. **Lint** — `ruff` (Python) + `sqlfluff` (SQL)
2. **Unit tests** — `pytest tests/unit/`
3. **dbt compile** — validates all models parse without errors
4. **dbt test** — runs schema + data tests against a test dataset

On merge to `main`, dbt docs are automatically published to GitHub Pages.

---

## Skills demonstrated

- Medallion / multi-hop architecture (Bronze → Silver → Gold)
- Delta Lake: ACID writes, schema evolution, time travel, OPTIMIZE + ZORDER
- SCD Type 2 merge logic in PySpark
- Kimball dimensional modeling (star schema, surrogate keys, conformed dims)
- dbt: staging → intermediate → mart layering, generic + singular tests, macros
- Data quality with Great Expectations (expectation suites, data docs)
- Apache Airflow: TaskFlow API, task dependencies, backfill, retries
- CI/CD with GitHub Actions: lint, test, dbt compile, auto-deploy docs
- Structured logging and pipeline observability

---

## Dataset

Uses the publicly available [NYC Taxi & Limousine Commission Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — a real-world dataset with millions of rows, ideal for demonstrating scalable pipeline design.

---

## License

MIT
