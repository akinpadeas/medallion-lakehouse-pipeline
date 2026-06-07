.PHONY: up down test lint dbt-run dbt-test dbt-docs clean

## ─── Local environment ───────────────────────────────────────────────────────

up:
	docker compose up -d
	@echo "Airflow UI: http://localhost:8080 (admin/admin)"
	@echo "Spark UI:   http://localhost:4040"

down:
	docker compose down

## ─── Testing ─────────────────────────────────────────────────────────────────

test:
	pytest tests/unit/ -v --tb=short --cov=. --cov-report=term-missing

test-integration:
	pytest tests/integration/ -v --tb=short

## ─── Linting ─────────────────────────────────────────────────────────────────

lint:
	ruff check .
	sqlfluff lint dbt_project/models --dialect sparksql

lint-fix:
	ruff check . --fix
	sqlfluff fix dbt_project/models --dialect sparksql

## ─── Pipeline (manual run) ───────────────────────────────────────────────────

bronze DATE?=$(shell date +%Y-%m-%d):
	python -m bronze.bronze_loader --source data/raw/trips/$(DATE)/ --date $(DATE)

silver DATE?=$(shell date +%Y-%m-%d):
	python -m silver.silver_transformer --date $(DATE)

## ─── dbt ─────────────────────────────────────────────────────────────────────

dbt-run:
	cd dbt_project && dbt run --select marts

dbt-test:
	cd dbt_project && dbt test --select marts

dbt-docs:
	cd dbt_project && dbt docs generate && dbt docs serve

## ─── Housekeeping ────────────────────────────────────────────────────────────

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf target/ dbt_packages/
