# Payment Analytics Pipeline

.PHONY: start stop restart status logs \
        run-settlement run-performance run-chargebacks run-funnel \
        run-quality run-all-batch \
        psql clean help

# === Lifecycle ===

start:  ## Start the full pipeline (all services)
	docker-compose up -d

stop:  ## Stop all services
	docker-compose down

restart:  ## Restart all services
	docker-compose down && docker-compose up -d

status:  ## Show status of all containers
	docker-compose ps

logs:  ## Tail logs from all services
	docker-compose logs -f --tail=50

build:  ## Rebuild all custom images (Spark, Generator, Airflow)
	docker-compose build spark-streaming generator airflow-init airflow-webserver airflow-scheduler

# === Spark Batch Jobs ===

SPARK_EXEC = docker exec spark-streaming /opt/spark/bin/spark-submit \
	--master "local[*]" --driver-memory 512m \
	--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC

run-settlement:  ## Run merchant settlement transformation
	$(SPARK_EXEC) /opt/spark/app/src/transformations/merchant_settlement.py

run-performance:  ## Run payment method performance transformation
	$(SPARK_EXEC) /opt/spark/app/src/transformations/payment_method_performance.py

run-chargebacks:  ## Run chargeback monitoring transformation
	$(SPARK_EXEC) /opt/spark/app/src/transformations/chargeback_monitoring.py

run-funnel:  ## Run conversion funnel transformation
	$(SPARK_EXEC) /opt/spark/app/src/transformations/conversion_funnel.py

run-quality:  ## Run data quality checks on all tables
	$(SPARK_EXEC) /opt/spark/app/src/quality/quality_runner.py

run-all-batch: run-settlement run-performance run-chargebacks run-funnel run-quality  ## Run all batch jobs + quality checks

# === Database Access ===

psql:  ## Open psql shell to the payments database
	docker exec -it postgres psql -U payments_user -d payments

# === Airflow ===

airflow-trigger:  ## Trigger the Airflow DAG manually
	docker exec airflow-scheduler airflow dags trigger payment_analytics_pipeline

airflow-status:  ## Show status of latest DAG run
	docker exec airflow-scheduler airflow dags list-runs -d payment_analytics_pipeline --limit 5

# === Cleanup ===

clean:  ## Stop all services and remove volumes (DESTRUCTIVE)
	docker-compose down -v
	@echo "All data volumes removed."

# === Help ===

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
