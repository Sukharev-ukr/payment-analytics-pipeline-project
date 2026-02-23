from __future__ import annotations

"""
Quality runner - runs data quality checks and logs results to Postgres.

Usage:
    spark-submit quality_runner.py                    # check all tables
    spark-submit quality_runner.py raw.payment_events # check one table

Exit code 1 on critical failures so Airflow marks the task as failed.
"""

import os
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
)
from pyspark.sql.functions import current_timestamp

# add app root so our modules are importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.quality.validators import DataValidator, CheckResult

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("quality_runner")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "payments")
POSTGRES_USER = os.getenv("POSTGRES_USER", "payments_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "payments_pass")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# Declarative check config: add a new dict here to add a check, no code changes needed.
# severity "critical" blocks the pipeline, "warning" just logs.
QUALITY_CHECKS = {
    "raw.payment_events": [
        {
            "check": "not_null",
            "columns": ["event_id", "psp_reference", "amount", "currency", "event_type", "merchant_id"],
            "severity": "critical",
        },
        {
            "check": "accepted_values",
            "column": "currency",
            "values": ["EUR", "USD", "GBP", "SEK", "DKK", "NOK", "CHF", "PLN"],
            "severity": "critical",
        },
        {
            "check": "accepted_values",
            "column": "event_type",
            "values": ["AUTHORIZATION", "CAPTURE", "SETTLEMENT", "REFUND", "CHARGEBACK", "CANCEL"],
            "severity": "critical",
        },
        {
            "check": "uniqueness",
            "column": "event_id",
            "severity": "critical",
        },
        {
            "check": "range_check",
            "column": "amount",
            "min": 0.01,
            "max": 999999.99,
            "severity": "warning",
        },
        {
            "check": "range_check",
            "column": "risk_score",
            "min": 0,
            "max": 100,
            "severity": "warning",
        },
        {
            "check": "freshness",
            "column": "event_timestamp",
            "max_age_hours": 2,
            "severity": "warning",
        },
        {
            "check": "statistical_outlier",
            "column": "amount",
            "n_stddev": 3,
            "severity": "warning",
        },
        {
            "check": "row_count",
            "min_rows": 1,
            "severity": "critical",
        },
    ],

    "analytics.merchant_daily_settlement": [
        {
            "check": "not_null",
            "columns": ["settlement_date", "merchant_id", "net_settlement_eur"],
            "severity": "critical",
        },
        {
            "check": "referential_integrity",
            "column": "merchant_id",
            "reference_table": "raw.payment_events",
            "reference_column": "merchant_id",
            "severity": "warning",
        },
        {
            "check": "row_count",
            "min_rows": 1,
            "severity": "critical",
        },
        {
            "check": "range_check",
            "column": "chargeback_ratio",
            "min": 0.0,
            "max": 1.0,
            "severity": "warning",
        },
    ],

    "analytics.payment_method_performance": [
        {
            "check": "not_null",
            "columns": ["report_date", "payment_method", "authorization_rate"],
            "severity": "critical",
        },
        {
            "check": "range_check",
            "column": "authorization_rate",
            "min": 0.0,
            "max": 1.0,
            "severity": "warning",
        },
        {
            "check": "row_count",
            "min_rows": 1,
            "severity": "critical",
        },
    ],

    "analytics.conversion_funnel": [
        {
            "check": "not_null",
            "columns": ["report_date", "merchant_id", "authorized_count"],
            "severity": "critical",
        },
        {
            "check": "range_check",
            "column": "auth_to_capture_rate",
            "min": 0.0,
            "max": 1.0,
            "severity": "warning",
        },
        {
            "check": "row_count",
            "min_rows": 1,
            "severity": "critical",
        },
    ],
}

QUALITY_LOG_SCHEMA = StructType([
    StructField("table_name", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("check_type", StringType(), False),
    StructField("status", StringType(), False),
    StructField("records_checked", IntegerType(), False),
    StructField("records_failed", IntegerType(), False),
    StructField("failure_percentage", DoubleType(), False),
    StructField("details", StringType(), True),
    StructField("severity", StringType(), False),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DataQualityRunner")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_table(spark: SparkSession, table_name: str):
    """Load a Postgres table into a PySpark DataFrame."""
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .options(**JDBC_PROPS)
        .load()
    )


def run_checks_for_table(
    spark: SparkSession,
    validator: DataValidator,
    table_name: str,
    checks: list[dict],
) -> list[CheckResult]:
    """Run all configured checks for a single table."""
    logger.info(f"  Loading table: {table_name}")
    df = load_table(spark, table_name)
    row_count = df.count()
    logger.info(f"  Table {table_name}: {row_count} rows")

    results = []
    for check_config in checks:
        config = {**check_config, "_table": table_name}
        check_type = config["check"]

        try:
            check_results = validator.run_check(df, config)
            results.extend(check_results)

            for r in check_results:
                icon = "✓" if r.status == "PASS" else ("⚠" if r.status == "WARN" else "✗")
                logger.info(f"    {icon} [{r.status}] {r.check_name}: {r.details}")
        except Exception as e:
            logger.error(f"    ✗ [ERROR] {check_type}: {str(e)}")
            results.append(CheckResult(
                table_name=table_name,
                check_name=f"{check_type}_error",
                check_type=check_type,
                status="FAIL",
                records_checked=0,
                records_failed=0,
                failure_percentage=0.0,
                details=f"Check failed with error: {str(e)}",
                severity=config.get("severity", "critical"),
            ))

    return results


def log_results_to_postgres(spark: SparkSession, results: list[CheckResult]) -> None:
    """Append check results to the quality log table."""
    if not results:
        return

    rows = [
        Row(
            table_name=r.table_name,
            check_name=r.check_name,
            check_type=r.check_type,
            status=r.status,
            records_checked=r.records_checked,
            records_failed=r.records_failed,
            failure_percentage=r.failure_percentage,
            details=r.details,
            severity=r.severity,
        )
        for r in results
    ]

    log_df = spark.createDataFrame(rows, schema=QUALITY_LOG_SCHEMA)
    log_df = log_df.withColumn("check_timestamp", current_timestamp())

    log_df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "analytics.data_quality_log") \
        .options(**JDBC_PROPS) \
        .mode("append") \
        .save()

    logger.info(f"Logged {len(results)} check results to analytics.data_quality_log")


def main() -> None:
    """Run quality checks for all tables (or a specific table if passed as arg)."""
    target_table = sys.argv[1] if len(sys.argv) > 1 else None

    logger.info("=" * 60)
    logger.info("Starting Data Quality Runner")
    if target_table:
        logger.info(f"  Target: {target_table}")
    else:
        logger.info("  Target: ALL tables")
    logger.info("=" * 60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    validator = DataValidator(spark)

    all_results: list[CheckResult] = []

    for table_name, checks in QUALITY_CHECKS.items():
        # Skip tables not matching the target (if specified)
        if target_table and table_name != target_table:
            continue

        logger.info(f"\n{'─' * 50}")
        logger.info(f"Checking: {table_name} ({len(checks)} checks)")
        logger.info(f"{'─' * 50}")

        results = run_checks_for_table(spark, validator, table_name, checks)
        all_results.extend(results)

    passed = sum(1 for r in all_results if r.status == "PASS")
    warned = sum(1 for r in all_results if r.status == "WARN")
    failed = sum(1 for r in all_results if r.status == "FAIL")
    critical_failures = sum(1 for r in all_results if r.is_critical_failure())

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Quality Check Summary: {passed} PASS | {warned} WARN | {failed} FAIL")
    if critical_failures > 0:
        logger.info(f"  *** {critical_failures} CRITICAL FAILURES ***")
    logger.info(f"{'=' * 60}")

    log_results_to_postgres(spark, all_results)

    spark.stop()

    if critical_failures > 0:
        logger.error(f"Exiting with code 1 due to {critical_failures} critical failures")
        sys.exit(1)

    logger.info("All quality checks passed (no critical failures)")


if __name__ == "__main__":
    main()
