"""Chargeback monitoring - flags merchants exceeding card scheme thresholds."""

import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    lit,
    current_timestamp,
    round as spark_round,
    to_date,
    max as spark_max,
    datediff,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("chargeback_monitoring")

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

# card scheme thresholds
VISA_THRESHOLD = 0.009       # 0.9%
MC_THRESHOLD = 0.010         # 1.0%
WARNING_THRESHOLD = 0.005    # 0.5%, getting close


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BatchJob_ChargebackMonitoring")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run() -> None:
    logger.info("=" * 60)
    logger.info("Starting Chargeback Monitoring batch job")
    logger.info("=" * 60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "raw.payment_events")
        .options(**JDBC_PROPS)
        .load()
    )

    max_date_row = raw_df.select(spark_max(to_date(col("event_timestamp"))).alias("max_date")).collect()
    if not max_date_row or max_date_row[0]["max_date"] is None:
        logger.warning("No events found, skipping chargeback monitoring")
        spark.stop()
        return

    alert_date = max_date_row[0]["max_date"]
    logger.info(f"Alert date (latest data date): {alert_date}")

    relevant_events = raw_df.filter(
        col("event_type").isin("AUTHORIZATION", "CHARGEBACK")
    )

    merchant_stats = (
        relevant_events
        .groupBy("merchant_id", "merchant_name")
        .agg(
            count(when(col("event_type") == "AUTHORIZATION", True)).alias("transaction_count"),
            count(when(col("event_type") == "CHARGEBACK", True)).alias("chargeback_count"),
        )
    )

    with_ratio = merchant_stats.withColumn(
        "chargeback_ratio",
        spark_round(
            when(col("transaction_count") > 0,
                 col("chargeback_count").cast("double") / col("transaction_count"))
            .otherwise(0), 4
        ),
    )

    with_threshold = (
        with_ratio
        .withColumn(
            "threshold_exceeded",
            when(
                (col("chargeback_ratio") > VISA_THRESHOLD) & (col("chargeback_ratio") > MC_THRESHOLD),
                lit("both"),
            )
            .when(col("chargeback_ratio") > VISA_THRESHOLD, lit("visa"))
            .when(col("chargeback_ratio") > MC_THRESHOLD, lit("mastercard"))
            .otherwise(lit(None)),
        )
    )

    with_severity = (
        with_threshold
        .withColumn(
            "severity",
            when(col("threshold_exceeded").isNotNull(), lit("critical"))
            .when(col("chargeback_ratio") > WARNING_THRESHOLD, lit("warning"))
            .otherwise(lit(None)),
        )
    )

    alerts_df = with_severity.filter(col("severity").isNotNull())

    result_df = (
        alerts_df
        .withColumn("alert_date", lit(alert_date))
        .withColumn("visa_threshold", lit(VISA_THRESHOLD))
        .withColumn("mc_threshold", lit(MC_THRESHOLD))
        .withColumn("computed_at", current_timestamp())
    )

    output_df = result_df.select(
        "alert_date", "merchant_id", "merchant_name",
        "chargeback_count", "transaction_count", "chargeback_ratio",
        "threshold_exceeded", "visa_threshold", "mc_threshold",
        "severity", "computed_at",
    )

    alert_count = output_df.count()
    logger.info(f"Generated {alert_count} chargeback alerts")

    output_df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "analytics.chargeback_alerts") \
        .options(**JDBC_PROPS) \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

    logger.info(f"Wrote {alert_count} rows to analytics.chargeback_alerts")
    spark.stop()
    logger.info("Chargeback monitoring job completed successfully")


if __name__ == "__main__":
    run()
