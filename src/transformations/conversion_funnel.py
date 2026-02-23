"""Conversion funnel - tracks auth/capture/settlement rates per merchant."""

import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    when,
    lit,
    current_timestamp,
    round as spark_round,
    to_date,
    min as spark_min,
    avg,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("conversion_funnel")

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


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BatchJob_ConversionFunnel")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run() -> None:
    logger.info("=" * 60)
    logger.info("Starting Conversion Funnel batch job")
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

    event_count = raw_df.count()
    logger.info(f"Read {event_count} events from raw.payment_events")

    if event_count == 0:
        logger.warning("No events found, skipping conversion funnel")
        spark.stop()
        return

    # use the auth date as the report date for each transaction
    auth_dates = (
        raw_df
        .filter(col("event_type") == "AUTHORIZATION")
        .select(
            col("psp_reference"),
            col("merchant_id"),
            to_date(col("event_timestamp")).alias("report_date"),
            col("event_timestamp").alias("auth_timestamp"),
        )
    )

    # count how far each txn progressed through the lifecycle
    lifecycle_counts = (
        raw_df
        .groupBy("psp_reference")
        .agg(
            count(when(col("event_type") == "AUTHORIZATION", True)).alias("has_auth"),
            count(when(col("event_type") == "CAPTURE", True)).alias("has_capture"),
            count(when(col("event_type") == "SETTLEMENT", True)).alias("has_settlement"),
            count(when(col("event_type") == "REFUND", True)).alias("has_refund"),
        )
    )

    txn_funnel = auth_dates.join(lifecycle_counts, on="psp_reference", how="inner")

    # grab capture timestamps so we can measure auth-to-capture latency
    capture_times = (
        raw_df
        .filter(col("event_type") == "CAPTURE")
        .select(
            col("psp_reference"),
            col("event_timestamp").alias("capture_timestamp"),
        )
    )

    txn_with_times = txn_funnel.join(capture_times, on="psp_reference", how="left")

    txn_with_times = txn_with_times.withColumn(
        "auth_to_capture_hours",
        when(
            col("capture_timestamp").isNotNull(),
            (col("capture_timestamp").cast("long") - col("auth_timestamp").cast("long")) / 3600.0,
        ),
    )

    funnel_df = (
        txn_with_times
        .groupBy("report_date", "merchant_id")
        .agg(
            count("*").alias("authorized_count"),
            spark_sum(when(col("has_capture") > 0, 1).otherwise(0)).alias("captured_count"),
            spark_sum(when(col("has_settlement") > 0, 1).otherwise(0)).alias("settled_count"),
            spark_sum(when(col("has_refund") > 0, 1).otherwise(0)).alias("refunded_count"),
            spark_round(avg(col("auth_to_capture_hours")), 2).alias("avg_auth_to_capture_hours"),
        )
    )

    result_df = (
        funnel_df
        .withColumn(
            "auth_to_capture_rate",
            spark_round(
                when(col("authorized_count") > 0,
                     col("captured_count").cast("double") / col("authorized_count"))
                .otherwise(0), 4
            ),
        )
        .withColumn(
            "capture_to_settle_rate",
            spark_round(
                when(col("captured_count") > 0,
                     col("settled_count").cast("double") / col("captured_count"))
                .otherwise(0), 4
            ),
        )
        .withColumn(
            "refund_rate",
            spark_round(
                when(col("captured_count") > 0,
                     col("refunded_count").cast("double") / col("captured_count"))
                .otherwise(0), 4
            ),
        )
        .withColumn("computed_at", current_timestamp())
    )

    output_df = result_df.select(
        "report_date", "merchant_id",
        "authorized_count", "captured_count", "settled_count", "refunded_count",
        "auth_to_capture_rate", "capture_to_settle_rate", "refund_rate",
        "avg_auth_to_capture_hours", "computed_at",
    )

    row_count = output_df.count()
    logger.info(f"Computed funnel for {row_count} merchant-date combinations")

    output_df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "analytics.conversion_funnel") \
        .options(**JDBC_PROPS) \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

    logger.info(f"Wrote {row_count} rows to analytics.conversion_funnel")
    spark.stop()
    logger.info("Conversion funnel job completed successfully")


if __name__ == "__main__":
    run()
