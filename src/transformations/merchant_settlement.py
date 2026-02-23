"""Merchant daily settlement: net = captured - refunded - chargebacks per merchant."""

import os
import sys
import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    when,
    lit,
    current_timestamp,
    round as spark_round,
    to_date,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("merchant_settlement")

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
        .appName("BatchJob_MerchantDailySettlement")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run() -> None:
    logger.info("=" * 60)
    logger.info("Starting Merchant Daily Settlement batch job")
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
        logger.warning("No events found, skipping settlement calculation")
        spark.stop()
        return

    events = raw_df.withColumn("settlement_date", to_date(col("event_timestamp")))

    # aggregate by merchant + date, each event type maps to a different metric
    settlement_df = (
        events
        .groupBy("settlement_date", "merchant_id", "merchant_name", "merchant_country")
        .agg(
            # amounts by event type (all in EUR)
            spark_round(
                spark_sum(when(col("event_type") == "AUTHORIZATION", col("amount_eur")).otherwise(0)), 2
            ).alias("total_authorized"),

            spark_round(
                spark_sum(when(col("event_type") == "CAPTURE", col("amount_eur")).otherwise(0)), 2
            ).alias("total_captured"),

            spark_round(
                spark_sum(when(col("event_type") == "REFUND", col("amount_eur")).otherwise(0)), 2
            ).alias("total_refunded"),

            spark_round(
                spark_sum(when(col("event_type") == "CHARGEBACK", col("amount_eur")).otherwise(0)), 2
            ).alias("total_chargebacks"),

            count(when(col("event_type") == "AUTHORIZATION", True)).alias("transaction_count"),
            count(when(col("event_type") == "CHARGEBACK", True)).alias("chargeback_count"),
        )
    )

    # net settlement = what the PSP owes the merchant after deductions
    result_df = (
        settlement_df
        .withColumn(
            "net_settlement_eur",
            spark_round(
                col("total_captured") - col("total_refunded") - col("total_chargebacks"), 2
            ),
        )
        .withColumn(
            "chargeback_ratio",
            spark_round(
                when(col("transaction_count") > 0,
                     col("chargeback_count").cast("double") / col("transaction_count"))
                .otherwise(0), 4
            ),
        )
        .withColumn("currency", lit("EUR"))
        .withColumn("computed_at", current_timestamp())
    )

    output_df = result_df.select(
        "settlement_date", "merchant_id", "merchant_name", "merchant_country",
        "total_authorized", "total_captured", "total_refunded", "total_chargebacks",
        "net_settlement_eur", "transaction_count", "chargeback_count",
        "chargeback_ratio", "currency", "computed_at",
    )

    row_count = output_df.count()
    logger.info(f"Computed settlement for {row_count} merchant-date combinations")

    output_df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "analytics.merchant_daily_settlement") \
        .options(**JDBC_PROPS) \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

    logger.info(f"Wrote {row_count} rows to analytics.merchant_daily_settlement")
    spark.stop()
    logger.info("Merchant settlement job completed successfully")


if __name__ == "__main__":
    run()
