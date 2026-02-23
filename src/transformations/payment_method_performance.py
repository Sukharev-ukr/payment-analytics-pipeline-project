"""Payment method performance - auth rates and volume per method and market."""

import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    avg,
    when,
    lit,
    current_timestamp,
    round as spark_round,
    to_date,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("payment_method_performance")

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
        .appName("BatchJob_PaymentMethodPerformance")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run() -> None:
    logger.info("=" * 60)
    logger.info("Starting Payment Method Performance batch job")
    logger.info("=" * 60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "(SELECT * FROM raw.payment_events WHERE event_type = 'AUTHORIZATION') AS auths")
        .options(**JDBC_PROPS)
        .load()
    )

    event_count = raw_df.count()
    logger.info(f"Read {event_count} AUTHORIZATION events")

    if event_count == 0:
        logger.warning("No authorization events found, skipping")
        spark.stop()
        return

    events = raw_df.withColumn("report_date", to_date(col("event_timestamp")))

    perf_df = (
        events
        .groupBy("report_date", "payment_method", "issuer_country")
        .agg(
            count("*").alias("total_authorizations"),
            count(when(col("acquirer_response_code") == "00", True)).alias("successful_authorizations"),

            spark_round(avg(col("amount_eur")), 2).alias("avg_amount_eur"),
            spark_round(spark_sum(col("amount_eur")), 2).alias("total_volume_eur"),

            spark_round(
                spark_sum(when(col("is_3ds_authenticated") == True, 1).otherwise(0)).cast("double")
                / count("*"), 4
            ).alias("is_3ds_rate"),
        )
    )

    result_df = (
        perf_df
        .withColumn(
            "authorization_rate",
            spark_round(
                col("successful_authorizations").cast("double") / col("total_authorizations"), 4
            ),
        )
        .withColumn("computed_at", current_timestamp())
    )

    output_df = result_df.select(
        "report_date", "payment_method", "issuer_country",
        "total_authorizations", "successful_authorizations", "authorization_rate",
        "avg_amount_eur", "total_volume_eur", "is_3ds_rate", "computed_at",
    )

    row_count = output_df.count()
    logger.info(f"Computed performance for {row_count} method-country-date combinations")

    output_df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "analytics.payment_method_performance") \
        .options(**JDBC_PROPS) \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

    logger.info(f"Wrote {row_count} rows to analytics.payment_method_performance")
    spark.stop()
    logger.info("Payment method performance job completed successfully")


if __name__ == "__main__":
    run()
