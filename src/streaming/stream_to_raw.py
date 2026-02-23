"""Structured Streaming job: reads from Kafka, validates, normalizes currency, writes to Postgres."""

import os
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    IntegerType,
)
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    lit,
    current_timestamp,
    struct,
    to_json,
    concat_ws,
    array,
    array_except,
    round as spark_round,
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payment.events.raw")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "payments")
POSTGRES_USER = os.getenv("POSTGRES_USER", "payments_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "payments_pass")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoint/raw_streaming")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# explicit schema - never use inferSchema for financial data
PAYMENT_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("psp_reference", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_country", StringType(), True),
    StructField("merchant_category_code", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("original_amount", DoubleType(), True),
    StructField("original_currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_method_variant", StringType(), True),
    StructField("card_bin", StringType(), True),
    StructField("card_last_four", StringType(), True),
    StructField("issuer_country", StringType(), True),
    StructField("shopper_interaction", StringType(), True),
    StructField("shopper_reference", StringType(), True),
    StructField("acquirer_response_code", StringType(), True),
    StructField("is_3ds_authenticated", BooleanType(), True),
    StructField("risk_score", IntegerType(), True),
])

# if any of these are NULL the event gets rejected
REQUIRED_FIELDS = ["event_id", "psp_reference", "merchant_id", "amount", "currency", "event_type"]

# approximate FX rates to EUR
FX_RATES = {
    "EUR": 1.0,
    "USD": 0.92,
    "GBP": 1.17,
    "SEK": 0.088,
    "DKK": 0.134,
    "NOK": 0.089,
    "CHF": 1.05,
    "PLN": 0.23,
}

# allowed values for validation
VALID_CURRENCIES = list(FX_RATES.keys())
VALID_EVENT_TYPES = ["AUTHORIZATION", "CAPTURE", "SETTLEMENT", "REFUND", "CHARGEBACK", "CANCEL"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("stream_to_raw")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("PaymentStreaming_KafkaToPostgres")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def build_fx_column(amount_col: str, currency_col: str):
    """Chain of WHEN expressions to convert supported currencies to EUR."""
    expr = lit(None).cast(DoubleType())
    for currency, rate in FX_RATES.items():
        expr = when(col(currency_col) == currency, col(amount_col) * lit(rate)).otherwise(expr)
    return spark_round(expr, 2)


def validate_and_split(parsed_df: DataFrame) -> tuple:
    """Split parsed events into valid and rejected DataFrames based on validation rules."""
    rejection_conditions = []

    for field_name in REQUIRED_FIELDS:
        rejection_conditions.append(
            when(col(field_name).isNull(), lit(f"missing_{field_name}"))
        )

    rejection_conditions.append(
        when(
            col("currency").isNotNull() & ~col("currency").isin(VALID_CURRENCIES),
            lit("invalid_currency"),
        )
    )

    rejection_conditions.append(
        when(
            col("event_type").isNotNull() & ~col("event_type").isin(VALID_EVENT_TYPES),
            lit("invalid_event_type"),
        )
    )

    rejection_conditions.append(
        when(col("amount").isNotNull() & (col("amount") <= 0), lit("non_positive_amount"))
    )

    # combine all non-null rejection reasons into a single string
    reason_array = array(
        *[when(cond.isNotNull(), cond).otherwise(lit(None)) for cond in rejection_conditions]
    )
    rejection_reason = concat_ws(
        ", ",
        array_except(reason_array, array(lit(None).cast(StringType()))),
    )

    df_with_reasons = parsed_df.withColumn("_rejection_reason", rejection_reason)

    valid_df = (
        df_with_reasons
        .filter(col("_rejection_reason") == "")
        .drop("_rejection_reason")
    )

    # keep the raw payload on rejected events for debugging
    schema_fields = [col(f.name) for f in PAYMENT_EVENT_SCHEMA]
    rejected_df = (
        df_with_reasons
        .filter(col("_rejection_reason") != "")
        .select(
            col("event_id"),
            to_json(struct(*schema_fields)).alias("raw_payload"),
            col("_rejection_reason").alias("rejection_reason"),
        )
    )

    return valid_df, rejected_df


def write_batch_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    """Write a micro-batch to Postgres via staging table upsert (avoids duplicates)."""
    if batch_df.isEmpty():
        return

    count = batch_df.count()
    logger.info(f"Processing batch {batch_id}: {count} raw messages from Kafka")

    parsed_df = (
        batch_df
        .select(
            from_json(col("value").cast("string"), PAYMENT_EVENT_SCHEMA).alias("data"),
            col("value").cast("string").alias("_raw_value"),
        )
        .select("data.*", "_raw_value")
    )

    valid_df, rejected_df = validate_and_split(parsed_df.drop("_raw_value"))

    transformed_df = (
        valid_df
        .withColumn(
            "event_timestamp",
            to_timestamp(
                col("event_timestamp"),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            ),
        )
        .withColumn("amount_eur", build_fx_column("amount", "currency"))
        .withColumn("ingested_at", current_timestamp())
    )

    output_df = transformed_df.select(
        "event_id", "psp_reference", "merchant_id", "merchant_name",
        "merchant_country", "merchant_category_code", "event_type",
        "event_timestamp", "amount", "currency", "original_amount",
        "original_currency", "payment_method", "payment_method_variant",
        "card_bin", "card_last_four", "issuer_country", "shopper_interaction",
        "shopper_reference", "acquirer_response_code", "is_3ds_authenticated",
        "risk_score", "amount_eur", "ingested_at",
    )

    output_df = output_df.dropDuplicates(["event_id"])

    valid_count = output_df.count()
    if valid_count > 0:
        staging_table = "raw._staging_payment_events"

        output_df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", staging_table) \
            .options(**JDBC_PROPERTIES) \
            .mode("overwrite") \
            .save()

        _execute_sql(f"""
            INSERT INTO raw.payment_events
            SELECT * FROM {staging_table}
            ON CONFLICT (event_id) DO NOTHING
        """)

        _execute_sql(f"DROP TABLE IF EXISTS {staging_table}")

        logger.info(f"  Batch {batch_id}: upserted {valid_count} valid events -> raw.payment_events")

    rejected_count = rejected_df.count()
    if rejected_count > 0:
        (
            rejected_df
            .withColumn("rejected_at", current_timestamp())
            .write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", "raw.rejected_events")
            .options(**JDBC_PROPERTIES)
            .mode("append")
            .save()
        )
        logger.info(f"  Batch {batch_id}: wrote {rejected_count} rejected events -> raw.rejected_events")


def _execute_sql(sql: str) -> None:
    """Run raw SQL against Postgres through the JVM JDBC connection."""
    spark = SparkSession.getActiveSession()
    jvm = spark._jvm

    jvm.Class.forName("org.postgresql.Driver")

    conn = jvm.java.sql.DriverManager.getConnection(JDBC_URL, POSTGRES_USER, POSTGRES_PASSWORD)
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


def main() -> None:
    """Start the streaming pipeline: Kafka -> validate -> transform -> Postgres."""
    logger.info("=" * 60)
    logger.info("Starting PySpark Structured Streaming: Kafka -> PostgreSQL")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS} / topic: {KAFKA_TOPIC}")
    logger.info(f"  PostgreSQL: {JDBC_URL}")
    logger.info(f"  Checkpoint: {CHECKPOINT_DIR}")
    logger.info("=" * 60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 500)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        kafka_df
        .select(
            col("value").cast("string").alias("value"),
            col("timestamp").alias("kafka_timestamp"),
        )
    )

    query = (
        parsed_stream
        .writeStream
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
