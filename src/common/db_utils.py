"""Shared JDBC helpers for PySpark <-> PostgreSQL. Config comes from env vars."""

import os


def get_jdbc_url() -> str:
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "payments")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def get_jdbc_properties() -> dict:
    return {
        "user": os.getenv("POSTGRES_USER", "payments_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "payments_pass"),
        "driver": "org.postgresql.Driver",
    }


def write_to_postgres(df, table_name: str, mode: str = "append") -> None:
    df.write \
        .format("jdbc") \
        .option("url", get_jdbc_url()) \
        .option("dbtable", table_name) \
        .options(**get_jdbc_properties()) \
        .mode(mode) \
        .save()


def read_from_postgres(spark, table_name: str):
    return spark.read \
        .format("jdbc") \
        .option("url", get_jdbc_url()) \
        .option("dbtable", table_name) \
        .options(**get_jdbc_properties()) \
        .load()
