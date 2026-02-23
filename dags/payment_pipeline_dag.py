"""
Payment Analytics Pipeline DAG.

Runs four Spark batch jobs + quality checks daily.
Spark jobs are executed inside the spark-streaming container via docker exec.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# spark-submit via docker exec into the spark container
SPARK_SUBMIT_CMD = (
    "docker exec spark-streaming "
    "/opt/spark/bin/spark-submit "
    "--master local[*] "
    "--driver-memory 512m "
    "--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC "
    "{script}"
)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}


def check_raw_data_freshness(**context):
    """Fail the task if the most recent event is older than 6h (stale data guard)."""
    hook = PostgresHook(postgres_conn_id="payments_db")
    result = hook.get_first(
        "SELECT MAX(event_timestamp) FROM raw.payment_events;"
    )

    if result is None or result[0] is None:
        raise ValueError("No data in raw.payment_events, pipeline cannot proceed")

    max_ts = result[0]
    now = datetime.utcnow()
    age_hours = (now - max_ts).total_seconds() / 3600

    if age_hours > 6:
        raise ValueError(
            f"Raw data is {age_hours:.1f}h old (SLA: 6h). "
            f"Latest event: {max_ts}. Check streaming job."
        )

    print(f"Raw data freshness OK: latest event is {age_hours:.1f}h old")


def create_postgres_connection(**context):
    """Ensure the 'payments_db' Airflow connection exists (idempotent)."""
    from airflow.models import Connection
    from airflow.utils.session import create_session

    conn_id = "payments_db"

    with create_session() as session:
        existing = (
            session.query(Connection)
            .filter(Connection.conn_id == conn_id)
            .first()
        )

        if existing is None:
            conn = Connection(
                conn_id=conn_id,
                conn_type="postgres",
                host="postgres",
                schema="payments",
                login="payments_user",
                password="payments_pass",
                port=5432,
            )
            session.add(conn)
            print(f"Created Airflow connection: {conn_id}")
        else:
            print(f"Airflow connection '{conn_id}' already exists")


with DAG(
    dag_id="payment_analytics_pipeline",
    default_args=default_args,
    description="Daily payment analytics: transformations + quality checks",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["payments", "analytics", "adyen"],
    doc_md=__doc__,
) as dag:

    setup_connection = PythonOperator(
        task_id="setup_postgres_connection",
        python_callable=create_postgres_connection,
    )

    check_freshness = PythonOperator(
        task_id="check_raw_data_freshness",
        python_callable=check_raw_data_freshness,
    )

    run_merchant_settlement = BashOperator(
        task_id="merchant_settlement",
        bash_command=SPARK_SUBMIT_CMD.format(
            script="/opt/spark/app/src/transformations/merchant_settlement.py"
        ),
    )

    quality_check_settlement = BashOperator(
        task_id="quality_check_settlement",
        bash_command=SPARK_SUBMIT_CMD.format(
            script="/opt/spark/app/src/quality/quality_runner.py analytics.merchant_daily_settlement"
        ),
    )

    run_payment_performance = BashOperator(
        task_id="payment_method_performance",
        bash_command=SPARK_SUBMIT_CMD.format(
            script="/opt/spark/app/src/transformations/payment_method_performance.py"
        ),
    )

    quality_check_performance = BashOperator(
        task_id="quality_check_performance",
        bash_command=SPARK_SUBMIT_CMD.format(
            script="/opt/spark/app/src/quality/quality_runner.py analytics.payment_method_performance"
        ),
    )

    run_chargeback_monitoring = BashOperator(
        task_id="chargeback_monitoring",
        bash_command=SPARK_SUBMIT_CMD.format(
            script="/opt/spark/app/src/transformations/chargeback_monitoring.py"
        ),
    )

    quality_check_chargebacks = BashOperator(
        task_id="quality_check_chargebacks",
        bash_command=(
            "docker exec spark-streaming "
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            "--driver-memory 512m "
            "--conf spark.driver.extraJavaOptions=-Duser.timezone=UTC "
            "/opt/spark/app/src/quality/quality_runner.py raw.payment_events"
        ),
    )

    run_conversion_funnel = BashOperator(
        task_id="conversion_funnel",
        bash_command=SPARK_SUBMIT_CMD.format(
            script="/opt/spark/app/src/transformations/conversion_funnel.py"
        ),
    )

    quality_check_funnel = BashOperator(
        task_id="quality_check_funnel",
        bash_command=SPARK_SUBMIT_CMD.format(
            script="/opt/spark/app/src/quality/quality_runner.py analytics.conversion_funnel"
        ),
    )

    pipeline_summary = PostgresOperator(
        task_id="pipeline_summary",
        postgres_conn_id="payments_db",
        sql="""
            SELECT
                status,
                COUNT(*) as check_count,
                SUM(CASE WHEN severity = 'critical' AND status = 'FAIL' THEN 1 ELSE 0 END) as critical_failures
            FROM analytics.data_quality_log
            WHERE check_timestamp >= NOW() - INTERVAL '1 hour'
            GROUP BY status
            ORDER BY status;
        """,
    )

    # setup -> freshness -> parallel transforms -> quality checks -> summary
    setup_connection >> check_freshness

    check_freshness >> run_merchant_settlement >> quality_check_settlement
    check_freshness >> run_payment_performance >> quality_check_performance
    check_freshness >> run_chargeback_monitoring >> quality_check_chargebacks
    check_freshness >> run_conversion_funnel >> quality_check_funnel

    [
        quality_check_settlement,
        quality_check_performance,
        quality_check_chargebacks,
        quality_check_funnel,
    ] >> pipeline_summary
