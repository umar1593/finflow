"""DAG для ежедневного запуска FinFlow."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

DB_CONN = {
    "host": "postgres",
    "port": 5432,
    "user": "finflow",
    "password": "finflow123",
    "dbname": "finflow_db",
}

FRAUD_RATE_THRESHOLD = 10.0
DATA_LOOKBACK_HOURS = 25

PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt_project"
DBT_BIN = "/opt/dbt-venv/bin/dbt"
GX_PYTHON = "/opt/gx-venv/bin/python"

DB_ENV = {
    "DB_HOST": DB_CONN["host"],
    "DB_PORT": str(DB_CONN["port"]),
    "DB_USER": DB_CONN["user"],
    "DB_PASSWORD": DB_CONN["password"],
    "DB_NAME": DB_CONN["dbname"],
}

default_args = {
    "owner": "finflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="finflow_pipeline",
    description="Ежедневный ETL/ELT пайплайн FinFlow",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["finflow", "etl", "daily"],
)


def get_connection():
    import psycopg2
    return psycopg2.connect(**DB_CONN)


def check_data_availability(**context):
    """Проверяет, что за последние сутки есть данные."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM transactions
                WHERE created_at >= NOW() - make_interval(hours => %s)
                """,
                (DATA_LOOKBACK_HOURS,),
            )
            count = cur.fetchone()[0]
    finally:
        conn.close()

    if count == 0:
        raise ValueError(
            f"Нет данных за последние {DATA_LOOKBACK_HOURS} часов — пайплайн остановлен"
        )

    print(f"Данных за последние {DATA_LOOKBACK_HOURS} часов: {count} транзакций")
    return count


def run_data_quality_check(**context):
    """Базовые проверки после расчёта витрин."""
    conn = get_connection()
    errors = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM transactions WHERE amount <= 0")
            bad_amounts = cur.fetchone()[0]
            if bad_amounts > 0:
                errors.append(f"Найдено {bad_amounts} транзакций с неположительной суммой")

            cur.execute(
                """
                SELECT COUNT(*) FROM transactions t
                LEFT JOIN users u ON t.user_id = u.user_id
                WHERE u.user_id IS NULL
                """
            )
            orphan_tx = cur.fetchone()[0]
            if orphan_tx > 0:
                errors.append(f"Найдено {orphan_tx} транзакций без пользователя")

            cur.execute(
                """
                SELECT ROUND(
                    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)::numeric
                    / NULLIF(COUNT(*), 0) * 100, 2
                ) FROM transactions
                """
            )
            fraud_rate = float(cur.fetchone()[0] or 0)
            if fraud_rate > FRAUD_RATE_THRESHOLD:
                errors.append(
                    f"Fraud rate {fraud_rate}% превышает порог {FRAUD_RATE_THRESHOLD}%"
                )
    finally:
        conn.close()

    if errors:
        raise ValueError("Data quality failures:\n" + "\n".join(errors))

    print(f"Все проверки прошли. Fraud rate: {fraud_rate}%")


start = EmptyOperator(task_id="start", dag=dag)

check_data = PythonOperator(
    task_id="check_data_availability",
    python_callable=check_data_availability,
    dag=dag,
)

gx_validate = BashOperator(
    task_id="gx_validate_source",
    bash_command=f"{GX_PYTHON} {PROJECT_DIR}/expectations/validate.py",
    env=DB_ENV,
    append_env=True,
    dag=dag,
)

spark_transform = BashOperator(
    task_id="spark_bronze_silver",
    bash_command=(
        f"spark-submit --jars /opt/postgresql.jar --master 'local[*]' "
        f"--driver-memory 1g {PROJECT_DIR}/spark/transform.py"
    ),
    env={
        **DB_ENV,
        "BRONZE_PATH": "/data/bronze",
        "SILVER_PATH": "/data/silver",
        "SPARK_JARS": "/opt/postgresql.jar",
    },
    append_env=True,
    dag=dag,
)

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=(
        f"{DBT_BIN} run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
    ),
    dag=dag,
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=(
        f"{DBT_BIN} test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
    ),
    dag=dag,
)

quality_check = PythonOperator(
    task_id="data_quality_check",
    python_callable=run_data_quality_check,
    dag=dag,
)

end = EmptyOperator(
    task_id="end",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

(
    start
    >> check_data
    >> gx_validate
    >> spark_transform
    >> dbt_run
    >> dbt_test
    >> quality_check
    >> end
)
