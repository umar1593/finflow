"""
FinFlow — главный DAG
Оркестрирует весь пайплайн: проверка данных → Spark → dbt → качество данных
Запускается каждый день в 6 утра
"""
 
from datetime import datetime, timedelta
 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
 
# ── константы ──────────────────────────────────────────────────────────────
DB_CONN = {
    "host":     "postgres",
    "port":     5432,
    "user":     "finflow",
    "password": "finflow123",
    "dbname":   "finflow_db",
}
 
FRAUD_RATE_THRESHOLD = 10.0   # максимально допустимый % фрода
DATA_LOOKBACK_HOURS  = 25     # окно проверки наличия данных
 
# ── настройки DAG ──────────────────────────────────────────────────────────
default_args = {
    "owner":            "finflow",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}
 
dag = DAG(
    dag_id="finflow_pipeline",
    description="Ежедневный ETL пайплайн FinFlow",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["finflow", "etl", "daily"],
)
 
# ── хелпер подключения ─────────────────────────────────────────────────────
def get_connection():
    import psycopg2
    return psycopg2.connect(**DB_CONN)
 
 
# ── задачи ─────────────────────────────────────────────────────────────────
 
def check_data_availability(**context):
    """Проверяем что за вчера есть данные в Postgres."""
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM transactions
            WHERE created_at >= NOW() - INTERVAL '%s hours'
        """, (DATA_LOOKBACK_HOURS,))
        count = cur.fetchone()[0]
    conn.close()
 
    if count == 0:
        raise ValueError(f"Нет данных за последние {DATA_LOOKBACK_HOURS} часов")
 
    print(f"Данных за последние {DATA_LOOKBACK_HOURS} часов: {count} транзакций")
    return count
 
 
def run_data_quality_check(**context):
    """Базовые проверки качества данных после dbt."""
    conn = get_connection()
    errors = []
 
    with conn.cursor() as cur:
 
        cur.execute("SELECT COUNT(*) FROM transactions WHERE amount <= 0")
        zero_amounts = cur.fetchone()[0]
        if zero_amounts > 0:
            errors.append(f"Найдено {zero_amounts} транзакций с нулевой суммой")
 
        cur.execute("""
            SELECT COUNT(*) FROM transactions t
            LEFT JOIN users u ON t.user_id = u.user_id
            WHERE u.user_id IS NULL
        """)
        orphan_tx = cur.fetchone()[0]
        if orphan_tx > 0:
            errors.append(f"Найдено {orphan_tx} транзакций без пользователя")
 
        cur.execute("""
            SELECT ROUND(
                SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)::numeric
                / COUNT(*) * 100, 2
            ) FROM transactions
        """)
        fraud_rate = float(cur.fetchone()[0] or 0)
        if fraud_rate > FRAUD_RATE_THRESHOLD:
            errors.append(f"Fraud rate {fraud_rate}% превышает порог {FRAUD_RATE_THRESHOLD}%")
 
    conn.close()
 
    if errors:
        raise ValueError("Data quality failures:\n" + "\n".join(errors))
 
    print(f"Все проверки прошли. Fraud rate: {fraud_rate}%")
 
 
# ── граф задач ─────────────────────────────────────────────────────────────
 
start = EmptyOperator(task_id="start", dag=dag)
 
check_data = PythonOperator(
    task_id="check_data_availability",
    python_callable=check_data_availability,
    dag=dag,
)
 
spark_transform = BashOperator(
    task_id="spark_bronze_silver",
    bash_command="docker compose --profile spark run --rm spark",
    dag=dag,
)
 
dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir /opt/airflow/dbt_project",
    dag=dag,
)
 
dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/airflow/dbt_project && dbt test --profiles-dir /opt/airflow/dbt_project",
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