"""
FinFlow — Kafka Producer
Читает новые транзакции из Postgres и отправляет в топик transactions
"""

import os
import time
import json
import logging
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "user": os.getenv("DB_USER", "finflow"),
    "password": os.getenv("DB_PASSWORD", "finflow123"),
    "dbname": os.getenv("DB_NAME", "finflow_db"),
}
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "transactions"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))


def connect_db(retries=10, delay=3):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            log.info("Подключились к Postgres")
            return conn
        except psycopg2.OperationalError as e:
            log.warning("Попытка %d/%d — Postgres не готов: %s", attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError("Не удалось подключиться к Postgres")


def connect_kafka(retries=15, delay=5):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info("Подключились к Kafka: %s", KAFKA_SERVERS)
            return producer
        except NoBrokersAvailable as e:
            log.warning("Попытка %d/%d — Kafka не готова: %s", attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError("Не удалось подключиться к Kafka")


def run(db_conn, producer):

    # Получаем последний transaction_id при старте
    with db_conn.cursor() as cur:
        cur.execute("SELECT MAX(created_at) FROM transactions")
        row = cur.fetchone()
        last_ts = row[0] if row[0] else datetime.min.replace(tzinfo=timezone.utc)

    log.info("Producer запущен, начинаем с %s, топик: %s", last_ts, TOPIC)
    total_sent = 0

    while True:
        with db_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    t.transaction_id::text,
                    t.user_id::text,
                    u.username,
                    u.country,
                    t.amount::float,
                    t.currency,
                    t.category,
                    t.merchant,
                    t.status,
                    t.is_fraud,
                    t.created_at::text
                FROM transactions t
                JOIN users u ON u.user_id = t.user_id
                WHERE t.created_at > %s
                ORDER BY t.created_at ASC
                LIMIT 500
                """,
                (last_ts,),
            )
            rows = cur.fetchall()

        if rows:
            for row in rows:
                producer.send(TOPIC, value=dict(row), key=row["transaction_id"].encode())
                last_ts = row["created_at"]

            producer.flush()
            total_sent += len(rows)
            log.info("Отправлено в Kafka: %d сообщений (всего %d)", len(rows), total_sent)
        else:
            log.debug("Новых транзакций нет, ждём %d сек...", POLL_INTERVAL)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    db_conn = connect_db()
    producer = connect_kafka()
    try:
        run(db_conn, producer)
    except KeyboardInterrupt:
        log.info("Остановлено вручную")
    finally:
        producer.close()
        db_conn.close()
