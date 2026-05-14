"""Читает транзакции из Postgres и отправляет их в Kafka."""

import os
import time
import json
import logging
from datetime import datetime
from typing import Tuple

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
STATE_KEY = os.getenv("PRODUCER_STATE_KEY", "transactions")
START_MODE = os.getenv("PRODUCER_START_MODE", "earliest")
ZERO_UUID = "00000000-0000-0000-0000-000000000000"


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


def ensure_state_table(db_conn) -> None:
    with db_conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS producer_offsets (
                stream_key TEXT PRIMARY KEY,
                last_created_at TIMESTAMP NOT NULL,
                last_transaction_id UUID NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
    db_conn.commit()


def get_latest_cursor(db_conn) -> Tuple[datetime, str]:
    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COALESCE(MAX(created_at), %s::timestamp)
            FROM transactions
            """,
            (datetime.min,),
        )
        row = cur.fetchone()
        last_created_at = row[0]

        cur.execute(
            """
            SELECT COALESCE(
                MAX(transaction_id)::text,
                %s
            )
            FROM transactions
            WHERE created_at = %s
            """,
            (ZERO_UUID, last_created_at),
        )
        last_transaction_id = cur.fetchone()[0]

    return last_created_at, last_transaction_id


def load_cursor(db_conn) -> Tuple[datetime, str]:
    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_created_at, last_transaction_id::text
            FROM producer_offsets
            WHERE stream_key = %s
            """,
            (STATE_KEY,),
        )
        row = cur.fetchone()

    if row:
        return row[0], row[1]

    if START_MODE == "earliest":
        return datetime.min, ZERO_UUID

    return get_latest_cursor(db_conn)


def save_cursor(db_conn, created_at: datetime, transaction_id: str) -> None:
    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO producer_offsets (
                stream_key,
                last_created_at,
                last_transaction_id,
                updated_at
            )
            VALUES (%s, %s, %s::uuid, NOW())
            ON CONFLICT (stream_key) DO UPDATE
            SET last_created_at = EXCLUDED.last_created_at,
                last_transaction_id = EXCLUDED.last_transaction_id,
                updated_at = NOW()
            """,
            (STATE_KEY, created_at, transaction_id),
        )
    db_conn.commit()


def run(db_conn, producer):
    ensure_state_table(db_conn)
    last_created_at, last_transaction_id = load_cursor(db_conn)

    log.info(
        "Producer запущен, курсор: %s / %s, топик: %s",
        last_created_at,
        last_transaction_id,
        TOPIC,
    )
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
                    t.created_at
                FROM transactions t
                JOIN users u ON u.user_id = t.user_id
                WHERE (
                    t.created_at > %s
                    OR (
                        t.created_at = %s
                        AND t.transaction_id > %s::uuid
                    )
                )
                ORDER BY t.created_at ASC, t.transaction_id ASC
                LIMIT 500
                """,
                (last_created_at, last_created_at, last_transaction_id),
            )
            rows = cur.fetchall()

        if rows:
            for row in rows:
                producer.send(
                    TOPIC,
                    value=dict(row),
                    key=row["transaction_id"].encode(),
                )

            last_row = rows[-1]
            last_created_at = last_row["created_at"]
            last_transaction_id = last_row["transaction_id"]

            producer.flush()
            save_cursor(db_conn, last_created_at, last_transaction_id)
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
