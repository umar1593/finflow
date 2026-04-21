"""
FinFlow — генератор синтетических транзакций
Создаёт пользователей и непрерывно пишет транзакции в Postgres
"""

import os
import time
import random
import logging

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

fake = Faker()

# ── настройки из переменных окружения ──────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "user": os.getenv("DB_USER", "finflow"),
    "password": os.getenv("DB_PASSWORD", "finflow123"),
    "dbname": os.getenv("DB_NAME", "finflow_db"),
}
TPS = float(os.getenv("TRANSACTIONS_PER_SECOND", "2"))

# ── справочники ────────────────────────────────────────────────────────────
CATEGORIES = [
    "groceries", "entertainment", "travel", "dining",
    "healthcare", "electronics", "clothing", "utilities",
]
MERCHANTS = {
    "groceries": ["Whole Foods", "Lidl", "Auchan", "Carrefour"],
    "entertainment": ["Netflix", "Spotify", "Steam", "Kinopoisk"],
    "travel": ["Booking.com", "Airbnb", "Aeroflot", "S7 Airlines"],
    "dining": ["McDonald's", "Burger King", "Local Cafe", "Sushi Bar"],
    "healthcare": ["Medsi", "DocPlus", "Pharmacy 36.6", "SM-Clinic"],
    "electronics": ["DNS", "Mvideo", "Apple Store", "Samsung Store"],
    "clothing": ["Zara", "H&M", "Wildberries", "Lamoda"],
    "utilities": ["Gazprom", "Mosenergo", "ER-Telecom", "Rostelecom"],
}
CURRENCIES = ["USD", "EUR", "RUB", "GBP"]
COUNTRIES = ["Russia", "Germany", "USA", "France", "UK", "Netherlands"]
STATUSES = ["completed", "completed", "completed", "completed", "failed", "pending"]
FRAUD_RATE = 0.03   # 3% транзакций — фрод


def connect(retries: int = 10, delay: int = 3) -> psycopg2.extensions.connection:
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            log.info("Подключились к Postgres")
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("Попытка %d/%d — Postgres ещё не готов: %s", attempt, retries, exc)
            time.sleep(delay)
    raise RuntimeError("Не удалось подключиться к Postgres")


def seed_users(conn, n: int = 100) -> list[str]:
    """Создаём пользователей если их ещё нет."""
    with conn.cursor() as cur:
        cur.execute("SELECT user_id FROM users LIMIT 1")
        if cur.fetchone():
            cur.execute("SELECT user_id FROM users")
            ids = [row[0] for row in cur.fetchall()]
            log.info("Пользователи уже есть: %d шт.", len(ids))
            return ids

    rows = [
        (
            fake.user_name() + str(random.randint(1, 9999)),
            fake.unique.email(),
            random.choice(COUNTRIES),
            random.randint(18, 70),
        )
        for _ in range(n)
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO users (username, email, country, age) VALUES %s RETURNING user_id",
            rows,
        )
        ids = [row[0] for row in cur.fetchall()]
    conn.commit()
    log.info("Создали %d пользователей", len(ids))
    return ids


def make_transaction(user_ids: list[str]) -> tuple:
    category = random.choice(CATEGORIES)
    merchant = random.choice(MERCHANTS[category])
    is_fraud = random.random() < FRAUD_RATE

    # Мошеннические транзакции — нетипично большие суммы
    if is_fraud:
        amount = round(random.uniform(500, 5000), 2)
    else:
        amount = round(random.uniform(1, 300), 2)

    return (
        random.choice(user_ids),
        amount,
        random.choice(CURRENCIES),
        category,
        merchant,
        random.choice(STATUSES),
        is_fraud,
    )


def run(conn, user_ids: list[str]) -> None:
    interval = 1.0 / TPS
    batch_size = max(1, int(TPS))   # пишем батчами раз в секунду
    batch: list[tuple] = []
    total = 0

    log.info("Генератор запущен. %.1f tx/сек, батч %d", TPS, batch_size)

    while True:
        batch.append(make_transaction(user_ids))

        if len(batch) >= batch_size:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO transactions
                        (user_id, amount, currency, category, merchant, status, is_fraud)
                    VALUES %s
                    """,
                    batch,
                )
            conn.commit()
            total += len(batch)
            log.info("Записано транзакций: %d (всего %d)", len(batch), total)
            batch = []

        time.sleep(interval)


if __name__ == "__main__":
    conn = connect()
    try:
        user_ids = seed_users(conn)
        run(conn, user_ids)
    except KeyboardInterrupt:
        log.info("Остановлено вручную")
    finally:
        conn.close()
