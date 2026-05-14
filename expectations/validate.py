"""Проверки данных через Great Expectations."""

import os
import sys
import logging

import pandas as pd
from sqlalchemy import create_engine
import great_expectations as gx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "user": os.getenv("DB_USER", "finflow"),
    "password": os.getenv("DB_PASSWORD", "finflow123"),
    "dbname": os.getenv("DB_NAME", "finflow_db"),
}

VALID_STATUSES = ["completed", "failed", "pending"]
VALID_CATEGORIES = [
    "groceries", "entertainment", "travel", "dining",
    "healthcare", "electronics", "clothing", "utilities",
]
VALID_CURRENCIES = ["USD", "EUR", "RUB", "GBP"]


def get_engine():
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(url)


def validate_transactions(context, df: pd.DataFrame):
    """Проверки для transactions."""
    asset = context.sources.add_or_update_pandas(
        "finflow_transactions"
    ).add_dataframe_asset(name="transactions")
    batch_request = asset.build_batch_request(dataframe=df)
    context.add_or_update_expectation_suite("transactions_suite")
    v = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="transactions_suite",
    )

    v.expect_column_values_to_not_be_null("transaction_id")
    v.expect_column_values_to_be_unique("transaction_id")
    v.expect_column_values_to_not_be_null("user_id")

    v.expect_column_values_to_not_be_null("amount")
    v.expect_column_values_to_be_between("amount", min_value=0, strict_min=True)

    v.expect_column_values_to_be_in_set("status", VALID_STATUSES)
    v.expect_column_values_to_be_in_set("category", VALID_CATEGORIES)
    v.expect_column_values_to_be_in_set("currency", VALID_CURRENCIES)

    v.expect_column_values_to_be_in_set("is_fraud", [True, False])
    v.expect_column_mean_to_be_between("is_fraud", min_value=0.0, max_value=0.10)

    return v.validate()


def validate_users(context, df: pd.DataFrame):
    """Проверки для users."""
    asset = context.sources.add_or_update_pandas(
        "finflow_users"
    ).add_dataframe_asset(name="users")
    batch_request = asset.build_batch_request(dataframe=df)
    context.add_or_update_expectation_suite("users_suite")
    v = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="users_suite",
    )

    v.expect_column_values_to_not_be_null("user_id")
    v.expect_column_values_to_be_unique("user_id")
    v.expect_column_values_to_be_unique("email")
    v.expect_column_values_to_not_be_null("country")
    v.expect_column_values_to_be_between("age", min_value=18, max_value=90)

    return v.validate()


def main() -> int:
    engine = get_engine()
    log.info("Читаем данные из Postgres...")
    tx = pd.read_sql("SELECT * FROM transactions", engine)
    users = pd.read_sql("SELECT * FROM users", engine)
    log.info("transactions: %d строк, users: %d строк", len(tx), len(users))

    if tx.empty or users.empty:
        log.error("Одна из таблиц пуста — нечего валидировать")
        return 1

    context = gx.get_context()
    results = {
        "transactions": validate_transactions(context, tx),
        "users": validate_users(context, users),
    }

    all_ok = True
    for name, result in results.items():
        stats = result["statistics"]
        passed = stats["successful_expectations"]
        total = stats["evaluated_expectations"]
        status = "OK" if result["success"] else "FAILED"
        log.info("[%s] %s — %d/%d ожиданий выполнено", name, status, passed, total)

        if not result["success"]:
            all_ok = False
            for r in result["results"]:
                if not r["success"]:
                    cfg = r["expectation_config"]
                    log.error(
                        "  ✗ %s | %s",
                        cfg["expectation_type"], cfg["kwargs"],
                    )

    if not all_ok:
        log.error("Валидация качества данных НЕ пройдена")
        return 1

    log.info("Все наборы ожиданий выполнены — данные валидны")
    return 0


if __name__ == "__main__":
    sys.exit(main())
