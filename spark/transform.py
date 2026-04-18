"""
FinFlow — PySpark трансформации
Bronze: сырые данные из Postgres → Parquet
Silver: очищенные и обогащённые данные → Parquet
"""

import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

DB_HOST     = os.getenv("DB_HOST",     "postgres")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_USER     = os.getenv("DB_USER",     "finflow")
DB_PASSWORD = os.getenv("DB_PASSWORD", "finflow123")
DB_NAME     = os.getenv("DB_NAME",     "finflow_db")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "/data/silver")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_PROPS = {
    "user":     DB_USER,
    "password": DB_PASSWORD,
    "driver":   "org.postgresql.Driver",
}


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FinFlow Transformations")
        .config("spark.jars", "/opt/spark/jars/postgresql.jar")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


# ── BRONZE ────────────────────────────────────────────────────────────────
def extract_bronze(spark: SparkSession) -> None:
    """Читаем сырые данные из Postgres и пишем в Parquet как есть."""
    log.info("Извлекаем transactions → Bronze")

    df_transactions = spark.read.jdbc(
        url=JDBC_URL,
        table="transactions",
        properties=JDBC_PROPS,
    )
    df_users = spark.read.jdbc(
        url=JDBC_URL,
        table="users",
        properties=JDBC_PROPS,
    )

    run_date = datetime.now().strftime("%Y-%m-%d")

    df_transactions.write.mode("overwrite").parquet(
        f"{BRONZE_PATH}/transactions/date={run_date}"
    )
    df_users.write.mode("overwrite").parquet(
        f"{BRONZE_PATH}/users/date={run_date}"
    )

    log.info("Bronze: transactions=%d, users=%d",
             df_transactions.count(), df_users.count())


# ── SILVER ────────────────────────────────────────────────────────────────
def transform_silver(spark: SparkSession) -> None:
    """Очищаем, джойним, добавляем derived-колонки → Silver."""
    log.info("Трансформируем Bronze → Silver")

    run_date = datetime.now().strftime("%Y-%m-%d")

    tx = spark.read.parquet(f"{BRONZE_PATH}/transactions/date={run_date}")
    users = spark.read.parquet(f"{BRONZE_PATH}/users/date={run_date}")

    # 1. Убираем дубли
    tx = tx.dropDuplicates(["transaction_id"])

    # 2. Фильтруем некорректные суммы
    tx = tx.filter(F.col("amount") > 0)

    # 3. Джойним с пользователями
    df = tx.join(
        users.select("user_id", "username", "country", "age"),
        on="user_id",
        how="left",
    )

    # 4. Derived-колонки
    df = (
        df
        .withColumn("hour_of_day",   F.hour("created_at"))
        .withColumn("day_of_week",   F.dayofweek("created_at"))
        .withColumn("is_weekend",    F.dayofweek("created_at").isin([1, 7]))
        .withColumn("amount_bucket",
            F.when(F.col("amount") < 10,   "micro")
             .when(F.col("amount") < 50,   "small")
             .when(F.col("amount") < 200,  "medium")
             .when(F.col("amount") < 1000, "large")
             .otherwise("whale")
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    # 5. Нормализуем строки
    df = (
        df
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn("status",   F.lower(F.trim(F.col("status"))))
    )

    # 6. Партиционируем по категории
    df.write.mode("overwrite").partitionBy("category").parquet(SILVER_PATH)

    log.info("Silver: %d строк записано", df.count())

    # Показываем схему и примеры
    df.printSchema()
    df.groupBy("category", "amount_bucket") \
      .agg(
          F.count("*").alias("count"),
          F.round(F.avg("amount"), 2).alias("avg_amount"),
          F.sum(F.col("is_fraud").cast("int")).alias("fraud_count"),
      ) \
      .orderBy("category", "amount_bucket") \
      .show(50, truncate=False)


# ── MAIN ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        extract_bronze(spark)
        transform_silver(spark)
        log.info("Трансформации завершены успешно")
    finally:
        spark.stop()
