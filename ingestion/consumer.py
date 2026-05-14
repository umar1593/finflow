"""
FinFlow — Kafka Consumer со статистическим fraud-детектом.

Читает топик transactions и в реальном времени помечает аномальные
транзакции с помощью потокового детектора (Z-оценка по пользователю и
по глобальному распределению, алгоритм Уэлфорда — см. stats.py).

Детектор не использует метку is_fraud при принятии решения. Метка из
генератора служит «эталоном»: по ней consumer считает precision /
recall / F1 и периодически печатает матрицу ошибок. Это показывает,
насколько статистический подход совпадает с разметкой.
"""

import os
import json
import time
import logging
from collections import defaultdict

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from stats import AnomalyDetector, ConfusionMatrix

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "transactions"
GROUP_ID = "finflow-consumer-group"

# Порог Z-оценки: 3.0 ≈ правило трёх сигм (для нормального распределения
# за пределами 3σ лежит ~0.3% наблюдений).
Z_THRESHOLD = float(os.getenv("Z_THRESHOLD", "3.0"))
SUMMARY_EVERY = int(os.getenv("SUMMARY_EVERY", "50"))


def connect_kafka(retries=15, delay=5):
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            log.info("Consumer подключился к Kafka, топик: %s", TOPIC)
            return consumer
        except NoBrokersAvailable as e:
            log.warning("Попытка %d/%d — Kafka не готова: %s", attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError("Не удалось подключиться к Kafka")


def run(consumer):
    detector = AnomalyDetector(z_threshold=Z_THRESHOLD)
    matrix = ConfusionMatrix()
    by_category = defaultdict(lambda: {"count": 0, "total": 0.0, "anomalies": 0})
    total = 0

    log.info("Consumer запущен. Порог Z-оценки = %.1f, ждём сообщения...", Z_THRESHOLD)

    while True:
        for message in consumer:
            tx = message.value
            cat = tx.get("category", "unknown")
            amount = float(tx.get("amount", 0))
            user_id = tx.get("user_id", "unknown")
            actual_fraud = bool(tx.get("is_fraud", False))

            result = detector.evaluate(user_id, amount)
            predicted = result["is_anomaly"]

            matrix.update(predicted, actual_fraud)
            by_category[cat]["count"] += 1
            by_category[cat]["total"] += amount
            if predicted:
                by_category[cat]["anomalies"] += 1
            total += 1

            if predicted:
                log.warning(
                    "ANOMALY | user=%s | amount=%.2f %s | merchant=%s | "
                    "z_user=%.2f z_global=%.2f | labeled_fraud=%s",
                    tx.get("username"), amount, tx.get("currency"),
                    tx.get("merchant"), result["z_user"], result["z_global"],
                    actual_fraud,
                )

            if total % SUMMARY_EVERY == 0:
                log.info(
                    "── Сводка: %d транзакций | детектор: precision=%.2f "
                    "recall=%.2f F1=%.2f | TP=%d FP=%d FN=%d TN=%d ──",
                    total, matrix.precision, matrix.recall, matrix.f1,
                    matrix.tp, matrix.fp, matrix.fn, matrix.tn,
                )
                for category, s in sorted(
                    by_category.items(), key=lambda x: -x[1]["count"]
                ):
                    log.info(
                        "  %-15s | %4d tx | %10.2f | аномалий: %d",
                        category, s["count"], s["total"], s["anomalies"],
                    )


if __name__ == "__main__":
    consumer = connect_kafka()
    try:
        run(consumer)
    except KeyboardInterrupt:
        log.info("Остановлено вручную")
    finally:
        consumer.close()
