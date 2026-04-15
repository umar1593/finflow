"""
FinFlow — Kafka Consumer
Читает топик transactions, считает базовую статистику и детектирует фрод
"""

import os
import json
import logging
from collections import defaultdict

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC         = "transactions"
GROUP_ID      = "finflow-consumer-group"


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
    stats = defaultdict(lambda: {"count": 0, "total": 0.0, "fraud": 0})
    total = 0
    fraud_total = 0

    log.info("Consumer запущен, ждём сообщения...")

    while True:
        for message in consumer:
            tx = message.value
            cat = tx.get("category", "unknown")
            amount = float(tx.get("amount", 0))
            is_fraud = tx.get("is_fraud", False)

            stats[cat]["count"]  += 1
            stats[cat]["total"]  += amount
            if is_fraud:
                stats[cat]["fraud"] += 1
                fraud_total += 1

            total += 1

            # Алерт на фрод
            if is_fraud:
                log.warning(
                    "FRAUD DETECTED | user=%s | amount=%.2f %s | merchant=%s",
                    tx.get("username"), amount, tx.get("currency"), tx.get("merchant")
                )

            # Печатаем сводку каждые 50 сообщений
            if total % 50 == 0:
                log.info("── Сводка (всего %d транзакций, фрод: %d) ──", total, fraud_total)
                for category, s in sorted(stats.items(), key=lambda x: -x[1]["count"]):
                    log.info(
                        "  %-15s | %4d tx | %9.2f USD | fraud: %d",
                        category, s["count"], s["total"], s["fraud"]
                    )


if __name__ == "__main__":
    consumer = connect_kafka()
    try:
        run(consumer)
    except KeyboardInterrupt:
        log.info("Остановлено вручную")
    finally:
        consumer.close()
