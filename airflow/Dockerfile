# FinFlow — кастомный образ Airflow.
#
# Airflow оркестрирует пайплайн из трёх тяжёлых инструментов: Spark, dbt и
# Great Expectations. У каждого свои жёсткие пины зависимостей, которые
# конфликтуют и друг с другом, и с самим Airflow. Поэтому dbt и GE ставятся
# в ИЗОЛИРОВАННЫЕ venv — DAG вызывает их по абсолютному пути к интерпретатору.
# Так образ остаётся самодостаточным (всё работает после `docker compose up`),
# без проблем с разрешением версий.
#
# В продакшене Spark/dbt чаще запускают отдельными воркерами или через
# KubernetesPodOperator; для локального воспроизводимого стенда выбран
# самодостаточный образ.
FROM apache/airflow:2.9.1

USER root

# default-jre — для локального spark-submit; procps — для отладки.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# JDBC-драйвер PostgreSQL для Spark.
RUN curl -sSL -o /opt/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Изолированный venv для dbt.
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
       "dbt-core==1.8.8" "dbt-postgres==1.8.8"

# Изолированный venv для Great Expectations.
RUN python -m venv /opt/gx-venv \
    && /opt/gx-venv/bin/pip install --no-cache-dir \
       "great-expectations==0.18.22" psycopg2-binary "sqlalchemy<2.0" pandas

USER airflow

# pyspark ставим в основное окружение Airflow — у него мало зависимостей
# (только py4j), конфликтов не создаёт.
RUN pip install --no-cache-dir "pyspark==3.5.1" psycopg2-binary
