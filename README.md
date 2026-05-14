# FinFlow

Локальный стенд для потока финансовых транзакций. Данные пишутся в
PostgreSQL, публикуются в Kafka, проходят через Spark и dbt, а качество
и итоговые метрики проверяются отдельными задачами.

## Что есть в проекте

- генератор пользователей и транзакций;
- producer/consumer для Kafka;
- Spark-джоба для Bronze и Silver;
- dbt-модели для витрин;
- проверки через Great Expectations;
- DAG для Airflow;
- Grafana-дашборд.

## Поток данных

```text
generator -> Postgres -> producer -> Kafka -> consumer
                    \-> Spark -> Parquet -> dbt
                    \-> Great Expectations
```

## Быстрый старт

Нужно:

- Docker Desktop;
- Python 3.11+, если запускать `dbt` или тесты с хоста.

```bash
git clone https://github.com/umar1593/finflow.git
cd finflow
cp .env.example .env
docker compose up -d
```

После старта доступны:

- Adminer: http://localhost:8080
- Kafka UI: http://localhost:8081
- Grafana: http://localhost:3000

## Отдельные шаги

Spark:

```bash
docker compose --profile spark run --rm spark
```

dbt:

```bash
cd dbt_project
dbt deps
dbt build --profiles-dir .
```

Great Expectations:

```bash
docker compose --profile gx run --rm gx-validate
```

Airflow:

```bash
docker compose --profile airflow up -d airflow
```

UI Airflow: http://localhost:8083

## Структура

```text
finflow/
├── ingestion/       генератор, producer, consumer, schema
├── spark/           Spark-трансформации
├── dbt_project/     staging и marts модели
├── expectations/    проверки качества данных
├── dags/            Airflow DAG
├── grafana/         provisioning и дашборд
├── tests/           unit-тесты
└── docker-compose.yml
```

## Что считается в проекте

- `consumer.py` помечает подозрительные суммы через Z-score по истории
  пользователя и общему распределению;
- `spark/transform.py` выгружает сырые таблицы в Bronze и собирает
  Silver-слой с нормализацией и derived-полями;
- `dbt_project/models/marts/` строит витрины дневных агрегатов,
  пользовательских метрик и аномальных транзакций.

## Ограничения

- producer хранит курсор в Postgres, но не решает дедупликацию при
  повторной отправке после аварийного рестарта;
- Spark и dbt рассчитаны на локальный запуск, без отдельного кластера;
- секреты для локального стенда лежат в `docker-compose.yml` и `.env`.
