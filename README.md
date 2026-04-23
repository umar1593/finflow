# FinFlow — Real-Time Financial Analytics Platform

> End-to-end data engineering платформа для обработки финансовых транзакций в реальном времени. Построена на современном open-source стеке, полностью локальная через Docker.

## Архитектура

```
PostgreSQL → Kafka (KRaft) → PySpark → dbt → Grafana
                ↓                  ↓
           Consumer            Airflow DAG
        (fraud detection)    (orchestration)
```

**Слои данных (Medallion Architecture):**
- **Bronze** — сырые данные из Postgres → Parquet (источник истины)
- **Silver** — очищенные, обогащённые данные + derived-колонки
- **Gold** — dbt агрегации для аналитики (daily_summary, user_metrics)

## Стек технологий

| Слой | Технология | Зачем |
|------|-----------|-------|
| Источник | PostgreSQL 15 | Хранение транзакций |
| Стриминг | Apache Kafka (KRaft) | Real-time поток событий |
| Batch обработка | Apache Spark (PySpark) | ETL, трансформации |
| Трансформации | dbt-core | ELT модели, тесты, документация |
| Оркестрация | Apache Airflow | Scheduling, зависимости задач |
| Мониторинг | Grafana | Дашборды, метрики |
| Качество данных | Great Expectations | Валидация схемы и диапазонов |
| CI/CD | GitHub Actions | Lint, тесты, dbt compile на каждый PR |
| Контейнеризация | Docker + Docker Compose | Весь стек локально |

## Быстрый старт

### Требования
- Docker Desktop
- Python 3.11+
- dbt-postgres
### Запуск основного стека

```bash
git clone https://github.com/umar1593/finflow.git
cd finflow
docker compose up -d
```

Сервисы после запуска:
- **Adminer** (UI для Postgres): http://localhost:8080
- **Kafka UI**: http://localhost:8081
- **Grafana**: http://localhost:3000 (admin/admin123)

### Запуск Spark джоба (Bronze + Silver)

```bash
docker compose --profile spark run --rm spark
```

### Запуск dbt (Gold слой)

```bash
cd dbt_project
dbt run
dbt test
dbt docs serve --port 8082  # документация на http://localhost:8082
```

### Запуск Airflow

```bash
docker compose --profile airflow up -d airflow
```

Airflow UI: http://localhost:8083 (admin/admin123)

## Структура проекта

```
finflow/
├── ingestion/
│   ├── generator.py          # Генератор синтетических транзакций
│   ├── producer.py           # Kafka producer (Postgres → топик)
│   ├── consumer.py           # Kafka consumer (fraud detection)
│   └── schema.sql            # Схема БД
├── spark/
│   └── transform.py          # PySpark Bronze/Silver трансформации
├── dbt_project/
│   └── models/
│       ├── staging/
│       │   └── stg_transactions.sql
│       └── marts/
│           ├── daily_summary.sql
│           └── user_metrics.sql
├── dags/
│   └── finflow_dag.py        # Airflow DAG
├── grafana/
│   └── datasource.yml        # Автоматическое подключение к Postgres
└── docker-compose.yml
```

## Ключевые архитектурные решения

**KRaft вместо Zookeeper** — Kafka 3.7 в режиме KRaft управляет метаданными без внешних зависимостей. Zookeeper deprecated в Kafka 3.x.

**ELT вместо ETL** — данные сначала загружаются в Bronze (сырые), потом трансформируются. Это позволяет переобработать данные с нуля при изменении бизнес-логики.

**Partition pruning** — Silver слой партиционирован по `category`. Запрос по одной категории читает 1/8 данных.

**Атомарная запись Spark** — при сбое джоба данные не повреждаются. Файл `_SUCCESS` сигнализирует об успешном завершении.

**Z-score аномалии** — модель `user_metrics` использует `stddev` для статистического обнаружения пользователей с нетипично высокими тратами.

## dbt тесты

```
PASS=20 WARN=0 ERROR=0
```

20 тестов покрывают: уникальность ID, not_null, допустимые значения статусов, целостность внешних ключей.

## CI/CD Pipeline

На каждый PR в `main`:
1. **Lint** — flake8 проверка Python кода
2. **Syntax** — проверка синтаксиса всех файлов
3. **dbt** — compile + run + test против реального Postgres в GitHub Actions

## Данные

Проект использует синтетические данные:
- 100 пользователей из разных стран
- ~2 транзакции в секунду
- 8 категорий: groceries, electronics, dining, travel, healthcare, clothing, entertainment, utilities
- 3% транзакций помечены как фрод (нетипично большие суммы: $500-$5000)

## Автор

**Umar Shirvaniev** — [github.com/umar1593](https://github.com/umar1593)
