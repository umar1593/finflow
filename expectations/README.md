# Data Quality — Great Expectations

Слой проверки качества данных. Валидирует **сырые** таблицы `transactions`
и `users` в Postgres до того, как они попадут в Spark и dbt — принцип
«fail fast»: лучше остановить пайплайн на входе, чем чинить витрины.

## Наборы ожиданий

**transactions_suite**
- `transaction_id` — не null, уникальный
- `user_id` — не null
- `amount` — не null, строго больше нуля
- `status` / `category` / `currency` — только из бизнес-справочников
- `is_fraud` — булево; средняя доля фрода в диапазоне 0–10%

**users_suite**
- `user_id` — не null, уникальный
- `email` — уникальный
- `country` — не null
- `age` — в диапазоне 18–90

## Запуск

Отдельным контейнером:

```bash
docker compose --profile gx run --rm gx-validate
```

Внутри Airflow DAG это задача `gx_validate_source` — она вызывает
`validate.py` из изолированного venv (`/opt/gx-venv`). Если хотя бы одно
ожидание не выполнено, скрипт завершается с кодом `1` и DAG падает на
этом шаге, не запуская Spark и dbt.

## Почему ephemeral-контекст

Скрипт использует `gx.get_context()` без `great_expectations.yml` на
диске — наборы ожиданий описаны прямо в коде. Для пет-проекта это проще
ревьюить и держать под контролем версий, чем YAML-конфиг и Data Docs.
