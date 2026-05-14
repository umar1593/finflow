# dbt

dbt-проект строит витрины поверх таблиц из Postgres.

## Модели

- `models/staging/stg_transactions.sql`:
  очистка транзакций, join с `users`, служебные поля для аналитики.
- `models/marts/daily_summary.sql`:
  дневные агрегаты по категориям и валютам.
- `models/marts/user_metrics.sql`:
  метрики по пользователям.
- `models/marts/transaction_anomalies.sql`:
  скоринг аномальных транзакций.

## Запуск

```bash
cd dbt_project
dbt deps
dbt build --profiles-dir .
```
