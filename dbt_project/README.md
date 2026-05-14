# FinFlow dbt — Gold-слой

dbt-проект строит аналитические витрины (Gold) поверх Silver-данных.

## Модели

**staging/** (материализация — `view`)
- `stg_transactions` — очищенные транзакции, join с users, derived-колонки
  (день, час, день недели, признак выходного, корзина по сумме).

**marts/** (материализация — `table`, кроме `daily_summary`)
- `daily_summary` — **инкрементальная** витрина дневных агрегатов по
  категориям и валютам. При повторном запуске пересчитываются только
  последние сутки; `unique_key` обеспечивает merge поздних данных.
- `user_metrics` — метрики по пользователям; флаг `is_high_value_user`
  по правилу mean + 2·σ.
- `transaction_anomalies` — статистический скоринг каждой транзакции:
  классическая Z-оценка и робастная (modified z-score на медиане и MAD).

## Тесты

- generic-тесты в `schema.yml`: `unique`, `not_null`, `accepted_values`,
  `dbt_utils.accepted_range`, кастомный `not_negative` (см.
  `macros/test_not_negative.sql`);
- singular-тест `tests/assert_fraud_rate_within_bounds.sql` — контроль
  доли фрода (< 10%).

## Запуск

Профиль подключения — `profiles.yml` в этой папке, параметры читаются из
переменных окружения (`DB_HOST`, `DB_USER`, …).

```bash
cd dbt_project
dbt deps          # подтянуть dbt_utils
dbt run           # построить модели
dbt test          # прогнать тесты
dbt docs generate && dbt docs serve --port 8082
```
