# Great Expectations

Проверки для таблиц `transactions` и `users` в Postgres.

Что проверяется:

- ключи и обязательные поля;
- диапазоны и справочники;
- базовые ограничения по доле `is_fraud`.

Запуск:

```bash
docker compose --profile gx run --rm gx-validate
```
