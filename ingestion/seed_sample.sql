-- FinFlow — небольшой детерминированный датасет.
-- Используется в CI, чтобы dbt-модели строились на реальных данных,
-- а тесты были осмысленными (а не проходили на пустых таблицах).
-- 10 пользователей × 10 транзакций = 100 строк, >= 5 транзакций на юзера
-- (нужно для модели transaction_anomalies).

INSERT INTO users (username, email, country, age)
SELECT
    'ci_user_' || g,
    'ci_user_' || g || '@finflow.local',
    (ARRAY['Russia', 'Germany', 'USA'])[1 + (g % 3)],
    20 + g
FROM generate_series(1, 10) AS g;

INSERT INTO transactions (user_id, amount, currency, category, merchant, status, is_fraud)
SELECT
    u.user_id,
    round((random() * 200 + 1)::numeric, 2),
    'USD',
    (ARRAY['groceries', 'dining', 'travel', 'electronics'])[1 + (s % 4)],
    'CI Merchant',
    'completed',
    false
FROM users u
CROSS JOIN generate_series(1, 10) AS s;
