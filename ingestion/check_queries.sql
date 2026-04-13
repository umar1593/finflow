-- FinFlow — проверочные SQL-запросы
-- Открой в Adminer (http://localhost:8080) и запускай по одному

-- 1. Сколько транзакций уже накопилось?
SELECT COUNT(*) AS total_transactions FROM transactions;

-- 2. Транзакции по категориям
SELECT
    category,
    COUNT(*)            AS tx_count,
    ROUND(SUM(amount)::numeric, 2)  AS total_amount,
    ROUND(AVG(amount)::numeric, 2)  AS avg_amount
FROM transactions
GROUP BY category
ORDER BY tx_count DESC;

-- 3. Последние 10 транзакций
SELECT
    t.transaction_id,
    u.username,
    t.amount,
    t.currency,
    t.category,
    t.merchant,
    t.is_fraud,
    t.created_at
FROM transactions t
JOIN users u ON u.user_id = t.user_id
ORDER BY t.created_at DESC
LIMIT 10;

-- 4. Топ-5 пользователей по сумме трат
SELECT
    u.username,
    u.country,
    COUNT(t.transaction_id)         AS tx_count,
    ROUND(SUM(t.amount)::numeric, 2) AS total_spent
FROM transactions t
JOIN users u ON u.user_id = t.user_id
GROUP BY u.user_id, u.username, u.country
ORDER BY total_spent DESC
LIMIT 5;

-- 5. Статистика по фроду
SELECT
    is_fraud,
    COUNT(*)                         AS count,
    ROUND(AVG(amount)::numeric, 2)   AS avg_amount,
    ROUND(MAX(amount)::numeric, 2)   AS max_amount
FROM transactions
GROUP BY is_fraud;

-- 6. Динамика по часам (последние 24 часа)
SELECT
    date_trunc('hour', created_at)   AS hour,
    COUNT(*)                          AS tx_count,
    ROUND(SUM(amount)::numeric, 2)   AS volume
FROM transactions
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY 1
ORDER BY 1 DESC;
