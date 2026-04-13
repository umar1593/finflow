-- FinFlow database schema
-- Запускается автоматически при первом старте Postgres

CREATE TABLE IF NOT EXISTS users (
    user_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username    VARCHAR(50)  NOT NULL UNIQUE,
    email       VARCHAR(100) NOT NULL UNIQUE,
    country     VARCHAR(50)  NOT NULL,
    age         INT          NOT NULL CHECK (age BETWEEN 18 AND 90),
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID        NOT NULL REFERENCES users(user_id),
    amount          NUMERIC(12, 2) NOT NULL,
    currency        VARCHAR(3)  NOT NULL DEFAULT 'USD',
    category        VARCHAR(50) NOT NULL,
    merchant        VARCHAR(100) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'completed',
    is_fraud        BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- Индексы для быстрых запросов
CREATE INDEX IF NOT EXISTS idx_transactions_user_id   ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_category   ON transactions(category);
CREATE INDEX IF NOT EXISTS idx_transactions_is_fraud   ON transactions(is_fraud);

-- Вьюха для быстрой проверки данных
CREATE OR REPLACE VIEW v_transaction_summary AS
SELECT
    date_trunc('hour', created_at) AS hour,
    category,
    COUNT(*)                        AS tx_count,
    SUM(amount)                     AS total_amount,
    AVG(amount)                     AS avg_amount,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_count
FROM transactions
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
