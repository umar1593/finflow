-- Singular-тест: общая доля фрода не должна превышать 10%.
-- Тест «падает», если запрос вернёт хотя бы одну строку — то есть
-- если доля подозрительных транзакций вышла за бизнес-порог.

with rate as (
    select
        sum(case when is_fraud then 1 else 0 end)::numeric
        / nullif(count(*), 0) * 100 as fraud_rate_pct
    from {{ ref('stg_transactions') }}
)

select fraud_rate_pct
from rate
where fraud_rate_pct > 10
