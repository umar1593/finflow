with base as (
    select * from {{ ref('stg_transactions') }}
),

user_metrics as (
    select
        user_id,
        username,
        country,
        age,
        count(*)                                    as total_transactions,
        round(sum(amount)::numeric, 2)              as total_spent,
        round(avg(amount)::numeric, 2)              as avg_transaction,
        round(max(amount)::numeric, 2)              as max_transaction,
        round(stddev(amount)::numeric, 2)           as stddev_amount,
        sum(case when is_fraud then 1 else 0 end)   as fraud_count,
        min(created_at)                             as first_transaction_at,
        max(created_at)                             as last_transaction_at,

        -- любимая категория пользователя
        mode() within group (order by category)    as top_category,

        -- процент weekend транзакций
        round(
            sum(case when is_weekend then 1 else 0 end)::numeric
            / nullif(count(*), 0) * 100, 2
        )                                           as weekend_tx_pct,

        -- Z-score флаг: пользователи с аномально высоким avg
        case
            when avg(amount) > (
                select avg(amount) + 2 * stddev(amount)
                from base
            ) then true
            else false
        end                                         as is_high_value_user

    from base
    group by 1, 2, 3, 4
)

select * from user_metrics
order by total_spent desc
