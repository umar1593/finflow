with base as (
    select * from {{ ref('stg_transactions') }}
),

daily_summary as (
    select
        transaction_date,
        category,
        currency,
        count(*)                                    as tx_count,
        round(sum(amount)::numeric, 2)              as total_amount,
        round(avg(amount)::numeric, 2)              as avg_amount,
        round(min(amount)::numeric, 2)              as min_amount,
        round(max(amount)::numeric, 2)              as max_amount,
        -- стандартное отклонение для Z-score
        round(stddev(amount)::numeric, 2)           as stddev_amount,
        sum(case when is_fraud then 1 else 0 end)   as fraud_count,
        round(
            sum(case when is_fraud then 1 else 0 end)::numeric
            / nullif(count(*), 0) * 100, 2
        )                                           as fraud_rate_pct,
        sum(case when status = 'failed' then 1 else 0 end) as failed_count,
        count(distinct user_id)                     as unique_users

    from base
    group by 1, 2, 3
)

select * from daily_summary
order by transaction_date desc, total_amount desc
