{{
    config(
        materialized='incremental',
        unique_key=['transaction_date', 'category', 'currency'],
        on_schema_change='sync_all_columns'
    )
}}

-- Дневные агрегаты по категориям и валютам.

with base as (
    select * from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    where transaction_date >= (
        select coalesce(
            max(transaction_date) - interval '7 day',
            '1900-01-01'::timestamp
        )
        from {{ this }}
    )
    {% endif %}
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
        round(stddev_samp(amount)::numeric, 2)      as stddev_amount,
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
