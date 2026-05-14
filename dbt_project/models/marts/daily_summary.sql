{{
    config(
        materialized='incremental',
        unique_key=['transaction_date', 'category', 'currency'],
        on_schema_change='sync_all_columns'
    )
}}

-- Витрина дневных агрегатов.
--
-- Модель инкрементальная: при повторном запуске пересчитываются только
-- последние сутки (а не вся история). unique_key обеспечивает merge —
-- строки за «открытый» день перезаписываются по мере поступления новых
-- транзакций, поэтому поздно пришедшие данные не теряются.

with base as (
    select * from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    -- берём только данные начиная с последней посчитанной даты
    where transaction_date >= (
        select coalesce(max(transaction_date), '1900-01-01')
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
        -- выборочное стандартное отклонение сумм за день
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
