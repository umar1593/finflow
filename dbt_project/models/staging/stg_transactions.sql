with source as (
    select * from {{ source('finflow', 'transactions') }}
),

users as (
    select * from {{ source('finflow', 'users') }}
),

joined as (
    select
        t.transaction_id,
        t.user_id,
        u.username,
        u.country,
        u.age,
        t.amount,
        t.currency,
        t.category,
        t.merchant,
        t.status,
        t.is_fraud,
        t.created_at,

        -- derived columns
        date_trunc('day', t.created_at)  as transaction_date,
        extract(hour from t.created_at)  as hour_of_day,
        extract(dow from t.created_at)   as day_of_week,
        case
            when extract(dow from t.created_at) in (0, 6) then true
            else false
        end                              as is_weekend,
        case
            when t.amount < 10   then 'micro'
            when t.amount < 50   then 'small'
            when t.amount < 200  then 'medium'
            when t.amount < 1000 then 'large'
            else 'whale'
        end                              as amount_bucket

    from source t
    left join users u on t.user_id = u.user_id
    where t.amount > 0
)

select * from joined
