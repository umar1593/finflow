-- Витрина статистического скоринга аномальных транзакций.
--
-- Для каждой транзакции считаются две оценки выброса относительно
-- ИСТОРИИ КОНКРЕТНОГО ПОЛЬЗОВАТЕЛЯ:
--
--   1. z_score        — классическая Z-оценка: (x − μ) / σ.
--                       Чувствительна к самим выбросам (μ и σ «едут»).
--   2. robust_z_score — модифицированная Z-оценка на медиане и MAD:
--                       0.6745 · (x − median) / MAD.
--                       Устойчива к выбросам, т.к. опирается на квантили.
--                       Константа 0.6745 приводит MAD к σ нормального
--                       распределения (это 0.75-квантиль стандартной
--                       нормали).
--
-- Транзакция помечается аномальной, если хотя бы одна оценка превышает
-- порог. Метка is_fraud из источника в скоринге НЕ участвует — она
-- остаётся в витрине только для сверки качества детектора.

with base as (
    select * from {{ ref('stg_transactions') }}
),

-- первый проход: среднее, σ и медиана по каждому пользователю
per_user as (
    select
        user_id,
        count(*)                                            as user_tx_count,
        avg(amount)                                         as user_avg,
        stddev_samp(amount)                                 as user_std,
        percentile_cont(0.5) within group (order by amount) as user_median
    from base
    group by 1
),

-- абсолютные отклонения от медианы — нужны для MAD
deviations as (
    select
        b.user_id,
        abs(b.amount - p.user_median) as abs_dev
    from base b
    join per_user p using (user_id)
),

-- второй проход: MAD = median(|x − median|)
mad as (
    select
        user_id,
        percentile_cont(0.5) within group (order by abs_dev) as user_mad
    from deviations
    group by 1
),

scored as (
    select
        b.transaction_id,
        b.user_id,
        b.username,
        b.country,
        b.amount,
        b.category,
        b.merchant,
        b.created_at,
        b.is_fraud,
        round(p.user_avg::numeric, 2)     as user_avg,
        round(p.user_std::numeric, 2)     as user_std,
        round(p.user_median::numeric, 2)  as user_median,
        round(m.user_mad::numeric, 2)     as user_mad,

        case
            when p.user_std > 0
            then round(((b.amount - p.user_avg) / p.user_std)::numeric, 2)
            else 0
        end as z_score,

        case
            when m.user_mad > 0
            then round((0.6745 * (b.amount - p.user_median) / m.user_mad)::numeric, 2)
            else 0
        end as robust_z_score

    from base b
    join per_user p using (user_id)
    join mad m using (user_id)
    -- нужна минимальная история, иначе оценки неустойчивы
    where p.user_tx_count >= 5
)

select
    *,
    (z_score > 3.0 or robust_z_score > 3.5) as is_anomaly
from scored
order by robust_z_score desc
