-- MART: Risk Signals
-- PURPOSE: Flag suspicious credit card accounts based on
--          velocity (too many txns in a day) and high spend
--          patterns. Used by fraud/risk teams for monitoring.
{{ config(
    materialized='table'
) }}

with base as (
    select * from {{ ref('stg_transactions')}}
),

-- Aggregate transactions per card per day
-- This tells us how active each card is on each date
velocity AS (
    SELECT
        cc_num_token,
        transaction_date,
        COUNT(*)            AS daily_txn_count,
        SUM(amount)         AS daily_spend,
        AVG(amount)         AS avg_daily_txn_amt
    FROM base
    GROUP BY 1, 2
),

-- Calculate rolling 7-day averages per card
-- Compares current day behaviour to the past week
-- This is more accurate than lifetime averages for fraud detection
velocity_with_rolling AS (
    SELECT
        cc_num_token,
        transaction_date,
        daily_txn_count,
        daily_spend,
        avg_daily_txn_amt,

        -- 7-day rolling average transaction count per card
        -- Represents this card's normal daily activity level
        ROUND(AVG(daily_txn_count) OVER (
            PARTITION BY cc_num_token
            ORDER BY transaction_date
            RANGE BETWEEN INTERVAL '6 DAYS' PRECEDING AND CURRENT ROW
        ), 2) AS avg_weekly_txn_count,

        -- 7-day rolling standard deviation for transaction count
        -- Measures how much this card's activity typically varies
        -- A low stddev = very consistent card behaviour
        -- A high stddev = card already has erratic patterns
        ROUND(STDDEV(daily_txn_count) OVER (
            PARTITION BY cc_num_token
            ORDER BY transaction_date
            RANGE BETWEEN INTERVAL '6 DAYS' PRECEDING AND CURRENT ROW
        ), 2) AS stddev_weekly_txn_count,

        -- 7-day rolling average daily spend per card
        ROUND(AVG(daily_spend) OVER (
            PARTITION BY cc_num_token
            ORDER BY transaction_date
            RANGE BETWEEN INTERVAL '6 DAYS' PRECEDING AND CURRENT ROW
        ), 2) AS avg_weekly_spend,

        -- 7-day rolling standard deviation for daily spend
        ROUND(STDDEV(daily_spend) OVER (
            PARTITION BY cc_num_token
            ORDER BY transaction_date
            RANGE BETWEEN INTERVAL '6 DAYS' PRECEDING AND CURRENT ROW
        ), 2) AS stddev_weekly_spend

    FROM velocity
),


-- customer_avg as (
--     select
--         cc_num_token,
--         avg(daily_txn_count) as avg_daily_txn_count,
--         stddev(daily_spend) as stddev_daily_spend,
--     from velocity
--     group by 1
-- ),


-- Calculate merchant-level fraud rates
-- Only include merchants with 10+ transactions (statistically meaningful)
-- Tells us which merchants have historically high fraud rates
merchant_risk AS (
    SELECT
        merchant,
        COUNT(*)                                        AS total_txns,
        SUM(is_fraud)                                   AS fraud_txns,
        ROUND(SUM(is_fraud) / COUNT(*) * 100, 2)       AS merchant_fraud_rate
    FROM base
    GROUP BY 1
    HAVING COUNT(*) >= 10
),

flagged AS (
    SELECT
        cc_num_token,
        transaction_date,
        daily_txn_count,
        daily_spend,
        avg_weekly_txn_count,
        avg_weekly_spend,
        stddev_weekly_txn_count,
        stddev_weekly_spend,

        -- Z-score: how anomalous is today's transaction count?
        -- Higher = more suspicious
        ROUND(
            CASE
                WHEN COALESCE(stddev_weekly_txn_count, 0) = 0 THEN 0
                ELSE (daily_txn_count - avg_weekly_txn_count)
                     / stddev_weekly_txn_count
            END, 2
        ) AS txn_z_score,

        -- Z-score: how anomalous is today's spend amount?
        ROUND(
            CASE
                WHEN COALESCE(stddev_weekly_spend, 0) = 0 THEN 0
                ELSE (daily_spend - avg_weekly_spend)
                     / stddev_weekly_spend
            END, 2
        ) AS spend_z_score,

        -- Flag: transaction count is statistically anomalous
        -- True when z-score exceeds 2 standard deviations
        CASE
            WHEN COALESCE(stddev_weekly_txn_count, 0) > 0
             AND (daily_txn_count - avg_weekly_txn_count)
                 / stddev_weekly_txn_count > 2
            THEN TRUE ELSE FALSE
        END AS high_velocity_flag,

        -- Flag: spend amount is statistically anomalous
        CASE
            WHEN COALESCE(stddev_weekly_spend, 0) > 0
             AND (daily_spend - avg_weekly_spend)
                 / stddev_weekly_spend > 2
            THEN TRUE ELSE FALSE
        END AS high_spend_flag

    FROM velocity_with_rolling
    -- Only keep rows where at least one flag is triggered
    WHERE
        (COALESCE(stddev_weekly_txn_count, 0) > 0
         AND (daily_txn_count - avg_weekly_txn_count)
             / stddev_weekly_txn_count > 2)
        OR
        (COALESCE(stddev_weekly_spend, 0) > 0
         AND (daily_spend - avg_weekly_spend)
             / stddev_weekly_spend > 2)
),

flagged_with_merchant AS (
    SELECT
        f.*,
        -- Get the most common merchant visited on this flagged day
        MODE(b.merchant) AS primary_merchant
    FROM flagged f
    LEFT JOIN base b
        ON f.cc_num_token = b.cc_num_token
        AND f.transaction_date = b.transaction_date
    GROUP BY
        f.cc_num_token,
        f.transaction_date,
        f.daily_txn_count,
        f.daily_spend,
        f.avg_weekly_txn_count,
        f.avg_weekly_spend,
        f.stddev_weekly_txn_count,
        f.stddev_weekly_spend,
        f.txn_z_score,
        f.spend_z_score,
        f.high_velocity_flag,
        f.high_spend_flag
)

SELECT
    f.cc_num_token,
    f.transaction_date,
    f.daily_txn_count,
    f.daily_spend,
    f.avg_weekly_txn_count,
    f.avg_weekly_spend,
    f.txn_z_score,
    f.spend_z_score,
    f.high_velocity_flag,
    f.high_spend_flag,

    -- Average fraud rate of merchants this card visited on the flagged day
    -- Calculated per date — not a global average
    -- High value here means the card visited risky merchants on a suspicious day
    ROUND(AVG(mr.merchant_fraud_rate) OVER (
        PARTITION BY f.transaction_date
    ), 2) AS avg_merchant_fraud_rate,
    ROUND(
        GREATEST(
            COALESCE(f.txn_z_score, 0),
            COALESCE(f.spend_z_score, 0)
        ), 2
    ) AS risk_severity_score
FROM flagged_with_merchant f
LEFT JOIN merchant_risk mr
    ON f.primary_merchant = mr.merchant