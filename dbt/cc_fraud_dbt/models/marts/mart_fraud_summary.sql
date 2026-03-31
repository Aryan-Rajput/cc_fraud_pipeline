{{ config(materialized='table') }}

WITH base AS (
    -- All transactions including both fraud and legitimate
    SELECT * FROM {{ ref('stg_transactions') }} 
),

-- Fraud breakdown by transaction category
-- e.g. is grocery fraud more common than entertainment fraud?
by_category AS (
    SELECT
        'category'                                              AS dimension,
        category                                               AS value,
        COUNT(*)                                               AS total_txns,
        SUM(is_fraud)                                          AS fraud_txns,
        COUNT(*) - SUM(is_fraud)                               AS legit_txns,
        ROUND(SUM(is_fraud) / COUNT(*) * 100, 3)              AS fraud_rate_pct,
        ROUND(SUM(CASE WHEN is_fraud = 1
              THEN amount ELSE 0 END), 2)                      AS fraud_amount,
        ROUND(SUM(CASE WHEN is_fraud = 0
              THEN amount ELSE 0 END), 2)                      AS legit_amount,
        ROUND(
            CASE WHEN SUM(is_fraud) = 0 THEN 0
            ELSE SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END)
                 / SUM(is_fraud)
            END, 2
        )                                                      AS avg_fraud_amount
    FROM base
    GROUP BY category
),

-- Fraud breakdown by US state
-- Which states have the highest fraud rates?
by_state AS (
    SELECT
        'state'                                                AS dimension,
        state                                                  AS value,
        COUNT(*)                                               AS total_txns,
        SUM(is_fraud)                                          AS fraud_txns,
        COUNT(*) - SUM(is_fraud)                               AS legit_txns,
        ROUND(SUM(is_fraud) / COUNT(*) * 100, 3)              AS fraud_rate_pct,
        ROUND(SUM(CASE WHEN is_fraud = 1
              THEN amount ELSE 0 END), 2)                      AS fraud_amount,
        ROUND(SUM(CASE WHEN is_fraud = 0
              THEN amount ELSE 0 END), 2)                      AS legit_amount,
        ROUND(
            CASE WHEN SUM(is_fraud) = 0 THEN 0
            ELSE SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END)
                 / SUM(is_fraud)
            END, 2
        )                                                      AS avg_fraud_amount
    FROM base
    GROUP BY state
),

-- Fraud breakdown by hour of day
-- Are fraudsters more active at 3am than 3pm?
by_hour AS (
    SELECT
        'hour_of_day'                                          AS dimension,
        LPAD(transaction_hour::VARCHAR, 2, '0')               AS value,
        COUNT(*)                                               AS total_txns,
        SUM(is_fraud)                                          AS fraud_txns,
        COUNT(*) - SUM(is_fraud)                               AS legit_txns,
        ROUND(SUM(is_fraud) / COUNT(*) * 100, 3)              AS fraud_rate_pct,
        ROUND(SUM(CASE WHEN is_fraud = 1
              THEN amount ELSE 0 END), 2)                      AS fraud_amount,
        ROUND(SUM(CASE WHEN is_fraud = 0
              THEN amount ELSE 0 END), 2)                      AS legit_amount,
        ROUND(
            CASE WHEN SUM(is_fraud) = 0 THEN 0
            ELSE SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END)
                 / SUM(is_fraud)
            END, 2
        )                                                      AS avg_fraud_amount
    FROM base
    GROUP BY transaction_hour
)

-- Combine all three dimensions into one table
-- Power BI filters by the dimension column to switch between views
SELECT * FROM by_category

UNION ALL 

SELECT * FROM by_state

UNION ALL 

SELECT * FROM by_hour