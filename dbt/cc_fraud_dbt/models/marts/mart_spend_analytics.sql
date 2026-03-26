{{ config(materialized='table') }}

WITH base AS (
  SELECT * FROM {{ ref('stg_transactions') }}
),

by_category AS (
  SELECT
    category,
    COUNT(*)                                                    AS txn_count,
    ROUND(SUM(amount), 2)                                      AS total_spend,
    ROUND(AVG(amount), 2)                                      AS avg_txn_amount,
    ROUND(MAX(amount), 2)                                      AS max_txn_amount,
    COUNT(DISTINCT cc_num_token)                               AS unique_customers_total,
    COUNT(DISTINCT CASE WHEN is_fraud = 0 
          THEN cc_num_token END)                               AS unique_spenders
  FROM base
  GROUP BY 1
)

SELECT * FROM by_category
ORDER BY total_spend DESC


