{{ config(materialized='table') }}

WITH txn_base AS (
  SELECT * FROM {{ ref('stg_transactions') }}
  WHERE is_fraud = 0
),

rfm_raw AS (
  SELECT
    cc_num_token,
    MIN(customer_name)                                        AS customer_name,
    MAX(state)                                               AS state,
    MAX(customer_age)                                        AS customer_age,
    MAX(transaction_date)                                    AS last_transaction_date,
    DATEDIFF('day', MAX(transaction_date), CURRENT_DATE())  AS recency_days,
    COUNT(DISTINCT trans_num)                                AS frequency,
    ROUND(SUM(amount), 2)                                    AS monetary_value,
    ROUND(AVG(amount), 2)                                    AS avg_order_value
  FROM txn_base
  GROUP BY 1
),

rfm_scored AS (
  SELECT *,
    NTILE(5) OVER (ORDER BY recency_days DESC)  AS r_score,
    NTILE(5) OVER (ORDER BY frequency)          AS f_score,
    NTILE(5) OVER (ORDER BY monetary_value)     AS m_score
  FROM rfm_raw
),

rfm_segmented AS (
  SELECT *,
    (r_score + f_score + m_score) AS rfm_total,
    CASE
      WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champion'
      WHEN r_score >= 3 AND f_score >= 3                  THEN 'Loyal Customer'
      WHEN r_score >= 4 AND f_score <= 2                  THEN 'New Customer'
      WHEN r_score <= 2 AND f_score >= 3                  THEN 'At Risk'
      WHEN r_score = 1                                    THEN 'Lost Customer'
      ELSE                                                     'Potential Loyalist'
    END AS customer_segment
  FROM rfm_scored
)

SELECT * FROM rfm_segmented
