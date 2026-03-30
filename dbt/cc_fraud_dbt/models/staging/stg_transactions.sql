{{config(
    materialized='incremental',
    unique_key='trans_num',
    incremental_strategy='merge',
    on_schema_change='fail'
) }}

WITH source AS (
    SELECT *,
        TO_TIMESTAMP(
            LTRIM(REPLACE(trans_date_trans_time, 'Z ', '')),
            'YYYY-MM-DD HH24:MI:SS.FF9'
        ) AS parsed_timestamp,
        SHA2(r.cc_num, 256) AS cc_num_token_raw
    FROM {{ source('staging', 'raw_transactions') }} r
    {% if is_incremental() %}
      WHERE load_timestamp > (SELECT COALESCE(MAX(load_timestamp), '1990-01-01') FROM {{ this }})
    {% endif %}
),

cleaned as(
    SELECT
        trans_num,
        parsed_timestamp                        AS transaction_at,
        DATE(parsed_timestamp)                 AS transaction_date,
        HOUR(parsed_timestamp)                 AS transaction_hour,
        DAYOFWEEK(parsed_timestamp)            AS day_of_week,
        cc_num                                 AS cc_num,
        cc_num_token_raw                       AS cc_num_token,
        merchant,
        LOWER(TRIM(category))                  AS category,
        ROUND(amt::FLOAT, 2)                   AS amount,
        first || ' ' || last                   AS customer_name,
        gender,
        city,
        state,
        zip,
        lat,
        long,
        city_pop,
        job,
        TO_DATE(
            LTRIM(REPLACE(DOB, 'Z ', '')),
            'YYYY-MM-DD HH24:MI:SS.FF9'
        ) AS date_of_birth,
        DATEDIFF('year',
            TO_DATE(LTRIM(REPLACE(DOB, 'Z ', '')), 'YYYY-MM-DD HH24:MI:SS.FF9'),
            CURRENT_DATE()
        ) AS customer_age,
        merch_lat,
        merch_long,
        is_fraud::INTEGER                      AS is_fraud,
        load_timestamp
    FROM source
    WHERE trans_num IS NOT NULL
        AND amt > 0
    
)

select * from cleaned