{{config(
    materialized='incremental',
    unique_key='trans_num',
    on_schema_change='fail'
) }}

with source as(
    select * from {{ source('staging', 'raw_transactions') }}
    {% if is_incremental() %}
        where load_timestamp > (select MAX(load_timestamp) from {{ this }})
    {% endif %}
),

cleaned as(
    SELECT
        trans_num,
        trans_date_trans_time::TIMESTAMP       AS transaction_at,
        DATE(trans_date_trans_time)            AS transaction_date,
        HOUR(trans_date_trans_time)            AS transaction_hour,
        DAYOFWEEK(trans_date_trans_time)       AS day_of_week,
        cc_num,
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
        DOB::DATE                              AS date_of_birth,
        DATEDIFF('year', DOB::DATE, CURRENT_DATE()) AS customer_age,
        merch_lat,
        merch_long,
        is_fraud::INTEGER                      AS is_fraud,
        load_timestamp
    FROM source
    WHERE trans_num IS NOT NULL
        AND amt > 0
    
)

select * from cleaned