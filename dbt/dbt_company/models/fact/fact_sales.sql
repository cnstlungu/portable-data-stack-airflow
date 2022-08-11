{{ config(
    materialized = 'table'
) }}

SELECT
    customer_key,
    transaction_id,
    product_key,
    channel_key,
    reseller_id,
    bought_date_key,
    geography_key,
    total_amount,
    qty,
    commissionpct,
    commissionpaid,
    product_price
FROM
    {{ ref('staging_transactions') }}
