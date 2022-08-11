WITH trans_main AS (
  SELECT
    {{ dbt_utils.surrogate_key(
      [ '0', 'customer_id']
    ) }} AS customer_key,
    customer_id,
    transaction_id,
    product_id,
    amount,
    qty,
    channel_id,
    bought_date,
    ROW_NUMBER() over (
      PARTITION BY transaction_id
      ORDER BY
        loaded_timestamp DESC
    )
  FROM
    {{ source(
      'import',
      'transactions'
    ) }}
),
resellers_csv AS (
  SELECT
    SPLIT_PART(SPLIT_PART(imported_file, '.', '-2'), '_', '-1') :: INT AS reseller_id,
    transaction_id,
    product_name,
    total_amount,
    number_of_purchased_postcards,
    created_date,
    office_location,
    sales_channel,
    loaded_timestamp
  FROM
    {{ source(
      'import',
      'resellercsv'
    ) }}
),
trans_csv AS (
  SELECT
    {{ dbt_utils.surrogate_key(
      [ 'reseller_id', 'transaction_id']
    ) }} AS customer_key,
    transaction_id,
    reseller_id,
    product_name,
    total_amount,
    number_of_purchased_postcards,
    created_date,
    office_location,
    sales_channel,
    ROW_NUMBER() over (
      PARTITION BY transaction_id,
      reseller_id
      ORDER BY
        loaded_timestamp DESC
    ) AS rn
  FROM
    resellers_csv
),
trans_xml AS (
  SELECT
    {{ dbt_utils.surrogate_key(
      [ 'reseller_id', 'transaction_id']
    ) }} AS customer_key,
    reseller_id,
    transaction_id,
    product_name,
    total_amount,
    no_purchased_postcards,
    date_bought,
    sales_channel,
    office_location,
    ROW_NUMBER() over (
      PARTITION BY transaction_id,
      reseller_id
      ORDER BY
        loaded_timestamp DESC
    ) AS rn
  FROM
    {{ source(
      'preprocessed',
      'resellerxmlextracted'
    ) }}
)
SELECT
  t.customer_key,
  transaction_id,
  e.product_key,
  C.channel_key,
  0 AS reseller_id,
  to_char(
    bought_date,
    'YYYYMMDD'
  ) :: INT AS bought_date_key,
  amount AS total_amount,
  qty,
  e.product_price,
  e.geography_key,
  NULL AS commissionpaid,
  NULL AS commissionpct
FROM
  trans_main t
  JOIN {{ ref('dim_product') }}
  e
  ON t.product_id = e.product_key
  JOIN {{ ref('dim_channel') }} C
  ON t.channel_id = C.channel_key
  JOIN {{ ref('dim_customer') }}
  cu
  ON t.customer_key = cu.customer_key
UNION ALL
SELECT
  t.customer_key,
  transaction_id,
  product_key,
  channel_key,
  t.reseller_id,
  to_char(
    created_date,
    'YYYYMMDD'
  ) :: INT AS bought_date_key,
  total_amount,
  number_of_purchased_postcards,
  e.product_price,
  e.geography_key,
  s.commission_pct * total_amount AS commisionpaid,
  s.commission_pct AS commission_pct
FROM
  trans_csv t
  JOIN {{ ref('dim_product') }}
  e
  ON t.product_name = e.product_name
  JOIN {{ ref('dim_channel') }} C
  ON t.sales_channel = C.channel_name
  JOIN {{ ref('dim_customer') }}
  cu
  ON t.customer_key = cu.customer_key
  JOIN {{ ref('dim_salesagent') }}
  s
  ON t.reseller_id = s.original_reseller_id
WHERE
  rn = 1
UNION ALL
SELECT
  t.customer_key,
  transaction_id,
  e.product_key,
  C.channel_key,
  t.reseller_id,
  to_char(
    date_bought,
    'YYYYMMDD'
  ) :: INT AS bought_date_key,
  total_amount,
  no_purchased_postcards,
  e.product_price,
  e.geography_key,
  s.commission_pct * total_amount AS commisionpaid,
  s.commission_pct
FROM
  trans_xml t
  JOIN {{ ref('dim_product') }}
  e
  ON t.product_name = e.product_name
  JOIN {{ ref('dim_channel') }} C
  ON t.sales_channel = C.channel_name
  JOIN {{ ref('dim_customer') }}
  cu
  ON t.customer_key = cu.customer_key
  JOIN {{ ref('dim_salesagent') }}
  s
  ON t.reseller_id = s.original_reseller_id
WHERE
  rn = 1
