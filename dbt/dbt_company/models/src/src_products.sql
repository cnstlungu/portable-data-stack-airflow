WITH raw_products AS (

    SELECT * FROM {{source('import', 'products' )}}
)

SELECT product_id, name AS product_name, price AS product_price, city AS product_city, loaded_timestamp

FROM raw_products