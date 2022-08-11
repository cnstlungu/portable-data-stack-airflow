WITH raw_resellercsv AS (

    SELECT * FROM {{source('import', 'resellercsv' )}}
)

SELECT transaction_id, product_name, number_of_purchased_postcards, total_amount, sales_channel, customer_first_name, customer_last_name, customer_email, office_location, created_date, imported_file, loaded_timestamp
FROM raw_resellercsv
