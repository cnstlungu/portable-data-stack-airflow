with raw_customers as (

    SELECT * from {{source('import', 'customers' )}}
)

select customer_id, first_name , last_name , email

from raw_customers