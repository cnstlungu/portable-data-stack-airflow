{{
config(
materialized = 'table',
unique_key = 'customer_key'
)
}}

select customer_key, customer_first_name, customer_last_name, customer_email, sales_agent_key
from {{ref('staging_customers')}}