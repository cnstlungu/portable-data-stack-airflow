
TRUNCATE staging.customers;

 with 

customers_main as (

    select customer_id, first_name, last_name, email from import.customers

),

customers_csv  as (

    select  
    
    customer_first_name, 
    customer_last_name ,
    customer_email,
    split_part(split_part(imported_file, '_', 3),'.',1)::int as reseller_id,
    transaction_id

    from import.resellercsv
)
,

customers_xml as (


    select customer_first_name, customer_last_name, customer_email,
    reseller_id,
    transaction_id
    
     from staging.resellerxmlextracted r
), 

customers as (


select concat(reseller_id,'-', transaction_id) as customer_id, customer_first_name, customer_last_name, customer_email, reseller_id  from customers_csv

union 

select concat(reseller_id,'-', transaction_id) as customer_id, customer_first_name, customer_last_name, customer_email, reseller_id  from customers_xml

union

select cast(customer_id as varchar(10)) as customer_id, first_name, last_name, email, -1 as resellerid  from customers_main
)


insert into staging.customers(customer_id, first_name, last_name, email, salesagentkey)

select customer_id, customer_first_name, customer_last_name, customer_email, coalesce(r.salesagentkey, -1) as salesagentkey

from customers c
left join warehouse.dim_salesagent r on r.originalresellerid = c.reseller_id;