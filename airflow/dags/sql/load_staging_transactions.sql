
truncate table staging.transactions;

with trans_main as (
select 

customer_id,
transaction_id,
product_id,
amount,
qty,
channel_id,
bought_date,
row_number() over (partition by transaction_id order by loaded_timestamp desc )
from import.transactions

)

insert into staging.transactions
(Customer_Key,
Transaction_ID ,
ProductKey ,
ChannelKey ,
SalesAgentKey,
BoughtDateKey ,
Total_Amount ,
Qty ,
Price,
geographykey,
CommissionPaid,
commissionpct
)

select 
CustomerKey,
transaction_id,
ProductKey,
ChannelKey,
-1 as SalesAgentKey,
to_char(bought_date,'YYYYMMDD')::int,
amount,
qty,
e.price,
e.geographykey,
NULL as commissionpaid,
NULL as commissionpct


from trans_main t
join warehouse.dim_product e on t.product_id = e.originalproductid
join warehouse.dim_channel c on t.channel_id = c.originalchannelid
join warehouse.dim_customer cu on cast(t.customer_id as varchar(32)) = cu.originalcustomerid;


with
 resellers_csv as (

select 

transaction_id,
product_name,
total_amount,
number_of_purchased_postcards,
created_date,
office_location,
sales_channel,
split_part(split_part(imported_file,'_',3),'.',1)::int as reseller_id,
loaded_timestamp

from import.resellercsv

),

trans_csv as (

select 
concat(reseller_id,'-', transaction_id) as customer_id,
reseller_id,
transaction_id,
product_name,
total_amount,
number_of_purchased_postcards,
created_date,
office_location,
sales_channel,
row_number() over (partition by transaction_id, reseller_id order by loaded_timestamp desc ) as rn
 from resellers_csv
)

insert into staging.transactions
(
customer_key,
transaction_id,
productkey,
channelkey,
salesagentkey,
boughtdatekey,
total_amount,
qty,
price,
geographykey,
CommissionPaid,
CommissionPct
)


select 

CustomerKey, 
transaction_id,
Productkey,
ChannelKey,
s.salesagentkey,
to_char(created_date,'YYYYMMDD')::int as boughtdatekey,
total_amount,
number_of_purchased_postcards,
e.price,
e.geographykey,
s.commissionpct*total_amount as commisionpaid,
s.commissionpct as commission_pct

from trans_csv t
join warehouse.dim_product e on t.product_name = e.productname
join warehouse.dim_channel c on t.sales_channel = c.channelname
join warehouse.dim_customer cu on t.customer_id =cu.originalcustomerid 
join warehouse.dim_salesagent s on s.originalresellerid = t.reseller_id
where rn = 1;

with trans_xml as (

select 
concat(reseller_id,'-', transaction_id) as customer_id,
transaction_id,
product_name,
total_amount,
no_purchased_postcards,
date_bought,
sales_channel,
office_location,
reseller_id,
row_number() over (partition by transaction_id, reseller_id order by loaded_timestamp desc) as rn

from staging.resellerxmlextracted

)

insert into staging.transactions
(
customer_key,
transaction_id,
productkey,
channelkey,
salesagentkey,
boughtdatekey,
total_amount,
qty,
price,
geographykey,
commissionpaid,
commissionpct
)


select 
cu.CustomerKey,
transaction_id,
e.ProductKey,
c.ChannelKey,
s.salesagentkey,
to_char(date_bought,'YYYYMMDD')::int as DateBoughtKey,
total_amount,
no_purchased_postcards,
e.price,
e.geographykey,
s.commissionpct*total_amount as commisionpaid,
s.commissionpct as commission_pct

from trans_xml t
join warehouse.dim_product e on t.product_name = e.productname
join warehouse.dim_channel c on t.sales_channel = c.channelname
join warehouse.dim_customer cu on t.customer_id =cu.originalcustomerid
join warehouse.dim_salesagent s on s.originalresellerid = t.reseller_id
where rn = 1;