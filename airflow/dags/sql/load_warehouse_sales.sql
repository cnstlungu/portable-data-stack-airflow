insert into warehouse.fact_sales
(
customerkey,
originaltransactionid,
productkey,
channelkey,
salesagentkey,
boughtdatekey,
geographykey,
totalamount,
nbrboughtpostcards,
commissionpct,
commissionpaid,
price
)

select 
customer_key,
transaction_id,
productkey,
channelkey,
salesagentkey,
boughtdatekey,
geographykey,
total_amount,
qty,
commissionpct,
commissionpaid,
price


from staging.transactions s

where 
( s.transaction_id not in (select originaltransactionid from warehouse.fact_sales) and s.salesagentkey not in (select salesagentkey from warehouse.fact_sales)) OR

( s.salesagentkey not in (select salesagentkey from warehouse.fact_sales) );




update warehouse.fact_sales w

set

customerkey = t.customer_key     ,
productkey = t.productkey             ,
channelkey = t.channelkey         ,
boughtdatekey = t.boughtdatekey   ,
geographykey = t.geographykey     ,
totalamount = t.total_amount     ,
nbrboughtpostcards = t.qty         ,
commissionpct = t.commissionpct   ,
commissionpaid = t.commissionpaid ,
price          = t.price,
loaded_timestamp = now()        

from staging.transactions t

where (w.originaltransactionid = t.transaction_id and w.salesagentkey = t.salesagentkey)
AND (
w.customerkey <> t.customer_key    OR
w.productkey <> t.productkey             OR
w.channelkey <> t.channelkey         OR
w.boughtdatekey <> t.boughtdatekey   OR
w.geographykey <> t.geographykey     OR
w.totalamount <> t.total_amount     OR
w.nbrboughtpostcards <> t.qty                       OR
w.commissionpct <> t.commissionpct   OR
w.commissionpaid <> t.commissionpaid OR
w.price          <> t.price         
  );

