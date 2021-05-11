insert into warehouse.dim_customer(originalcustomerid, customerfirstname, customerlastname, customeremail, salesagentkey)
select customer_id, first_name, last_name, email, salesagentkey
from staging.customers c
where c.customer_id not in (select originalcustomerid from warehouse.dim_customer);


update warehouse.dim_customer w
set customerfirstname = c.first_name, customerlastname = c.last_name, customeremail = c.email, loaded_timestamp=now()
from staging.customers c
where w.originalcustomerid = c.customer_id and
(w.customerfirstname <> c.first_name or w.customerlastname <> c.last_name or w.customeremail <> c.email);