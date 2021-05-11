
INSERT INTO warehouse.dim_salesagent(originalresellerid, resellername, commissionpct)
select reseller_id, reseller_name, commission_pct
from staging.resellers r
where reseller_id not in (select originalresellerid from warehouse.dim_salesagent);



UPDATE warehouse.dim_salesagent w
set resellername = r.reseller_name, commissionpct = r.commission_pct, loaded_timestamp=now()
FROM staging.resellers r 
where r.reseller_id = w.originalresellerid and (w.resellername <> r.reseller_name or w.commissionpct <> r.commission_pct) 
