truncate staging.resellers; 

with resellers as (

SELECT reseller_id, reseller_name, commission_pct, row_number() over (partition by reseller_id order by loaded_timestamp desc ) as rn 
from import.resellers

)

insert into staging.resellers(reseller_id, reseller_name, commission_pct)
select reseller_id, reseller_name, commission_pct

from resellers

where rn =1;

