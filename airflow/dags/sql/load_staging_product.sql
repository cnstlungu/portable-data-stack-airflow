
truncate staging.product;
with products as (

SELECT product_id, name, g.geographykey, price, row_number() over (partition by product_id order by e.loaded_timestamp desc ) as rn 
from import.products e
join warehouse.dim_geography g on g.cityname = e.city

)
insert into staging.product(product_id, name, geographykey, price)

select product_id, name, geographykey, price

from products

where rn = 1
