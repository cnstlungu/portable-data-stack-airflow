with products as (

SELECT  product_id, product_name, g.id as geography_key, product_price, row_number() over (partition by product_id order by e.loaded_timestamp desc ) as rn 
from {{ref('src_products')}} e
join {{ref('geography')}} g on g.cityname = e.product_city

)

select product_id, product_name, geography_key, product_price::numeric AS product_price

from products

where rn = 1
