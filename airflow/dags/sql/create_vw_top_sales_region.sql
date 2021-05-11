


CREATE OR REPLACE VIEW  warehouse.vw_topproducts_region as

with cte as (

select g.regionname, e.productname, sum(s.nbrboughtpostcards) as PostcardsBought

from warehouse.fact_sales s

join warehouse.dim_product e on s.productkey = e.productkey

join warehouse.dim_geography g on g.geographykey = e.geographykey

group by g.regionname, e.productname

), ranked as (

select regionname, productname, PostcardsBought,

rank() over (partition by regionname order by PostcardsBought desc) as rank

from cte
)

select regionname, productname, PostcardsBought from ranked where rank <= 5

