INSERT INTO warehouse.dim_product(originalproductid, productname, geographykey, price )
select product_id, name, geographykey , price

from staging.product e
where product_id not in (select originalproductid from warehouse.dim_product);

UPDATE warehouse.dim_product w
set productname = e.name, geographykey =  e.geographykey, price = e.price, loaded_timestamp=now()
FROM staging.product e 
where e.product_id = w.originalproductid and 

(w.productname <> e.name or w.geographykey <> e.geographykey or w.price <> e.price)
