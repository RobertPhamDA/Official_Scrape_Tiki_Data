create or replace view BrandInfor as (
select 
distinct 
brand_id
,brand_name
from tiki_product_detail tpd )