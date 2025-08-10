create or replace view authorInfor as (
select distinct 
author_id
,author_name
from tiki_product_detail
)