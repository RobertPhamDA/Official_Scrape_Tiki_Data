create or replace view categoryInfor as (
select
category_code
,min(category_name) as category_name
from (
SELECT
    category_item->>0 AS category_code,
    category_item->>1 AS category_name
FROM
    tiki_product_detail,
    LATERAL jsonb_array_elements(categories) AS category_item) sub
group by 
category_code
)