create or replace view ProductInfor as (
select  
id
,sku
,product_name
,short_description
,price
,list_price
,price_usd
,discount
,discount_rate
,order_count
,review_count
,inventory_status
,stock_item_qty
,stock_item_max_sale_qty
,publisher_id
,category_item->>0 AS category_id
,author_id
from tiki_product_detail,
    LATERAL jsonb_array_elements(categories) AS category_item
)