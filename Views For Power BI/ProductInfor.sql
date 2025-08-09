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
,review_count
,inventory_status
,is_visible
,stock_item_qty
,stock_item_max_sale_qty
,brand_id
from tiki_product_detail tpd 
)