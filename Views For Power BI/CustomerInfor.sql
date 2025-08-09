create or replace view CustomerInfor as (
select
customer_id
,min(customer_name) as customer_name
from 
tiki_comment_detail tcd
group by 
customer_id
)