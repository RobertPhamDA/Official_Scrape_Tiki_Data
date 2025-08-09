create or replace view CustomerInfor as (
select
distinct
customer_id
,customer_name
from 
tiki_comment_detail tcd
)