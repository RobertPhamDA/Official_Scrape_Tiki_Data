create or replace view CommentDetail as (
select
id
,product_id
,title
,content
,thank_count
,customer_id
,rating
,created_at as comment_date
,purchased_at
from 
tiki_comment_detail tcd
)