CREATE OR REPLACE VIEW publisherInfor as (
SELECT DISTINCT
    publisher_id,
    publisher_name
FROM tiki_product_detail);