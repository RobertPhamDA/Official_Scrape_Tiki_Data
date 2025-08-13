import pandas as pd
import requests
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
import os
import time
import random
import hashlib



def safe_get(dct, *keys):
    """Truy cập an toàn vào dict lồng nhau."""
    for key in keys:
        if isinstance(dct, dict):
            dct = dct.get(key)
        else:
            return None
    return dct



def extract_products_id_tiki():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:83.0)',
        'Accept': 'application/json',
        'Referer': 'https://tiki.vn/',
        'x-guest-token': '8jWSuIDBb2NGVzr6hsUZXpkP1FRin7lY'
    }

    params = {
        'limit': '48',
        'include': 'sale-attrs,badges,product_links,brand,category,stock_item,advertisement',
        'aggregations': '1',
        'trackity_id': '70e316b0-96f2-dbe1-a2ed-43ff60419991',
        'category': '8322',  # sách
        'page': '1',
        'src': 'c8322',
        'urlKey': 'nha-sach-tiki',
    }

    product_id = []

    for i in range(1, 21):
        params['page'] = i
        response = requests.get('https://tiki.vn/api/v2/products', headers=headers, params=params)
        if response.status_code == 200:
            print(f'Request page {i} success.')
            for record in response.json().get('data', []):
                product_id.append({'id': record.get('id')})
        else:
            print(f'Failed to get product list on page {i}: {response.status_code}')
        time.sleep(random.uniform(1.5, 3))

    df = pd.DataFrame(product_id)
    return df.drop_duplicates(subset=['id'])




def parser_product(json):
    # Lấy publisher & author từ specifications
    publisher_id, publisher_name = None, None
    author_id, author_name = None, None

    for group in json.get("specifications", []):
        for attr in group.get("attributes", []):
            name_lower = attr.get("name", "").strip().lower()
            if name_lower == "nhà xuất bản":
                publisher_id = attr.get("id")
                publisher_name = attr.get("value")
            elif name_lower == "tác giả":
                author_id = attr.get("id")
                author_name = attr.get("value")

    # Fallback nếu specifications không có
    if not publisher_name:
        publisher_name = safe_get(json, 'publisher', 'name')
    # Tạo publisher_id cố định từ publisher_name
    if publisher_name:
        publisher_id = hashlib.md5(publisher_name.encode('utf-8')).hexdigest()
    else:
        publisher_id = None

    if not author_name:
        authors = json.get('authors', [])
        if authors:
            author_id = authors[0].get('id')
            author_name = authors[0].get('name')

    # Lấy category
    categories = json.get("categories")
    if isinstance(categories, list):
        category_info = [(c.get("id"), c.get("name")) for c in categories]
    elif isinstance(categories, dict):
        category_info = [(categories.get("id"), categories.get("name"))]
    else:
        category_info = []

    return {
        'id': json.get('id'),
        'sku': json.get('sku'),
        'product_name': json.get('name'),
        'short_description': json.get('short_description'),
        'price': json.get('price'),
        'list_price': json.get('list_price'),
        'price_usd': json.get('original_price'),
        'discount': json.get('discount'),
        'discount_rate': json.get('discount_rate'),
        'review_count': json.get('review_count'),
        'order_count': json.get('all_time_quantity_sold'),
        'inventory_status': json.get('thumbnail_url'),
        'is_visible': json.get('is_visible'),
        'stock_item_qty': safe_get(json, 'stock_item', 'qty'),
        'stock_item_max_sale_qty': safe_get(json, 'stock_item', 'max_sale_qty'),
        'brand_id': safe_get(json, 'brand', 'id'),
        'brand_name': safe_get(json, 'brand', 'name'),
        'publisher_id': publisher_id,
        'publisher_name': publisher_name,
        'author_id': author_id,
        'author_name': author_name,
        'categories': category_info
    }

def scraper_tiki_product_detail():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:83.0)',
        'Accept': 'application/json',
        'Referer': 'https://tiki.vn/',
        'x-guest-token': '8jWSuIDBb2NGVzr6hsUZXpkP1FRin7lY',
    }

    df_id = extract_products_id_tiki()  # <- Hàm này bạn đã có
    p_ids = df_id['id'].tolist()
    print(f'Total IDs: {len(p_ids)}')

    result = []
    for pid in p_ids:
        try:
            response = requests.get(f'https://tiki.vn/api/v2/products/{pid}',
                                    headers=headers, params={'platform': 'web'})
            if response.status_code == 200:
                print(f'Success: {pid}')
                data = response.json()
                result.append(parser_product(data))
            else:
                print(f'Failed to get detail for {pid}: {response.status_code}')
        except Exception as e:
            print(f'Error with {pid}: {e}')
        time.sleep(random.uniform(1.5, 3))

    df_product = pd.DataFrame(result)
    df_product['insertedDate'] = pd.Timestamp.now()
    return df_product.drop_duplicates(subset=['id'])



def load_to_postgres_tiki_product_detail(df_product: pd.DataFrame):
    db_user = os.getenv("POSTGRES_USER", "airflow")
    db_pass = os.getenv("POSTGRES_PASSWORD", "airflow")
    db_host = os.getenv("POSTGRES_HOST", "postgres")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "airflow")

    engine = create_engine(f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
    metadata = MetaData(bind=engine)
    table = Table("tiki_product_detail", metadata, autoload_with=engine)

    with engine.begin() as conn:
        for record in df_product.to_dict(orient="records"):
            stmt = insert(table).values(**record)
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_=record
            )
            conn.execute(upsert_stmt)
