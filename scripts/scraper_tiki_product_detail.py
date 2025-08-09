import pandas as pd
import requests
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
import os
import time
import random


# ---------------------- Hàm hỗ trợ ----------------------

def safe_get(dct, *keys):
    """Truy cập an toàn vào dict lồng nhau."""
    for key in keys:
        if isinstance(dct, dict):
            dct = dct.get(key)
        else:
            return None
    return dct


# ---------------------- Extract ----------------------

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

    for i in range(1, 10):
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


# ---------------------- Transform ----------------------

def scraper_tiki_product_detail():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:83.0)',
        'Accept': 'application/json',
        'Referer': 'https://tiki.vn/',
        'x-guest-token': '8jWSuIDBb2NGVzr6hsUZXpkP1FRin7lY',
    }

    df_id = extract_products_id_tiki()
    p_ids = df_id['id'].tolist()
    print(f'Total IDs: {len(p_ids)}')

    def parser_product(json):
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
            'order_count': json.get('order_count'),
            'inventory_status': json.get('inventory_status'),
            'is_visible': json.get('is_visible'),
            'stock_item_qty': safe_get(json, 'stock_item', 'qty'),
            'stock_item_max_sale_qty': safe_get(json, 'stock_item', 'max_sale_qty'),
            'brand_id': safe_get(json, 'brand', 'id'),
            'brand_name': safe_get(json, 'brand', 'name'),
        }

    result = []
    for pid in p_ids:
        try:
            response = requests.get(f'https://tiki.vn/api/v2/products/{pid}', headers=headers, params={'platform': 'web'})
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


# ---------------------- Load ----------------------

def load_to_postgres_tiki_product_detail(df_product: pd.DataFrame):
    db_user = os.getenv("POSTGRES_USER", "airflow")
    db_pass = os.getenv("POSTGRES_PASSWORD", "airflow")
    db_host = os.getenv("POSTGRES_HOST", "postgres")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "airflow")

    engine = create_engine(f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
    df_product.to_sql("tiki_product_detail", engine, if_exists="replace", index=False)
