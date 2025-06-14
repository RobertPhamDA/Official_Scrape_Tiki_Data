import requests
import pandas as pd
from sqlalchemy import create_engine
import os

def extract_products(sbd_start: int = 34000001, sbd_end: int = 34000210) -> pd.DataFrame:
    sbd_start = 34000001
    sbd_end = 34000210
    df_list = []
    session = requests.Session()

    for sbd in range(sbd_start, sbd_end):
        url = f"https://vietnamnet.vn/giao-duc/diem-thi/tra-cuu-diem-thi-tot-nghiep-thpt/2023/{sbd}.html"
        response = session.get(url)
        if response.ok:
            tables = pd.read_html(url)
            desired_table = tables[0].transpose()
            desired_table.columns = desired_table.iloc[0]
            desired_table = desired_table[1:]
            desired_table['SBD'] = sbd
            df_list.append(desired_table)

    session.close()  # Close the session after use

    if df_list:
        df = pd.concat(df_list, ignore_index=True)
        return df
    else:
        return pd.DataFrame()  # Trả về DataFrame rỗng nếu không có dữ liệu

def load_to_postgres(df: pd.DataFrame):
    db_user = os.getenv("POSTGRES_USER", "airflow")
    db_pass = os.getenv("POSTGRES_PASSWORD", "airflow")
    db_host = os.getenv("POSTGRES_HOST", "postgres")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "airflow")

    engine = create_engine(f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
    df.to_sql("ecommerce_products", engine, if_exists="replace", index=False)
