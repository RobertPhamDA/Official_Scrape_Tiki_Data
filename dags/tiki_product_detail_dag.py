import sys
sys.path.append('/opt/airflow')  # Đảm bảo Airflow tìm thấy module trong container

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.scraper_tiki_product_detail import scraper_tiki_product_detail, load_to_postgres_tiki_product_detail
import pandas as pd

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="tiki_scraper_product_detail_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["tiki", "products", "ETL"]
) as dag:

    def extract(**kwargs):
        df = scraper_tiki_product_detail()
        df_json = df.to_json(orient="split")  # Chuyển sang định dạng JSON rõ ràng
        kwargs['ti'].xcom_push(key='products_df', value=df_json)

    def load(**kwargs):
        df_json = kwargs['ti'].xcom_pull(key='products_df', task_ids='extract_data')
        df = pd.read_json(df_json, orient="split")  # Cần đúng định dạng với lúc push
        load_to_postgres_tiki_product_detail(df)

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load,
        provide_context=True,
    )

    extract_task >> load_task
