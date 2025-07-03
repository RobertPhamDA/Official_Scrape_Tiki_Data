import sys
sys.path.append('/opt/airflow')
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.scraper_tiki_product_detail import scraper_tiki_product_detail, load_to_postgres_tiki_product_detail

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

    def extract(**context):
        df = scraper_tiki_product_detail()
        context['ti'].xcom_push(key='products_df', value=df.to_json())

    def load(**context):
        from pandas import read_json
        df_json = context['ti'].xcom_pull(key='products_df', task_ids='extract_data')
        df = read_json(df_json)
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
