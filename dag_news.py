from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import importlib
from datetime import datetime, timedelta
import os

DAG_DESC = "DAG to extract news from the NewsAPI and insert them into an SQLite database"
OWNER = "Adenilson Castro"
START_DATE = datetime(2021, 7, 4)
SCHEDULE = '0 17 * * *'

DAG_ID = "news_api"
AIRFLOW_HOME = os.getenv('airflow_home')


def start(**kwargs) -> None:
    print("DAG started")


def extract(**kwargs) -> None:
    print("DAG extract")
    news_extract = importlib.import_module('news_etl')
    news_extract.main()


default_args = {
    'owner': OWNER,
    'depends_on_past': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'priority_weight': 1
}

with DAG(
    DAG_ID,
    default_args=default_args,
    description=DAG_DESC,
    schedule_interval=SCHEDULE,
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='start',
        python_callable=start,
        provide_context=True
    )

    etl_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True
    )

start_task >> etl_task
