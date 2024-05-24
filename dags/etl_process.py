from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import configparser

# database credential
HOST_NAME = 'zoom'
DATABASE = 'zoom_db'
USER_NAME = 'zoom'
PASSWORD = 'zoom'
PORT_ID = 5432


def get_users(**kwargs):
    ti = kwargs['ti']
    url = 'https://api.zoom.us/v2/users?page_size=300'

    myToken = ti.xcom_pull(dag_id='OAuth_Zoom', task_ids='run_OAuth', key='access_token')

    headers = {'Authorization': 'Bearer ' + myToken}
    response = requests.get(url=url, headers=headers).json()

    total_page = int(response['page_count']) + 1
    logger = logging.getLogger(__name__)
    logger.info(total_page)


with DAG(
    'zoom_etl',
    default_args={
        'owner': 'Anh',
        'depends_on_past': False,
        'email': ['anhnguyen@westcliff.edu'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Extracting Data from Zoom',
    start_date=datetime(2024, 6, 1),
    schedule='@once',
    catchup=False,
    tags=['Zoom']

) as dag:
    get_users = PythonOperator (
        task_id='get_users',
        python_callable=get_users,
    )

