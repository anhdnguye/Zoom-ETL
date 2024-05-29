from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

import logging
import time
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

default_args={
    'owner': 'Anh',
    'depends_on_past': False,
    'email': ['anhnguyen@westcliff.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    }

@dag(schedule='@once', default_args=default_args, start_date=datetime(2024, 5, 21),
     catchup=False, tags=['Zoom'], description='Extracting Data from Zoom')
def etl_process():

    @task
    def get_all_users(**kwargs):
        myToken = kwargs['ti'].xcom_pull(dag_id='OAuth_Zoom', task_ids='run_OAuth', key='access_token')

        lstuser_endpoint = 'https://api.zoom.us/v2/users'
        params = {
            'page_size' : 300,
            'status' : 'active'
        }
        header = {'Authorization' : 'Bearer ' + myToken}
        response = requests.get(url=lstuser_endpoint, headers=header, params=params).json()
        next_page_token = response['next_page_token']

        user_email = []

        while next_page_token:
            response = requests.get(url=lstuser_endpoint, headers=header, params=params).json()
            next_page_token = response['next_page_token']
            params['next_page_token'] = next_page_token

            user_ids = [(user['email']) for user in response['users']]
            user_email.extend(user_ids)
        return user_email

    @task
    def get_user_info(user_email, **kwargs):
        user_endpoint = 'https://api.zoom.us/v2/users'

        myToken = kwargs['ti'].xcom_pull(dag_id='OAuth_Zoom', task_ids='run_OAuth', key='access_token')
        header = {'Authorization' : 'Bearer ' + myToken}
        users_data = []
        for email in user_email:
            try:
                response = requests.get(url=user_endpoint + '/' + email, headers=header)
                data_ = response.json()
                users_data.append(data_)
            except:
                if response.status_code in [401]:
                    myToken = kwargs['ti'].xcom_pull(dag_id='OAuth_Zoom', task_ids='run_OAuth', key='access_token')
                    response = requests.get(url=user_endpoint + '/' + email, headers=header)
                    data_ = response.json()
                    users_data.append(data_)

etl_process()

# with DAG(
#     'Zoom_ETL',
#     default_args={
#         'owner': 'Anh',
#         'depends_on_past': False,
#         'email': ['anhnguyen@westcliff.edu'],
#         'email_on_failure': False,
#         'email_on_retry': False,
#         'retries': 1,
#         'retry_delay': timedelta(minutes=5)
#     },
#     description='Extracting Data from Zoom',
#     start_date=datetime(2024, 5, 21),
#     schedule='@once',
#     catchup=False,
#     tags=['Zoom']

# ) as dag:
#     get_all_users = PythonOperator (
#         task_id='get_users',
#         python_callable=get_all_users,
#     )

#     get_user_info = PythonOperator (
#         task_id='get_info',
#         python_callable=get_user_info
#     )

#     get_all_users >> get_user_info