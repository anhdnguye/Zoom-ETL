# from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

import os
import logging
import time
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv

# database credential
HOST_NAME = os.getenv('ZOOM_HOST_NAME')
DATABASE = os.getenv('ZOOM_DATABASE')
USER_NAME = os.getenv('ZOOM_USER')
PASSWORD = os.getenv('ZOOM_PASS')
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

@dag('ETL', schedule='@once', default_args=default_args, start_date=datetime(2024, 5, 21),
     catchup=False, tags=['Zoom'], description='Extracting Data from Zoom')
def etl_process():

    @task
    def get_all_users(**kwargs):
        myToken = kwargs['ti'].xcom_pull(dag_id='OAuth_Zoom', task_ids='run_OAuth',
                                         key='access_token', include_prior_dates=True)

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
        kwargs['ti'].xcom_push(key='lstUsers', value=user_email)
        # return user_email

    @task
    def get_user_info(**kwargs):
        user_endpoint = 'https://api.zoom.us/v2/users'

        myToken = kwargs['ti'].xcom_pull(dag_id='OAuth_Zoom', task_ids='run_OAuth',
                                         key='access_token', include_prior_dates=True)
        user_email = kwargs['ti'].xcom_pull(task_ids='get_all_users',
                                            key='lstUsers', include_prior_dates=True)
        header = {'Authorization' : 'Bearer ' + myToken}
        users_data = []
        for email in user_email:
            try:
                response = requests.get(url=user_endpoint + '/' + email, headers=header)
                data_ = response.json()
                users_data.append(data_)
            except:
                if response.status_code in [401]:
                    myToken = kwargs['ti'].xcom_pull(dag_id='OAuth_Zoom', task_ids='run_OAuth',
                                                     key='access_token', include_prior_dates=True)
                    response = requests.get(url=user_endpoint + '/' + email, headers=header)
                    data_ = response.json()
                    users_data.append(data_)
        logging.info(f'number of users:{len(users_data)}')
    
    get_all_users() >> get_user_info()

etl_process()
