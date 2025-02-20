# from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator

import os
from datetime import datetime, timedelta

import sys
sys.path.insert(1, '/opt/airflow/scripts')
from extract import DataExtractor

from dotenv import load_dotenv
load_dotenv()

# database credential
HOST_NAME = os.getenv('ZOOM_HOST_NAME')
DATABASE = os.getenv('ZOOM_DATABASE')
USER_NAME = os.getenv('ZOOM_USER')
PASSWORD = os.getenv('ZOOM_PASS')
PORT_ID = 5432

default_args={
    'owner': 'Anh',
    'depends_on_past': False,
    'email': ['test@test.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 21)
    }

@dag('ETL', schedule='@once', default_args=default_args,
     catchup=False, tags=['Zoom'], description='Extracting Data from Zoom')
def etl_process():

    @task
    def get_all_users(**kwargs):
        """Task to get all users IDs."""
        extractor = DataExtractor('https://api.zoom.us/v2')
        return extractor.get_all_user_ids()
    
    @task
    def get_user_info(user_id):
        """Task to get user information"""
        extractor = DataExtractor('https://api.zoom.us/v2')
        user_details = extractor.get_user_details(user_id)
        return {"user_id": user_id, "detail": user_details}
    
    @task
    def get_last_run_timestamp():
        """Task to get the last time that the pipeline ran"""
        extractor = DataExtractor('https://api.zoom.us/v2')
        return extractor.get_last_run_timestamp()
    
    @task
    def set_last_run_timestamp():
        """Task to set the current time"""
        extractor = DataExtractor('https://api.zoom.us/v2')
        return extractor.set_last_run_timestamp()
    
    start = DummyOperator(task_id='start')
    

etl_process()
