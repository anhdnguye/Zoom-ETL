from airflow import DAG
from airflow.operators.python import PythonOperator

import logging
from datetime import datetime, timedelta
import requests
import json
import base64


def oauth_(**kwargs):
    info_file = '/opt/airflow/keys.json'

    with open(info_file, 'r') as jsonFile:
        myKeys = json.load(jsonFile)

    key = myKeys['Client_ID'] + ':' + myKeys['Client_Secret']
    encodedBytes = base64.b64encode(key.encode('utf-8'))
    auth = str(encodedBytes, "utf-8")

    Refresh_Endpoint = 'https://zoom.us/oauth/token?grant_type=account_credentials&account_id=' + myKeys['account_ID']

    Header = {
        'Authorization': 'Basic ' + auth
    }

    response = requests.post(url=Refresh_Endpoint, headers=Header).json()

    myKeys['access_token'] = response['access_token']

    jsonFile = open(info_file, 'w+')
    jsonFile.write(json.dumps(myKeys))
    jsonFile.close()

    kwargs['ti'].xcom_push(key='access_token', value=response['access_token'])

    logging.info(f'Completed!\n{response['access_token']}')


with DAG(
        'OAuth_Zoom',
        default_args={
            'owner': 'Anh',
            'depends_on_past': False,
            'email': ['anhnguyen@westcliff.edu'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Generating Access Token',
        start_date=datetime(2024, 5, 20),
        schedule='*/30 * * * *',
        catchup=False,
        tags=['Zoom']

) as dag:
    oauth_ = PythonOperator(
        task_id='run_OAuth',
        python_callable=oauth_,
        provide_context=True,
        dag=dag
    )
