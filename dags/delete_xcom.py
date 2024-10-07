from airflow import DAG

from airflow import settings
from airflow.utils.dates import days_ago
from airflow.models import XCom
from airflow.decorators import task

import logging

from datetime import datetime, timezone, timedelta

with DAG('XCom_Cleanup', schedule='@once', schedule_interval=None, start_date=days_ago(2)) as dag:
    @task
    def xcom_cleanup(**kwags):
        with settings.Session() as session:
            ts_limit = datetime.now(timezone.utc) - timedelta(days=3)
            logging.info(f'{ts_limit}')
            cnt = session.query(XCom).filter(XCom.execution_date <= ts_limit).delete()
            logging.info(f'{cnt}')
    xcom_cleanup()
