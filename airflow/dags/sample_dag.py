import logging
import random
from datetime import datetime, timedelta

from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator

def generate_random_number(ti):
    number = random.randint(1, 10)
    ti.xcom_push(key="random_number", value=number)
def guess_number(ti):
    guess = random.randint(1, 10)
    number = ti.xcom_pull(key="random_number", task_ids="generate_random_number")
    if guess == number:
        logging.info(
            f"Congratulations, your guess was right! Number: {number}, guess: {guess}"
        )
    else:
        raise AirflowException(f"Wrong guess! Number: {number}, guess: {guess}")
with DAG(
    "xcom_guess",
    start_date=datetime(2023, 2, 27),
    schedule=timedelta(minutes=5),
) as dag:
    generate_random_number = PythonOperator(
        task_id="generate_random_number", python_callable=generate_random_number
    )
    guess_number = PythonOperator(task_id="guess_number", python_callable=guess_number)
    generate_random_number >> guess_number