"""
PythonOperator 기본 예시
"""
from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
import random

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 5, 8, tz="Asia/Seoul"),
    catchup=False,
    tags = ["example","study"]
) as dag:
    def random_number():
        print(random.randint(0,3))

    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=random_number
    )
    
    py_t1
    