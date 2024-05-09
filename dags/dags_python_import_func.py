from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import generate_random

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 5, 8, tz="Asia/Seoul"),
    catchup=False,
    tags = ["example","study"]
) as dag:
    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=generate_random
    )
    
    py_t1
    