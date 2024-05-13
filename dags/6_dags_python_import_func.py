"""
외부 파이썬 함수 수행

- Airflow 자동적으로 dags폴더와 plugins폴더를 sys.path에 추가함

common 또는 원하는 디렉토리 이름으로 생성, 그 안에 python파일 작성
- from common.common_func import func_name 하면 되지만 현재 여기서 작성한 경로에선 로컬에선 오류가남
- 로컬 개발 환경에선 from plugins.common.common_func 
-> 이렇게 안하려면
    - airflow_study 디렉토리에 .env 에 설정추가 
    예시)
    ```
    WORKSPACE_FOLDER=/Users/xxx/My Drive/data/GitHub/airflow_study
    PYTHONPATH=${WORKSPACE_FOLDER}/plugins
    ```
"""
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
    