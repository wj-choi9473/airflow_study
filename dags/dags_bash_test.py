from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_test",
    schedule="10 0 * * 6#1",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    t1 = BashOperator(
        task_id="t1",
        bash_command="/opt/airflow/plugins/shell/test.sh Hello", # airflow경로 설정
    )

    t2 = BashOperator(
        task_id="t2",
        bash_command="/opt/airflow/plugins/shell/test.sh Airflow",
    )

    t1 >> t2