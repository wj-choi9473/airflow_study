from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False,
    tags = ["example","study"]
) as dag:
    bash_task1 = BashOperator(
        task_id="bash_task1",
        bash_command="echo hello world", #Logs를 보면 hello world가 output으로 나옴
    )

    bash_task2 = BashOperator(
        task_id="bash_task2",
        bash_command="echo $HOSTNAME", # airflow worker의 컨테이너 아이디가 나올것 -> 워커 컨테이너에서 실행이 된다는 뜻 
    )

    bash_task1 >> bash_task2