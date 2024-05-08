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
        bash_command="echo whoami",
    )

    bash_task2 = BashOperator(
        task_id="bash_task2",
        bash_command="echo $HOSTNAME",
    )

    bash_task1 >> bash_task2