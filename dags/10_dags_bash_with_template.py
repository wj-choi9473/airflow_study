"""
Jinja 템플릿 사용 예시 

{{}} 중괄호 두개 사용하여 Airflow에서 기본적으로 제공하는 변수들을 치환된 값으로 입력할 수 있음
https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html 

- 모든 오퍼레이터, 파라미터에 Template 변수 적용이 불가능 하므로 확인 필요

"""
from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_template",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024, 5, 9, tz="Asia/Seoul"),
    catchup=False,
    tags = ["example","study"]
) as dag:
    bash_t1 = BashOperator(
        task_id='bash_t1',
        bash_command='echo "data_interval_end: {{ data_interval_end }}"' #배치 돌리는 날짜
    )

    bash_t2 = BashOperator(
        task_id='bash_t2',
        env={
            'START_DATE':'{{data_interval_start | ds }}', #배치 이전에 돌았던 날짜 (이전 배치일,논리적 기준일)
            'END_DATE':'{{data_interval_end | ds }}'
        },
        bash_command='echo $START_DATE && echo $END_DATE'
    )

    bash_t1 >> bash_t2