"""
Pool 예시
- 9개의 task가 한번에 도는게 아니라 3개씩 도는 예시
- priority_weight 큰 순서대로 3개씩 

개념
- slot이용하여 airflow내 동시 수행되는 task 개수 제어
- 활용예시
    - 소스 디비 부하를 주지 않으려고 동시에 수행하는 task의 개수를 제어
    - 무거운 태스크가 돌때 함깨 수행될 수 있는 태스크의 갯수를 줄여야함 -> pool_slots=3으로 할당

pool 만드는법
- airflow ui 에 admin > Pools
- pool이름, slots 지정

"""
from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_pool",
    schedule="10 0 * * 6",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args={
        'pool':'pool_small'
    }
) as dag:
    bash_task_1 = BashOperator(
        task_id='bash_task_1',
        bash_command='sleep 30',
        priority_weight=6
    )

    bash_task_2 = BashOperator(
        task_id='bash_task_2',
        bash_command='sleep 30',
        priority_weight=5
    )

    bash_task_3 = BashOperator(
        task_id='bash_task_3',
        bash_command='sleep 30',
        priority_weight=4
    )

    bash_task_4 = BashOperator(
        task_id='bash_task_4',
        bash_command='sleep 30'
    )

    bash_task_5 = BashOperator(
        task_id='bash_task_5',
        bash_command='sleep 30'
    )

    bash_task_6 = BashOperator(
        task_id='bash_task_6',
        bash_command='sleep 30'
    )

    bash_task_7 = BashOperator(
        task_id='bash_task_7',
        bash_command='sleep 30',
        priority_weight=7
    ) 

    bash_task_8 = BashOperator(
        task_id='bash_task_8',
        bash_command='sleep 30',
        priority_weight=8
    ) 

    bash_task_9 = BashOperator(
        task_id='bash_task_9',
        bash_command='sleep 30',
        priority_weight=9
    ) 