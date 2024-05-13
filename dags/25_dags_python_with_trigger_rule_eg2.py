"""
Trigger rule 예시

task decorator로 @task(task_id = , trigger_rule="") 설정

Trigger rule은 하위 task에 거는 조건이며 여러 설정이 있음
- all_success (기본값) : 상위 Task 가 모두 성공하면 실행
- all_failed : 상위 Task 가 모두 실패하면 실행
- all_done : 상위 Task가 모두 수행되면 실행 (상위 task의 실패여부 상관 x)
- all_skipped : 상위 Task가 모두 Skipped 상태면 실행
- one_failed : 상위 Task 중 하나 이상 실패하면 실행 (모든 상위 Task 완료를 기다리지 않음)
- one_success : 상위 Task 중 하나 이상 성공하면 실행 (모든 상위 Task 완료를 기다리지 않음)
- one_done : 상위 Task 중 하나 이상 성공 또는 실패하면 실행
- none_failed : 상위 Task 중 실패가 없는 경우 실행 (성공 또는 skipped 상태)
- none_failed_min_one_success : 상위 Task 중 실패가 없고 성공한 Task가 적어도 1개 이상이면 실행
- none_skipped : Skip 된 상위 Task가 없으면 실행 (상위Task가 성공, 실패여도 무방)
- always : 언제나 실행
"""
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

import pendulum

with DAG(
    dag_id='dags_python_with_trigger_rule_eg2',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    @task.branch(task_id='branching')
    def random_branch(): # 분기처리로 인한 skip 발생
        import random
        item_lst = ['A', 'B', 'C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item == 'B':
            return 'task_b'
        elif selected_item == 'C':
            return 'task_c'

    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo upstream1'
    )

    @task(task_id='task_b')
    def task_b():
        print('정상 처리')


    @task(task_id='task_c')
    def task_c():
        print('정상 처리')

    @task(task_id='task_d', trigger_rule='none_skipped') #2,3,4중 2가지 Task는 Skipped라 5번 Task는 돌지 못함
    def task_d():
        print('정상 처리')

    random_branch() >> [task_a, task_b(), task_c()] >> task_d()