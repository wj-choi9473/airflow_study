"""
분기처리 예시 with BranchPythonOperator

- task1 의 결과에 따라 선택지로 task 2,3,4 중 하나만 실행하도록 해야하는 경우
"""
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

with DAG(
    dag_id='dags_branch_python_operator',
    start_date=pendulum.datetime(2024,5,1, tz='Asia/Seoul'), 
    schedule='0 1 * * *',
    catchup=False
) as dag:
    def select_random():
        import random

        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a' # 후행 task_id 설정
        elif selected_item in ['B','C']:
            return ['task_b','task_c'] 
    
    python_branch_task = BranchPythonOperator(
        task_id='python_branch_task',
        python_callable=select_random
    )
    
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    python_branch_task >> [task_a, task_b, task_c]