"""
Task group 설정

- Task들의 모음, Task 그룹 안에 또 다른 task 그룹도 계층적으로 담을 수 있음
- 관련 task끼리 그룹핑 하여 UI적으로 직관적 표현, 관리 용이
- Task Group 간에도 Flow 정의가 가능
- Group이 다르면 task_id가 같아도 무방합니다.
- Tooltip 파라미터를 이용해 UI화면에서 Task group에 대한 설명 제공이 가능 (데커레이터 활용시 docstring)

Task Group 작성 방법
1. 데커레이터
2. 클래스
"""
from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or '' 
        print(msg)

    @task_group(group_id='first_group')
    def group_1():
        ''' task_group 데커레이터를 이용한 첫 번째 그룹입니다. '''# ui tooltip에 독스트링 표시

        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('첫 번째 TaskGroup 내 첫 번째 task입니다.')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'첫 번째 TaskGroup내 두 번쨰 task입니다.'}
        )

        inner_func1() >> inner_function2

    with TaskGroup(group_id='second_group', tooltip='두 번째 그룹입니다') as group_2:
        ''' 여기에 적은 docstring은 표시되지 않습니다''' # doc string은  표시되지 않으나 직접 tooltip지정 해놓은게 표시됨
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('두 번째 TaskGroup 내 첫 번째 task입니다.')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg': '두 번째 TaskGroup내 두 번째 task입니다.'}
        )
        inner_func1() >> inner_function2

    group_1() >> group_2