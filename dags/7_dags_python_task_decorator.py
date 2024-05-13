"""
task decorator 사용 예시

PythonOperator 대신 데코레이터 사용
def python func1():
    ...

py_task_1 = PythonOperator(
    Task_id="py_task1",
    python_callable=python_func1
)

py_task_1
```

"""
from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2024, 5, 9, tz="Asia/Seoul"),
    catchup=False,
    tags = ["example","study"]
) as dag:
    
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context('task_decorator 실행')

