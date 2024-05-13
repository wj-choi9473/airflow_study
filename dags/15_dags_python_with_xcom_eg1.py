"""
Xcom 예시

- Airflow DAG 안 Task 간 데이터 공유
- 주로 작은 규모의 데이터 공유 (1GB 이상의 데이터 공유는 외부 솔루션 사용 필요 AWS S3, HDFS 등)

방법1. **kwargs에 존재하는 ti(task_instance) 객체 활용
방법2. 파이썬 함수의 return 값 활용

- return을 하게되면 xcom에 저장함
    - task 데코레이터 사용시 함수 입/출력 관계만으로 task flow 정의가 됨
"""
from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_xcom_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # 방법1 예시
    @task(task_id='python_xcom_push_task1')
    def xcom_push1(**kwargs):
        ti = kwargs['ti'] 
        ti.xcom_push(key="result1", value="value_1")
        ti.xcom_push(key="result2", value=[1,2,3])

    @task(task_id='python_xcom_push_task2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="result1", value="value_2")
        ti.xcom_push(key="result2", value=[1,2,3,4])

    @task(task_id='python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(key="result1")
        value2 = ti.xcom_pull(key="result2", task_ids='python_xcom_push_task1')
        print(value1) #value_2
        print(value2) #[1,2,3]


    xcom_push1() >> xcom_push2() >> xcom_pull()