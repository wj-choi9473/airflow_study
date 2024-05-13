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
    dag_id="dags_python_with_xcom_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # 방법2 예시
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'


    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids='python_xcom_push_by_return')
        print('xcom_pull 메서드로 직접 찾은 리턴 값:' + value1)

    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print('함수 입력값으로 받은 값:' + status)


    python_xcom_push_by_return = xcom_push_result()
    xcom_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_1()