"""
op_args 사용 예시

def op_args_test(name,age,*args):
    print(f"이름: {name}")
    print(f"나이: {age}")
    print(f"기타: {args}")

op_args=['wj',25,'kr','seoul'] 이와 같이 리스트로 입력
"""
from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import op_args_test
with DAG(
    dag_id="dags_python_with_op_args",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 5, 9, tz="Asia/Seoul"),
    catchup=False,
    tags = ["example","study"]
) as dag:
    
    op_args_test_t1 = PythonOperator(
        task_id='op_args_test_t1',
        python_callable=op_args_test,
        op_args=['wj',25,'kr','seoul']
    )

    op_args_test_t1