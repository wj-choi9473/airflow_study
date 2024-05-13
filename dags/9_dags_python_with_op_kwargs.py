"""
op_kwargs 예시

def op_kwargs_test(name, age, *args, **kwargs):
    print(f'이름: {name}')
    print(f'나이: {age}')
    print(f'기타: {args}')
    email = kwargs['email'] or None
    phone = kwargs.get('phone') or None
    if email:
        print(f"이메일: {email}")
    if phone:
        print(f"핸드폰: {phone}")
        
#op_kwargs_test("wj",19,"a","b","c",email="a",phone="010")

"""
from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import op_kwargs_test
with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 5, 9, tz="Asia/Seoul"),
    catchup=False,
    tags = ["example","study"]
) as dag:
    
    op_kwargs_test_t1 = PythonOperator(
        task_id='op_kwargs_test_t1',
        python_callable=op_kwargs_test,
        op_args=['wj',30,'kr','seoul'],
        op_kwargs={'email':'test@mail.com','phone':'010'}
    )

    op_kwargs_test_t1