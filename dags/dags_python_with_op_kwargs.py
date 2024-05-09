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