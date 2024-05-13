"""
Macro 변수 사용 예시

PythonOperator with task decorator 예시
1. task using macros
2. task 직접 날짜 계산
"""
from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    #start_date
    @task(task_id='task_using_macros',
      templates_dict={'start_date':'{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}',
                      'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'
     }
    )
    def get_datetime_macro(**kwargs):# templates_dict가 kwargs로 들어감
        
        templates_dict = kwargs.get('templates_dict') or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'start_date없음'
            end_date = templates_dict.get('end_date') or 'end_date없음'
            print(start_date)
            print(end_date)

    # 날짜 연산을 get_datetime_macro처럼 Python Operator에서 macro를 사용안하고 DAG안에서 직접하면 다음과 같음
    @task(task_id='task_direct_calc')
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta

        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(months=-1, day=1)
        prev_month_day_last = data_interval_end.in_timezone('Asia/Seoul').replace(day=1) +  relativedelta(days=-1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))

    get_datetime_macro() >> get_datetime_calc()