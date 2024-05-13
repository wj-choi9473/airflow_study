"""
Macro 변수 사용 예시

Macro 변수는 Jinja 템플릿 내에서 날짜 연산을 가능하게끔 해주는 기능
파이썬의 datetime이나 dateutil 같은 라이브러리를 이용해서 날짜연산을 할 수 있도록 지원

활용예시
- 매월 말일에 도는 배치 스케줄일시 전월 마지막일 ~ 어제날짜 까지 between값을 주고자 할 때
"""
from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg2",
    schedule="10 0 * * 6#2",
    start_date=pendulum.datetime(2024, 4, 13, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # START_DATE: 2주전 월요일, END_DATE: 2주전 토요일
    bash_task_2 = BashOperator(
        task_id='bash_task_2',
        env={'START_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=19)) | ds}}',
             'END_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=14)) | ds}}'
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )