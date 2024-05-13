"""
Edge Label 예시

ui에서 순서 그래프 보여줄때 엣지(Task 간 연결)에 설명 추가해주는것
"""
from airflow import DAG
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label


with DAG(
    dag_id="dags_empty_with_edge_label",
    schedule=None,
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    empty_1 = EmptyOperator(
        task_id='empty_1'
    )

    empty_2 = EmptyOperator(
        task_id='empty_2'
    )

    empty_1 >> Label('1과 2사이') >> empty_2

    empty_3 = EmptyOperator(
        task_id='empty_3'
    )

    empty_4 = EmptyOperator(
        task_id='empty_4'
    )

    empty_5 = EmptyOperator(
        task_id='empty_5'
    )

    empty_6 = EmptyOperator(
        task_id='empty_6'
    )

    empty_2 >> Label('Start Branch') >> [empty_3,empty_4,empty_5] >> Label('End Branch') >> empty_6