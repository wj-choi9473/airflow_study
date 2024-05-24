"""
ExternalTaskSensor 예시

모니터링 하고자 하는 DAG을 설정
- 그 안에서도 어떤 task를 모니터링 할지 설정 가능 
- 협업환경에서 모니터링 당하는 DAG 수정시 주의 필요
"""
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
from datetime import timedelta
from airflow.utils.state import State 

with DAG(
    dag_id='dags_external_task_sensor',
    start_date=pendulum.datetime(2024,5,22, tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False
) as dag:
    external_task_sensor_a = ExternalTaskSensor(
        task_id='external_task_sensor_a',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_a',
        allowed_states=[State.SKIPPED],
        execution_delta=timedelta(hours=6),
        poke_interval=10        #10초
    )

    external_task_sensor_b = ExternalTaskSensor(
        task_id='external_task_sensor_b',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_b',
        failed_states=[State.SKIPPED],
        execution_delta=timedelta(hours=6),
        poke_interval=10        #10초
    )

    external_task_sensor_c = ExternalTaskSensor(
        task_id='external_task_sensor_c',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_c',
        allowed_states=[State.SUCCESS],
        execution_delta=timedelta(hours=6),
        poke_interval=10        #10초
    )