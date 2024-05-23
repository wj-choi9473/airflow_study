"""
DAG 간 의존관계 설정을 위한 TriggerDagRun 오퍼레이터 예시

- 방식: 실행할 다른 DAG의 ID를 지정하여 수행 
- Trigger 되는 DAG의 선행 DAG이 하나만 있는 경우 사용 권고 (선행 DAG가 2개 이상인 경우 ExternalTask Sensor사용)

필수값
- task_id 
- trigger_dag_id (어떤 DAG를 트리거 할건지)
"""
# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

with DAG(
    dag_id='dags_trigger_dag_run_operator',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule='30 9 * * *',
    catchup=False
) as dag:

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start!"',
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='dags_python_operator',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None
        )

    start_task >> trigger_dag_task