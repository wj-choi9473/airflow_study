"""
외부 쉘 스크립트 수행 예시

worker 컨테이너가 쉘 스크립트를 수행하려면?
- 문제점
    - 컨테이너는 외부의 파일은 인식할 수 없음
    - 컨테이너 안에 파일을 만들어주면 컨테이너 재시작시 파일이 사라짐
- 해결방법
    - docker-compose.yaml 파일 수정
        - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins 여기에 쉘 스크립트 저장
        - 커스텀한 py,sh를 여기에 넣어두도록 가이드함 저 위치에 인식할 수 있도록 수정 필요
        - 예시) ${AIRFLOW_PROJ_DIR:-.}/My Drive/data/GitHub/airflow_study/plugins:/opt/airflow/plugins

- shell script 실행권한을 주기 위해서는 chmod +x [scriptname.sh] 예) chmod +x test.sh 
- 실행은 ./test.sh 
"""
from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_test",
    schedule="10 0 * * 6#1",
    start_date=pendulum.datetime(2024, 5, 8, tz="Asia/Seoul"),
    catchup=False,
    tags = ["example","study"]
    
) as dag:
    
    t1 = BashOperator(
        task_id="t1",
        bash_command="/opt/airflow/plugins/shell/test.sh Hello", # airflow경로 설정
    )

    t2 = BashOperator(
        task_id="t2",
        bash_command="/opt/airflow/plugins/shell/test.sh Airflow",
    )

    t1 >> t2