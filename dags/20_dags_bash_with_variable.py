"""
전역변수 Variable 이용 예시

xcom -> DAG에 있는 task들 끼리만 데이터 공유가 가능한 방법
Variable을 사용하면 모든 DAG에서 데이터 공유 가능
- airflow 페이지에서 값 등록 가능. ADmin -> variables
- meta DB에 variable 테이블에 저장됨
- jinja 템플릿 {{}} 이용, 오퍼레이터 내부에서 가져오는걸 권고
- 언제 쓰면 좋을지?
    - 플바플, 보통 협업 환경에서 표준화된 dag를 만들기 위해 주로 사용. 주로 상수(CONST)로 지정해서 사용할 변수 세팅
    - e.g. email,Alert 메시지 담당자 email정보
    - e.g. base_file_dir = /opt/airflow/plugins/files
"""
from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    var_value = Variable.get("sample_key")

    bash_var_1 = BashOperator(
    task_id="bash_var_1",
    bash_command=f"echo variable:{var_value}"
    )

    bash_var_2 = BashOperator(
    task_id="bash_var_2",
    bash_command="echo variable:{{var.value.sample_key}}"
    )
