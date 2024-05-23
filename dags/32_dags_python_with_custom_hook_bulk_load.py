"""
CustomHook 사용 DAG 예시

plugin/hooks/custom_postgres_hook.py 

hook의 bulk_load 메서드 문제점 개선
문제점
- Load 가능한 delimiter는 tab 고정
- Header까지 포함해서 업로드 됨
- 특수문자로 인해 파싱이 안될 경우 에러 발생
custom hook을 통한 개선방안 
- csv등 입력 받게 하기
- Header 여부 선택
- 특수문자 제거 로직
- sqlalchemy 이용하여 load
- 테이블 생성하면서 업로드
"""
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgresHook

with DAG(
        dag_id='dags_python_with_custom_hook_bulk_load',
        start_date=pendulum.datetime(2024, 5, 1, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'TbCorona19CountStatus_bulk2',
                   'file_nm':'/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbCorona19CountStatus.csv'}
    )