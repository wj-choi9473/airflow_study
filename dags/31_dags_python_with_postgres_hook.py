"""
Postgres 데이터 insert 예시

- postgres DB에 py_opr_drct_insrt 테이블 및 컬럼생성 필요

create table py_opr_drct_insrt (
dag_id varchar(100),
task_id varchar(100),
run_id varchar(100),
msg text
)

DAG의 문제점
- 접속정보 노출
- 접속정보 변경시 대응 어려움 
해결방안
- Variable 이용 
- Hook 이용 (variable 등록 필요 없음)

Hook 이용 해결단계
- UI서 connection 정보 입력
- PostgresHook 사용 get_conn 메서드 사용 (내부 소스코드는 psycopg2를 쓰며 동일한 로직)
"""
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
        dag_id='dags_python_with_postgres_hook',
        start_date=pendulum.datetime(2024, 5, 1, tz='Asia/Seoul'),
        schedule=None,
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing
        postgres_hook = PostgresHook(postgres_conn_id) # Hook 연결
        with closing(postgres_hook.get_conn()) as conn: 
            # hook을 사용하지 않고 직접 연걸시 psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port)) 
            # 다만 오퍼레이터에 op_args로 직접 값을 입력하여 노출
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'hook insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    insrt_postgres_with_hook = PythonOperator(
        task_id='insrt_postgres_with_hook',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom'}
    )
    insrt_postgres_with_hook