"""
BaseHook을 이용한 CustomHook 예시

Postgres연결
1. get_conn 메서드 구현
    - DB 와의 연결 세션 객체인 conn을 리턴하도록 구현 (에어플로우에 등록한 connection정보를 담은 conndl dksla)
    - BaseHook의 추상 메서드, 자식 클래스에서 구현 필요
2. bulk_load 메서드 구현

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
from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd

class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id) #class method
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)
        return self.postgres_conn

    def bulk_load(self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool):
        from sqlalchemy import create_engine

        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블 :' + table_name)
        self.get_conn()
        header = 0 if is_header else None                       # is_header = True면 0, False면 None
        if_exists = 'replace' if is_replace else 'append'       # is_replace = True면 table이 있어도 replace, False면 append
        file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)

        for col in file_df.columns:                             
            try:
                # string 문자열이 아닐 경우 continue
                file_df[col] = file_df[col].str.replace('\r\n','')      # 줄넘김 및 ^M 제거
                self.log.info(f'{table_name}.{col}: 개행문자 제거')
            except:
                continue 
                
        self.log.info('적재 건수:' + str(len(file_df)))
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        engine = create_engine(uri)
        file_df.to_sql(name=table_name,
                            con=engine,
                            schema='public',
                            if_exists=if_exists,
                            index=False
                        )