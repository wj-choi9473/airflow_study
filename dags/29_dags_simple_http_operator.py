"""
SimpleHttpOperator 사용 예시

- HTTP를 이용하여 API를 처리하는 RestAPI 호출시 사용 가능
1. gui Admin에서 connections 작성 
- Conn_id: 다른 Conn이름과 중복되지 않게 작성
- Connection_type: HTTP
- Host: openapi.seoul.go.kr
- Port: 8088
- 그 외에는 작성 안해도 무방

2. API 키 변경 및 코드에 노출하기 않기 위한 설정 -> variable등록
- key: apikey_openapi_seoul_go_kr
- value: 발급 받은 api key 값
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:

    '''서울시 공공자전거 대여소 정보'''
    tb_cycle_station_info = SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10/', # api key 값 variable등록
        method='GET',
        headers={'Content-Type': 'application/json',
                        'charset': 'utf-8',
                        'Accept': '*/*'
                        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))
        
    tb_cycle_station_info >> python_2()
