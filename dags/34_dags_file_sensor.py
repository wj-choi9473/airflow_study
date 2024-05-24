"""
FileSensor 예시

해당경로 파일 존재유무

먼저 connection 필요
Connection ID: fs_example
Connection Type: file (path)
Host, Schema. Login, Password, Port: empty
Path: {"path":"/tmp"} # compose에서 설정한 컨테이너 경로
"""
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
import pendulum

with DAG(
    dag_id='dags_file_sensor',
    start_date=pendulum.datetime(2024,5,22, tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False
) as dag:
    tvCorona19VaccinestatNew_sensor = FileSensor(
        task_id='tvCorona19VaccinestatNew_sensor',
        fs_conn_id='conn_file_opt_airflow_files',
        filepath='tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv',
        recursive=False,
        poke_interval=60,
        timeout=60*60*24, # 1일
        mode='reschedule'
    )
