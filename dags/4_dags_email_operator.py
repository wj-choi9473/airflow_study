"""
EamilOperator with Gmail 예시

- 지메일 -> 설정 -> 모든 설정보기 -> 전달및pop/imap -> imap 사용
- 구글계정관리 -> 보안 -> 2단계 인증 -> 앱비밀번호 셋팅

- docker-compose.yaml environment 부분 편집
    AIRFLOW__SMTP__SMTP_HOST : 'smtp.gmail.com'
    AIRFLOW__SMTP__SMTP_USER : 'xxx@gmail.com'
    AIRFLOW__SMTP__SMTP_PASSWORD : '구글에서 발급받은 16자리 앱 비밀번호(공백없이)'
    AIRFLOW__SMTP__SMTP_PORT : '587'
    AIRFLOW__SMTP__SMTP_MAIL_FROM : 'xxx@gmail.com' #이메일 계정
"""
from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='xxx@naver.com',
        subject='Airflow 성공메일',
        html_content='Airflow 작업이 완료되었습니다'
    )