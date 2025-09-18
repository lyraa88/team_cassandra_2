# 어노테이션을 사용한 태스크 관리
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum # python의 datetime을 좀더 편하게 사용할 수 있게 돕는 모델
from datetime import datetime
from datetime import timedelta

local_tz = pendulum.timezone("Asia/Seoul")

@task
def print_hello():
    print('hello')

@task
def print_world():
    print('world')

with DAG(
    dag_id = '01_HelloWorld_v2',
    tags=["fisaai", 'my_dags'],
    start_date=datetime(2025, 3, 1, hour=12, minute=30, tzinfo=local_tz),
    schedule="10 * * * *", 
    catchup=False # 이후 동작할 시간에 대해서만 pipeline이 작동하도록 
) as dag:

    # 함수() 순서로 함수명을 태스크명으로 사용합니다.
    print_hello() >> print_world()