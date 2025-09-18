# OFFICLAL DOCUMENTATION: https://airflow.apache.org/docs/apache-airflow/stable/operators-and-hooks-ref.html
# import 구문
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum # python의 datetime을 좀더 편하게 사용할 수 있게 돕는 모델
from datetime import datetime
from datetime import datetime, timedelta


local_tz = pendulum.timezone("Asia/Seoul")

def print_world() -> None:
    print('world')

# DAG의 전체 특성을 작성해줌
with DAG(
    dag_id="00_hello_world", # DAG의 식별자용 아이디입니다.
    description="My First DAG 입니다.", # DAG에 대해 설명합니다.
    start_date=datetime(2025, 9, 1, hour=12, minute=30, tzinfo=local_tz),  # DAG 정의 기준 시간부터 시작합니다
    # 스케쥴의 간격과 함께 DAG 첫 실행 시작 날짜를 지정해줍니다.
    # 주의: 9월 1일에 DAG를 작성하고 자정마다 실행하도록 schedule을 지정한다면 태스크는91월 2일 자정부터 수행됩니다  
    catchup=False, # 시작날짜부터 지금까지 채우지 못했던 부분을 backfill 할지 결정합니다.      
    end_date=datetime(year=2025, month=9, day=22), # backfill은 end_date 전날까지의 횟수만 시도되며, end_date 이후에는 이 dag를 실행할 수 없습니다.
    schedule="@daily", # DAG 실행 간격 - 매일 자정에 실행
    tags=["fisaai", "my_dags"],
    ) as dag:

    # DAG에서 실행할 각각의 task를 정의
    t1 = BashOperator(
        task_id="print_hello",
        bash_command="echo Hello",
        owner="fisa", # 이 작업의 오너입니다. 보통 작업을 담당하는 사람 이름을 넣습니다.
        retries=3, # 이 태스크가 실패한 경우, 3번 재시도 합니다.
        retry_delay=timedelta(minutes=1), # 재시도하는 시간 간격은 1분입니다.
        depends_on_past=False,
    )

    # -2. python 함수인 print_world를 실행합니다.
    t2= PythonOperator(
        task_id="print_world",
        python_callable=print_world,
        # depends_on_past=True,
        owner="fisa",
        retries=3,
        retry_delay=timedelta(minutes=3),
    )

# task의 흐름을 >> 마지막에 작성
t1 >> t2