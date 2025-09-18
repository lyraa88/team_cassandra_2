from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'log_data_processing_pipeline',
    default_args=default_args,
    description='A data pipeline to process and store log data from Kaggle',
    schedule_interval='@daily',
    tags=['log-processing'],
) as dag:
    # 프로듀서 스크립트 실행 태스크
    run_producer = BashOperator(
        task_id='run_producer',
        bash_command='python /opt/airflow/dags/producer.py',
    )

    # 컨슈머 스크립트 실행 태스크
    run_consumer = BashOperator(
        task_id='run_consumer',
        bash_command='python /opt/airflow/dags/consumer.py',
    )

    # 파이프라인 의존성 설정: 프로듀서 실행 후 컨슈머 실행
    run_producer >> run_consumer