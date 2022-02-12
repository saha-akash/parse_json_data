import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'max_active_runs': 1
    'email': ['reviews@testsample.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}
dag = DAG(
    "LoadjsonDatatoDB",
    default_args=default_args,
    description='Extracts Data from json files and loads into Database',
    start_date=datetime(year=2021, month=06, day=06),
    schedule_	interval="* 0-23/8 * * *",
    catchup=False
)
load_json_data_to_db_unzip_file = BashOperator(
    task_id="load_json_data_to_db_unzip_file",
    bash_command="gzip -d review.json.gz",
    dag=dag,
)
load_json_data_to_db_pipeline = BashOperator(
    task_id="load_json_data_to_db_pipeline",
    bash_command="python3 json_data_load_pipeline.py review.json",
    dag=dag,
)
load_json_data_to_db_unzip_file >> load_json_data_to_db_pipeline
