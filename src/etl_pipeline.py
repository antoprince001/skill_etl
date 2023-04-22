from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

from google_scraper import task_extract_job_postings
from process_job_postings import task_transform_job_postings

with DAG(
    'skill_etl',
    start_date=datetime(2023, 4, 22),
    schedule_interval='@daily',
    catchup=False
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    extract_jobs = PythonOperator(
        task_id='extract_jobs',
        python_callable=task_extract_job_postings
    )

    transform_jobs = PythonOperator(
        task_id='transform_jobs',
        python_callable=task_transform_job_postings
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> extract_jobs >> transform_jobs >> end
