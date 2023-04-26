from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime

from tasks import process_job_postings 
from tasks import google_scraper
from tasks import store_job_postings

with DAG(
    'skill_etl',
    start_date=datetime(2023, 4, 22),
    schedule_interval='@once',
    catchup=False
) as dag:
    start = EmptyOperator(
        task_id="start_step",
    )

    extract_jobs = PythonOperator(
        task_id='extract_job_posts',
        python_callable=google_scraper.task_extract_job_postings
    )

    transform_jobs = PythonOperator(
        task_id='transform_job_posts',
        python_callable=process_job_postings.task_transform_job_postings
    )

    load_jobs = PythonOperator(
        task_id='load_job_posts',
        python_callable=store_job_postings.task_load_job_postings
    )

    end = EmptyOperator(
        task_id="end_step",
    )

    start >> extract_jobs >> transform_jobs >> load_jobs >> end
    # start >> load_jobs >> end
