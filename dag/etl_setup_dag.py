from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime

with DAG(
    'skill_etl_setup',
    start_date=datetime(2023, 4, 22),
    schedule_interval='@once',
    catchup=False
) as dag:

    begin = EmptyOperator(
        task_id="begin",
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS job_posts (
                id TEXT NOT NULL primary key,
                title TEXT,
                description TEXT NOT NULL,
                via TEXT,
                role TEXT NOT NULL
            )
        '''
    )

    end = EmptyOperator(
        task_id="end",
    )

    begin >> create_table >> end
