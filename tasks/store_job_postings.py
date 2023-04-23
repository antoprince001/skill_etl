from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv
import os
from utils import config, folder

load_dotenv()
CONFIG_PATH = os.getenv("CONFIG_PATH", default=None)


def load_job_posting(job_file_name):
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY job_posts FROM stdin WITH DELIMITER as ',' CSV QUOTE '\"' ",
        filename=job_file_name
    )


def task_load_job_postings():
    config_json = config.get_config_json(CONFIG_PATH)
    job_roles = config_json.get("job_roles")

    folder_path = config_json.get("project_path")+config_json.get("processed").get("folder")+"/csv"
    folder_exists = os.path.exists(folder_path)
    
    if folder_exists:
        print("Copy csv to Postgres db")
        for job_role in job_roles:
            job_csv_path = f"{folder_path}/{job_role}.csv"
            load_job_posting(job_csv_path)
