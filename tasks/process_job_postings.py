from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from dotenv import load_dotenv
import os
from utils import config, folder
import uuid

load_dotenv()
CONFIG_PATH = os.getenv("CONFIG_PATH", default=None)

spark = SparkSession.builder.appName('com.spark-analysis').getOrCreate()

# def generate_uuid():
#     return f"{str(uuid.uuid4())}"

def read_skill_json(FILE_PATH):
    return spark.read.format('json')\
            .options(header='true', inferSchema='true', multiLine='true')\
            .load(FILE_PATH)


def transform_array_to_rows(df):
    return df.select(F.explode("jobs_results").alias("skills"))


def parse_skill_columns(df):
    return df.select(
        F.col("skills.description"),
        F.col("skills.job_id"),
        F.col("skills.title"),
        F.col("skills.via")
    )


def skill_data_transform(file_path, job_role, output_folder_path):
    skills_df = read_skill_json(file_path)
    job_results_df = transform_array_to_rows(skills_df)
    job_skills_df = parse_skill_columns(job_results_df)
    # job_skills_df = job_skills_df.withColumn("id",
    #                                          F.monotonically_increasing_id())
    # uuid_udf = udf(generate_uuid, StringType())
    job_skills_df = job_skills_df.withColumn("id", F.expr("uuid()"))
    job_skills_df = job_skills_df.drop("job_id")
    job_skills_df = job_skills_df.withColumn("role",
                                             F.lit(job_role))
    job_skills_df = job_skills_df.withColumn("via",
                                             F.regexp_replace("via", "via", ""))
    job_skills_df = job_skills_df.na.drop(subset=["description"])
    job_skills_df.write.mode("overwrite").parquet(f"{output_folder_path}/parquet/{job_role}")
    pandas_df = job_skills_df.toPandas()
    pandas_data = pandas_df[['id', 'title', 'description',  'via', 'role']]
    pandas_data.to_csv(f"{output_folder_path}/csv/{job_role}.csv", index=None, header=False)


def task_transform_job_postings():
    config_json = config.get_config_json(CONFIG_PATH)
    job_roles = config_json.get("job_roles")

    folder_path = config_json.get("project_path")+config_json.get("raw").get("folder")
    folder_exists = os.path.exists(folder_path)

    output_folder_path = config_json.get("project_path")+config_json.get("processed").get("folder")

    folder.configure_folder_path(output_folder_path)
    os.makedirs(f"{output_folder_path}/csv")
    if folder_exists:
        for job_role in job_roles:
            file_path = f"{folder_path}/skills-{job_role}.json"
            if os.path.isfile(file_path) is True:
                print(f"Processing {file_path}")
                skill_data_transform(file_path, job_role, output_folder_path)
        

# task_transform_job_postings()