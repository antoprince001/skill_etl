from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os
from utils import config

load_dotenv()
CONFIG_PATH = os.getenv("CONFIG_PATH", default=None)

spark = SparkSession.builder.appName('com.spark-analysis').getOrCreate()


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
    job_skills_df = job_skills_df.withColumn("id",
                                             F.monotonically_increasing_id())
    job_skills_df = job_skills_df.drop("job_id")
    job_skills_df = job_skills_df.withColumn("role",
                                             F.lit(job_role))
    job_skills_df = job_skills_df.withColumn("via",
                                             F.regexp_replace("via", "via", ""))
    job_skills_df.write.mode("overwrite").parquet(f"{output_folder_path}/{job_role}")


if __name__ == "__main__":
    config_json = config.get_config_json(CONFIG_PATH)
    job_roles = config_json.get("job_roles")

    folder_path = config_json.get("project_path")+config_json.get("raw").get("folder")
    folder_exists = os.path.exists(folder_path)

    output_folder_path = config_json.get("project_path")+config_json.get("processed").get("folder")
    output_folder_exists = os.path.exists(output_folder_path)

    if not output_folder_exists:
        os.makedirs(output_folder_path)
    if folder_exists:
        for job_role in job_roles:
            file_path = f"{folder_path}/skills-{job_role}.json"
            if os.path.isfile(file_path) is True:
                print(f"Processing {file_path}")
                skill_data_transform(file_path, job_role, output_folder_path)
