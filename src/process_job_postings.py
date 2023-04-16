from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('com.spark-analysis').getOrCreate()


def read_skill_json(FILE_PATH):
    return spark.read.format('json')\
            .options(header='true', inferSchema='true', multiLine='true')\
            .load(f"./raw/{FILE_PATH}")


def transform_array_to_rows(df):
    return df.select(F.explode("jobs_results").alias("skills"))


def parse_skill_columns(df):
    return df.select(
        F.col("skills.description"),
        F.col("skills.job_id"),
        F.col("skills.title"),
        F.col("skills.via")
    )


def skill_data_transform(file_path, job_role):
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
    job_results_df.write.mode("overwrite").parquet(f"./processed/{job_role}")




    




