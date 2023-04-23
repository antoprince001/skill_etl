from serpapi import GoogleSearch
import json
from dotenv import load_dotenv
import os
from utils import config, folder

load_dotenv()
CONFIG_PATH = os.getenv("CONFIG_PATH", default=None)
API_KEY = os.getenv("SERP_API_KEY", default=None)


def scrape_job_role_postings(job_role, folder_path):
    params = {
      "engine": "google_jobs",
      "q": job_role,
      "hl": "en",
      "api_key": API_KEY
    }

    search = GoogleSearch(params)
    results = search.get_dict()
    # jobs_results = results["jobs_results"]

    JOB = params["q"]
    with open(f"{folder_path}/skills-{JOB}.json", "w") as outfile:
        json.dump(results, outfile)


def task_extract_job_postings():
    config_json = config.get_config_json(CONFIG_PATH)
    job_roles = config_json.get("job_roles")
    folder_path = config_json.get("project_path")+config_json.get("raw").get("folder")
    print(folder_path)
    folder.configure_folder_path(folder_path)
    for job_role in job_roles:
        print(f"Extract for {job_role}")
        scrape_job_role_postings(job_role, folder_path)
