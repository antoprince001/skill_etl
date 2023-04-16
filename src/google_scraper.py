from serpapi import GoogleSearch
import json
from dotenv import load_dotenv
import os

load_dotenv()
CONFIG_PATH = os.getenv("CONFIG_PATH", default=None)
API_KEY = os.getenv("SERP_API_KEY", default=None)


def get_config_json(CONFIG_PATH):
    with open(CONFIG_PATH, "r") as config:
        return json.load(config)


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


if __name__ == "__main__":
    config = get_config_json(CONFIG_PATH)
    job_roles = config.get("job_roles")
    folder_path = config.get("project_path")+config.get("raw").get("folder")
    folder_exists = os.path.exists(folder_path)
    if not folder_exists:
        os.makedirs(folder_path)
    for job_role in job_roles:
        scrape_job_role_postings(job_role, folder_path)