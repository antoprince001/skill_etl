# skill_etl
ETL Data pipeline project developed to process online job posts using Airflow, Spark, Postgres and Tableau. 

## Project Architecture

<img alt='elt' src='https://raw.githubusercontent.com/antoprince001/skill_etl/main/assets/skill_etl_architecture.png' />

## Components of the ETL Pipeline

### Extract

Job Posts is fetched from Google Jobs API based on job role and the data for each job role is stored as individual JSON file in raw folder.

### Transform

JSON files for each job role is read using PySpark and the data is processed to fetch required data columns in required format. The cleaned data is then 
stored in parquet and CSV format in the processed folder.

### Load

The CSV files are loaded into PostgreSQL database using Airflow copy_expert operation using Postgres Hook. 

### Pipelines

The project uses the following two DAGs (Directed Acyclic Graph)

- ### Skill ETL setup pipeline

Setup pipelines runs the SQL script to create destination table in postgres db if it does not exist.

<img alt='elt' src='https://raw.githubusercontent.com/antoprince001/skill_etl/main/assets/skill_etl_setup_dag.png' />

- ### Skill ETL pipeline

This pipeline extract the Extract, Transform and Load tasks to process the data from API ingestion layer to data storage layer.

<img alt='elt' src='https://raw.githubusercontent.com/antoprince001/skill_etl/main/assets/skill_etl_dag.png' />


### Reporting

A tableau dashboard is created in order to visualise the insights from the data stored in PostgreSQL. A PL/SQL function is created to convert the postgres table 
to csv which is then converted to excel file. This excel file is used as the source for Tableau visualisation. It is stored in reporting folder.

Link to Dashboard - https://public.tableau.com/app/profile/antony.prince.j/viz/skill_etl/Dashboard1?publish=yes

<div class='tableauPlaceholder' id='viz1682542050984' style='position: relative'><noscript><a href='#'><img alt='Dashboard 1 ' src='https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;sk&#47;skill_etl&#47;Dashboard1&#47;1_rss.png' style='border: none' /></a></noscript><object class='tableauViz'  style='display:none;'><param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' /> <param name='embed_code_version' value='3' /> <param name='site_root' value='' /><param name='name' value='skill_etl&#47;Dashboard1' /><param name='tabs' value='no' /><param name='toolbar' value='yes' /><param name='static_image' value='https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;sk&#47;skill_etl&#47;Dashboard1&#47;1.png' /> <param name='animate_transition' value='yes' /><param name='display_static_image' value='yes' /><param name='display_spinner' value='yes' /><param name='display_overlay' value='yes' /><param name='display_count' value='yes' /><param name='language' value='en-GB' /></object></div>                


### Configuration

The Config file in the project under config/ is used to configure the job roles to be queried and the path in which each stage of the pipeline should store the data during the Airflow DAG execution.


```json
{
    "job_roles" : 
        [ 
            "data%20engineer%20india",
            "backend%developer%20india",
            "blockchain%developer%20india",
            "data%scientist%20india",
            "fullstack%developer%20india"
        ],
    "project_path": "/Users/antonyprincej/airflow/dags/skill_graph/",
    "raw" : {
        "folder" : "raw",
        "type" : "json"
    },
    "processed" : {
        "folder" : "processed",
        "type" : ["parquet","csv"]
    },
    "destination" : {
        "type" : "sql"
    }

}
```

