from datetime import datetime, timedelta

import json
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROFILES_DIR = DBT_PROJECT_DIR = "/opt/airflow/dags/dbt_modules/data_analytic_engineer_dwh_final_project"
DBT_TARGET = "dev"

with DAG(
    "dbt_dwh_bigquery",
    start_date=datetime(2021, 1, 1),
    description="A dbt wrapper for Airflow.",
    schedule_interval="0 2 * * *",
    catchup=False,
    doc_md=__doc__,
    # max_active_runs=3,
    max_active_tasks=3,
) as dag:
    def load_manifest():
        local_filepath = f"{DBT_PROJECT_DIR}/target/manifest.json"
        with open(local_filepath) as f:
            data = json.load(f)
        return data

    def make_dbt_task(node, dbt_verb):
        """Returns an Airflow operator either run and test an individual model"""
        GLOBAL_CLI_FLAGS = "--no-write-json"
        model = node.split(".")[-1]
        if dbt_verb == "run":
            dbt_task = BashOperator(
                task_id=node,
                bash_command=f"python /opt/airflow/dags/dbt_modules/dbt_runner --no-write-json run --target {DBT_TARGET} --models {model} --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}",
                retries=1,
                retry_delay=timedelta(seconds=10),
            )
        elif dbt_verb == "test":
            node_test = node.replace("model", "test")
            dbt_task = BashOperator(
                task_id=node,
                bash_command=f"python /opt/airflow/dags/dbt_modules/dbt_runner --no-write-json run --target {DBT_TARGET} --models {node_test} --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}",
                retries=1,
                retry_delay=timedelta(seconds=10),
            )
        elif dbt_verb == "source":
            model = node.replace("model", "source")
            dbt_task = BashOperator(
                task_id=node,
                bash_command=f"python /home/airflow/gcs/dags/dbt_modules/dbt_runner --no-write-json run --target {DBT_TARGET} --models {model} --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}",
                retries=1,
                retry_delay=timedelta(seconds=10),
            )
        return dbt_task

    dbt_seed = BashOperator(
        task_id="dbt_start",
        bash_command=f"echo 1",
    )

    data = load_manifest()
    dbt_tasks = {}

    for node in data["nodes"].keys():
        if node.split(".")[0] == "model":
            # node_test = node.replace("model", "test")
            dbt_tasks[node] = make_dbt_task(node, "run")
            # dbt_tasks[node_test] = make_dbt_task(node, "test")

    for node in data["nodes"].keys():
        if node.split(".")[0] == "model":
            # Set dependency to run tests on a model after model runs finishes
            # node_test = node.replace("model", "test")
            # dbt_tasks[node] >> dbt_tasks[node_test]
            # Set all model -> model dependencies
            for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
                upstream_node_type = upstream_node.split(".")[0]
                if upstream_node_type == "model":
                    dbt_seed >> dbt_tasks[upstream_node] >> dbt_tasks[node]
