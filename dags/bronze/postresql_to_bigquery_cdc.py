# Step 1: Make cursor to query data

import datetime
import json
import logging

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from google.cloud import bigquery

from bronze.database_config import POSTGRES_TO_BIGQUERY_ETL_CONFIG


# Step 1: Make cursor to query data


def prepare_parameter_func(ds, **kwargs):
    dag_run = kwargs["dag_run"]
    execution_date = dag_run.execution_date.replace(minute=0, second=0, microsecond=0)

    execution_date_str = execution_date.strftime("%Y-%m-%d/%H-%M-%S")

    task_instance = kwargs["task_instance"]
    task_instance.xcom_push(key="EXECUTION_DATE_STR", value=execution_date_str)


def prepare_cdc_sql_task_func(ds, **kwargs):
    source_table_name = kwargs["source_table_name"]
    primary_key = kwargs["primary_key"]
    audit_column = kwargs["audit_column"]

    dag_run = kwargs["dag_run"]
    execution_date = dag_run.execution_date
    start_time = execution_date.replace(minute=0, second=0, microsecond=0)
    end_time = start_time + datetime.timedelta(hours=1)

    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

    sql = f"""
    SELECT * FROM {source_table_name} WHERE {audit_column} >= '{start_time_str}' AND {audit_column} < '{end_time_str}'
    """

    task_instance = kwargs["task_instance"]
    task_instance.xcom_push("SQL", sql)


def load_extracted_file_to_bigquery_func(ds, **kwargs):
    task_instance = kwargs["task_instance"]

    source_table_name = kwargs["source_table_name"]
    bucket = "data-analytics-engineer"
    execution_date_str = task_instance.xcom_pull(key='EXECUTION_DATE_STR')
    schema_filename = "thelook_ecommerce/" + source_table_name + f"/{execution_date_str}/schema.json"

    gcs_hook = GCSHook()

    logging.info(schema_filename)

    schema_json = json.loads(
        gcs_hook.download(bucket, schema_filename).decode("utf-8")
    )

    job_config = bigquery.LoadJobConfig(
        source_format="CSV",
        write_disposition="WRITE_APPEND",
        schema=schema_json,
        skip_leading_rows=1
    )

    delta_table_id = f"class-data-analytics-engineer.bronze.delta_{source_table_name}"

    bigquery_client = bigquery.Client()
    job = bigquery_client.load_table_from_uri(
        source_uris=f"gs://{bucket}/thelook_ecommerce/" + source_table_name + f"/{execution_date_str}/data/*.csv",
        destination=delta_table_id,
        job_config=job_config
    )
    job.result()


with DAG(
    dag_id="postgresql_to_bigquery",
    schedule_interval="15 * * * *",
    start_date=pendulum.DateTime(2023,2,18),
    catchup=False
) as dag:

    prepare_parameter_task = PythonOperator(
        dag=dag,
        task_id="prepare_parameter_task",
        python_callable=prepare_parameter_func
    )

    for source_table_name, table_config in POSTGRES_TO_BIGQUERY_ETL_CONFIG.items():

        prepare_cdc_sql_task = PythonOperator(
            dag=dag,
            task_id=f"{source_table_name}_prepare_cdc_sql_task",
            python_callable=prepare_cdc_sql_task_func,
            op_kwargs={
                "source_table_name": source_table_name,
                "primary_key": table_config["primary_key"],
                "audit_column": table_config["audit_column"]
            }
        )

        prepare_parameter_task.set_downstream(prepare_cdc_sql_task)

        extract_postgresql_to_gcs = PostgresToGCSOperator(
            task_id=f"{source_table_name}_postgresql_to_gcs_task",
            bucket="data-analytics-engineer",
            filename=f"thelook_ecommerce/{source_table_name}/" + "{{ti.xcom_pull(key='EXECUTION_DATE_STR')}}/data/{}.csv",
            export_format="csv",
            schema_filename=f"thelook_ecommerce/{source_table_name}/" + "{{ti.xcom_pull(key='EXECUTION_DATE_STR')}}/schema.json",
            postgres_conn_id="thelook_postgres_connection",
            use_server_side_cursor=True,
            sql="{{ti.xcom_pull(task_ids='" + source_table_name + "_prepare_cdc_sql_task', key='SQL')}}"
        )

        prepare_cdc_sql_task.set_downstream(extract_postgresql_to_gcs)

        load_extracted_file_from_gcs_to_bigquery_task = PythonOperator(
            task_id=f"{source_table_name}_load_extracted_file_from_gcs_to_bigquery_task",
            dag=dag,
            python_callable=load_extracted_file_to_bigquery_func,
            op_kwargs={
                "source_table_name": source_table_name,
                "primary_key": table_config["primary_key"],
                "audit_column": table_config["audit_column"]
            }
        )

        extract_postgresql_to_gcs.set_downstream(load_extracted_file_from_gcs_to_bigquery_task)
