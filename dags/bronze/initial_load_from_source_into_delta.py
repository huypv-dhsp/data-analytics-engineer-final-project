import json
import json
import logging

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from google.cloud import bigquery

from bronze.database_config import POSTGRES_TO_BIGQUERY_ETL_CONFIG


def prepare_parameter_func(ds, **kwargs):
    dag_run = kwargs["dag_run"]
    task_instance = kwargs["task_instance"]

    execution_date = dag_run.execution_date.replace(minute=0, second=0, microsecond=0)

    execution_date_str = execution_date.strftime("%Y-%m-%d/%H-%M-%S")

    task_instance.xcom_push(key="EXECUTION_DATE_STR", value=execution_date_str)

    source_table_name = dag_run.conf["source_table_name"]
    task_instance.xcom_push(key="SOURCE_TABLE_NAME", value=source_table_name)
    task_instance.xcom_push(key="AUDIT_COLUMN", value=POSTGRES_TO_BIGQUERY_ETL_CONFIG[source_table_name]["audit_column"])

    delta_table_id = f"class-data-analytics-engineer.bronze.delta_{source_table_name}"
    task_instance.xcom_push(key="DELTA_TABLE_ID", value=delta_table_id)


def load_extracted_file_to_bigquery_func(ds, **kwargs):
    task_instance = kwargs["task_instance"]
    dag_run = kwargs["dag_run"]

    source_table_name = dag_run.conf["source_table_name"]
    bucket = "data-analytics-engineer"
    execution_date_str = task_instance.xcom_pull(key='EXECUTION_DATE_STR')
    schema_filename = "thelook_ecommerce/" + source_table_name + f"/init_load/{execution_date_str}/schema.json"

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

    table_id = task_instance.xcom_pull(key='DELTA_TABLE_ID')

    bigquery_client = bigquery.Client()
    job = bigquery_client.load_table_from_uri(
        source_uris=f"gs://{bucket}/thelook_ecommerce/" + source_table_name + f"/init_load/{execution_date_str}/data/*.csv",
        destination=table_id,
        job_config=job_config
    )
    job.result()


with DAG(
    dag_id="init_load_postgresql_to_bigquery",
    schedule_interval=None,
    start_date=pendulum.DateTime(2023,2,18),
    catchup=False,
) as dag:

    prepare_parameter_task = PythonOperator(
        dag=dag,
        task_id="prepare_parameter_task",
        python_callable=prepare_parameter_func
    )

    extract_postgresql_to_gcs = PostgresToGCSOperator(
        task_id="postgresql_to_gcs_task",
        bucket="data-analytics-engineer",
        filename="thelook_ecommerce/" + "{{ dag_run.conf['source_table_name'] }}" + "/init_load/{{ti.xcom_pull(key='EXECUTION_DATE_STR')}}/data/{}.csv",
        export_format="csv",
        schema_filename="thelook_ecommerce/" + "{{ dag_run.conf['source_table_name']}}" + "/init_load/{{ti.xcom_pull(key='EXECUTION_DATE_STR')}}/schema.json",
        postgres_conn_id="thelook_postgres_connection",
        use_server_side_cursor=True,
        sql="SELECT * FROM {{dag_run.conf['source_table_name']}}",
        approx_max_file_size_bytes=19000000
    )

    prepare_parameter_task.set_downstream(extract_postgresql_to_gcs)

    load_extracted_file_from_gcs_to_bigquery = PythonOperator(
        task_id="load_extracted_file_from_gcs_to_bigquery",
        python_callable=load_extracted_file_to_bigquery_func
    )

    extract_postgresql_to_gcs.set_downstream(load_extracted_file_from_gcs_to_bigquery)
