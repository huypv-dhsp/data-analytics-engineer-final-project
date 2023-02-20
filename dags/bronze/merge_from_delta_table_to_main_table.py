import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum
from google.oauth2.service_account import Credentials

from bronze.database_config import POSTGRES_TO_BIGQUERY_ETL_CONFIG


def prepare_parameter_func(ds, **kwargs):
    dag_run = kwargs["dag_run"]
    execution_date = dag_run.execution_date.replace(minute=0, second=0, microsecond=0)

    execution_date_str = execution_date.strftime("%Y-%m-%d/%H-%M-%S")

    task_instance = kwargs["task_instance"]
    task_instance.xcom_push(key="EXECUTION_DATE_STR", value=execution_date_str)


def _create_and_insert_all_data_into_main_table(source_table_name, primary_key, audit_column):
    sql = f"""
    CREATE TABLE class-data-analytics-engineer.bronze.main_{source_table_name}
    AS
    SELECT * EXCEPT(row_num)
          FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY {primary_key} ORDER BY {audit_column} DESC) AS row_num
            FROM class-data-analytics-engineer.bronze.delta_{source_table_name}
            )
          WHERE row_num = 1
    """
    bigquery_client = bigquery.Client()
    bigquery_client.query(sql).result()


def execute_merge_data_from_delta_to_main_table_func(ds, **kwargs):
    dag_run = kwargs["dag_run"]
    execution_date = dag_run.execution_date

    source_table_name = kwargs["source_table_name"]
    primary_key = kwargs["primary_key"]
    audit_column = kwargs["audit_column"]

    bigquery_client = bigquery.Client()
    main_table = None
    try:
        main_table = bigquery_client.get_table(f"class-data-analytics-engineer.bronze.main_{source_table_name}")
    except:
        pass
    if main_table is None:
        return _create_and_insert_all_data_into_main_table(source_table_name, primary_key, audit_column)

    main_table_schema = main_table.schema
    main_table_schema_field_names = []
    for field in main_table_schema:
        main_table_schema_field_names.append(field.name)

    insert_express = ",".join(main_table_schema_field_names)
    insert_value_list = []
    for field_name in main_table_schema_field_names:
        insert_value_list.append(f"d.{field_name}")
    insert_value_express = ",".join(insert_value_list)

    set_update_list = []
    for field_name in main_table_schema_field_names:
        if field_name == primary_key:
            continue
        set_update_list.append(f"{field_name} = d.{field_name}")
    set_update_express = ",".join(set_update_list)

    sql = f"""
        MERGE bigquery_change_data_capture_example.products_main m
        USING
          (
          SELECT * EXCEPT(row_num)
          FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY {primary_key} ORDER BY {audit_column} DESC) AS row_num
            FROM bronze.products_delta delta
            WHERE DATE({audit_column}) = '{execution_date.strftime("%Y-%m-%d")}'
            )
          WHERE row_num = 1
        ) d
        ON  m.id = d.id
        
        WHEN NOT MATCHED THEN
        INSERT ({insert_express})
        VALUES ({insert_value_express})
        
        WHEN MATCHED AND (m.{audit_column} <= d.{audit_column}) THEN
        UPDATE
        SET {set_update_express}
    """
    # print(sql)

    bigquery_client.query(sql).result()

with DAG(
    dag_id="merge_from_delta_table_to_main_table",
    schedule_interval="30 0 * * *",
    start_date=pendulum.DateTime(2023,2,18),
    catchup=False
) as dag:

    prepare_parameter_task = PythonOperator(
        dag=dag,
        task_id="prepare_parameter_task",
        python_callable=prepare_parameter_func
    )

    for source_table_name, table_config in POSTGRES_TO_BIGQUERY_ETL_CONFIG.items():

        execute_merge_data_from_delta_to_main_table = PythonOperator(
            task_id=f"{source_table_name}_execute_merge_data_from_delta_to_main_table",
            python_callable=execute_merge_data_from_delta_to_main_table_func,
            op_kwargs={
                "source_table_name": source_table_name,
                "primary_key": table_config["primary_key"],
                "audit_column": table_config["audit_column"]
            }
        )

        prepare_parameter_task.set_downstream(execute_merge_data_from_delta_to_main_table)
