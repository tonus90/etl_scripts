from datetime import datetime, timedelta
from multiprocessing import connection
from symbol import parameters
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import psycopg2
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import os
import sys
import subprocess


def to_csv():
    query = "SELECT table_name FROM information_schema.tables where table_schema like 'public';"
    pg_hook_source = PostgresHook(postgres_conn_id='postgres_source', schema='my_database')
    pg_conn_source = pg_hook_source.get_conn()
    cursor = pg_conn_source.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    for element, in result:
        q = f"COPY {element} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open(f'/opt/airflow/dags/tmp/{element}.csv', 'w') as f:
            cursor.copy_expert(q, f)

def create_tables_target():
    query = "SELECT table_name FROM information_schema.tables where table_schema like 'public';"
    pg_hook_source = PostgresHook(postgres_conn_id='postgres_source', schema='my_database')
    conn_source = pg_hook_source.get_conn()
    cursor_source = conn_source.cursor()
    cursor_source.execute(query)
    result = cursor_source.fetchall()
    pg_hook_target = PostgresHook(postgres_conn_id='postgres_target', schema='my_database')
    conn_target = pg_hook_target.get_conn()
    cursor_target = conn_target.cursor()
    for element, in result:
        cursor_target.execute(f"drop table if exists stage.{element} cascade")

    with open('/opt/airflow/dags/ddl.sql') as f:
        lines = f.read().replace('\n', '')
        lines = lines.replace('\t', '')
    query_ddl = lines
    cursor_target.execute(query_ddl)
    conn_target.commit() # <--- makes sure the change is shown in the database
    conn_target.close()
    cursor_target.close()


def to_db():
    query = "SELECT table_name FROM information_schema.tables where table_schema like 'stage';"
    pg_hook_target = PostgresHook(postgres_conn_id='postgres_target', schema='my_database')
    conn_target = pg_hook_target.get_conn()
    cursor_target = conn_target.cursor()
    cursor_target.execute(query)
    result = cursor_target.fetchall()
    result = [res for res, in result]
    result.sort(key=len)
    for table_name in result:
        q = f"COPY stage.{table_name} from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open(f'/opt/airflow/dags/tmp/{table_name}.csv', 'r') as f:
            cursor_target.copy_expert(q, f)
            conn_target.commit()
            # pg_conn.close()
            # cursor.close() # <--- makes sure the change is shown in the database

            
def create_tables_core():
    query = "SELECT table_name FROM information_schema.tables where table_schema like 'core';"
    pg_hook_target = PostgresHook(postgres_conn_id='postgres_target', schema='my_database')
    conn_target = pg_hook_target.get_conn()
    cursor_target = conn_target.cursor()
    cursor_target.execute(query)
    result = cursor_target.fetchall()
    for element, in result:
        cursor_target.execute(f"drop table if exists core.{element} cascade")

    with open('/opt/airflow/dags/data_vault_ddl.sql') as f:
        lines = f.read().replace('\n', '')
        lines = lines.replace('\t', '')
    query_ddl = lines
    cursor_target.execute(query_ddl)
    conn_target.commit() # <--- makes sure the change is shown in the database



DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2022, 11, 9),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

with DAG(
    dag_id="to_core_data_vault",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    tags=['data-flow'],
) as dag:

    save_from_dataset_to_csv = PythonOperator(
    task_id='from_source_to_csv',
    python_callable=to_csv
    )

    create_tables_tgt = PythonOperator(
    task_id='create_tables_in_target',
    python_callable=create_tables_target,
    )

    from_cdv_to_target_db = PythonOperator(
    task_id='from_csv_to_target',
    python_callable=to_db
    )

    fill_hubs = PostgresOperator(
        task_id="fill_hubs_core",
        postgres_conn_id = 'postgres_target',
        sql= 'call core.insert_data_hubs ()'
    )

    create_tables_in_core = PythonOperator(
    task_id='create_tables_in_core',
    python_callable=create_tables_core,
    )

    fill_links = PostgresOperator(
        task_id="fill_links_core",
        postgres_conn_id = 'postgres_target',
        sql= 'call core.insert_data_links ()' 
    )

    fill_sattelities = PostgresOperator(
        task_id="fill_sattelities_core",
        postgres_conn_id = 'postgres_target',
        sql= 'call core.insert_data_sattelites ()'
    )



save_from_dataset_to_csv >> create_tables_tgt >> from_cdv_to_target_db >> create_tables_in_core >> fill_hubs >> fill_links >> fill_sattelities