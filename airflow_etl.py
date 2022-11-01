from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import psycopg2
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import os


def to_csv():
    query = "SELECT table_name FROM information_schema.tables where table_schema like 'public';"
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_source',
        schema='my_database'
        )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    for element, in result:
        q = f"COPY {element} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open(f'/opt/airflow/dags/tmp/{element}.csv', 'w') as f:
            cursor.copy_expert(q, f)

def create_tables():
    query = "SELECT table_name FROM information_schema.tables where table_schema like 'public';"
    pg_hook1 = PostgresHook(
        postgres_conn_id='postgres_source',
        schema='my_database'
        )
    pg_conn = pg_hook1.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    pg_hook2 = PostgresHook(
        postgres_conn_id='postgres_target',
        schema='my_database'
        )
    pg_conn = pg_hook2.get_conn()
    cursor = pg_conn.cursor()
    for element, in result:
        cursor.execute(f"drop table if exists {element}")

    with open('/opt/airflow/dags/dss.ddl') as f:
        lines = f.readlines()
        lines.pop(0)
    query_ddl = ''.join(lines)
    cursor.execute(query_ddl)
    pg_conn.commit() # <--- makes sure the change is shown in the database
    pg_conn.close()
    cursor.close()


def to_db():
    query = "SELECT table_name FROM information_schema.tables where table_schema like 'public';"
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_target',
        schema='my_database'
        )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    for element, in result:
        q = f"COPY {element} from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open(f'/opt/airflow/dags/tmp/{element}.csv', 'r') as f:
            cursor.copy_expert(q, f)
            pg_conn.commit() # <--- makes sure the change is shown in the database


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2022, 10, 29),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

with DAG(
    dag_id="save_tables_to_csv",
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
    python_callable=create_tables
    )

    from_cdv_to_target_db = PythonOperator(
    task_id='from_csv_to_target',
    python_callable=to_db
    )

save_from_dataset_to_csv >> create_tables_tgt >> from_cdv_to_target_db