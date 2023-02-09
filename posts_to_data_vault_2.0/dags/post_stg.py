import os
import datetime as dt
import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.telegram.operators.telegram import TelegramOperator


# send notify to telegram bot @an_emil_bot on tasks failure
def send_notify_on_failure(context):
    task_instance = context['task_instance']
    message = f"Task failed: {task_instance.task_id}"
    notify_fail = TelegramOperator(
        task_id=f'telegram_alert_task',
        telegram_conn_id='telegram_conn',
        chat_id='4084485',
        text=message,
        dag=dag,
    )
    return notify_fail.execute(context)


default_args = {
    'owner': 'emil`',
    'start_date': dt.datetime(2023, 2, 8),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'on_failure_callback': send_notify_on_failure,
}


# get home path with file name
def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)


# save csv to datalake
def create_csv(**kwargs):
    response = requests.get(kwargs['params']['url'])
    execution_date = kwargs['ti'].execution_date
    execution_date_str = execution_date.strftime("%d_%m_%Y")
    if response.status_code == 200:
        data_list = response.json()
        df = pd.DataFrame(data_list)
        df.to_csv((get_path(f'post_{execution_date_str}.csv')), encoding='utf-8', index=False, sep='|')
        return execution_date_str
    else:
        response.raise_for_status()


# populate stg layer in data warehouse
def fill_db(**kwargs):
    execution_date_str = kwargs['ti'].xcom_pull(task_ids='save_json_csv')
    pg_hook = (PostgresHook(postgres_conn_id='postgres_default', schema='dwh'))
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = """COPY stg.post(userid, id, title, body) FROM STDIN 
            WITH (FORMAT CSV, DELIMITER '|', NULL 'NULL', HEADER true);"""
    file_path = get_path(f"post_{execution_date_str}.csv")
    with open(file_path, 'r') as file:
        cursor.copy_expert(query, file)
        conn.commit()


with DAG(
        dag_id='post_stg',
        schedule_interval='10 0 * * *',
        default_args=default_args,
        catchup=False
) as dag:

    # greeting to telegram bot @an_emil_bot
    first_task = TelegramOperator(
        task_id='greeting',
        telegram_conn_id='telegram_conn',
        chat_id='4084485',
        text='''I'm starting to collect posts. TIME: {{ execution_date }}''',
    )

    # save data from API to csv
    save_posts = PythonOperator(
        task_id='save_json_csv',
        python_callable=create_csv,
        provide_context=True,
        on_failure_callback=send_notify_on_failure,
        params={'url': 'https://jsonplaceholder.typicode.com/posts'},
    )

    # create table in stg layer
    with TaskGroup("prepare_stg_tables") as prepare_table:
        create_schemas = PostgresOperator(
            task_id='create_schemas',
            postgres_conn_id='postgres_default',
            sql='''CREATE SCHEMA IF NOT EXISTS stg;
                    CREATE SCHEMA IF NOT EXISTS dds;
                    DROP SCHEMA IF EXISTS public CASCADE;''',
            on_failure_callback=send_notify_on_failure,
        )

        drop_tables = PostgresOperator(
            task_id='drop_tables_in_stg',
            postgres_conn_id='postgres_default',
            sql='''DROP TABLE IF EXISTS stg.post;''',
            on_failure_callback=send_notify_on_failure,
        )

        create_tables = PostgresOperator(
            task_id='create_tables_in_stg',
            postgres_conn_id='postgres_default',
            sql=f'''./ddl/stg_ddl.sql''',
            on_failure_callback=send_notify_on_failure,
        )
    create_schemas >> drop_tables >> create_tables

    # save data to stg
    fill_post_table = PythonOperator(
        task_id='filling_post_table',
        python_callable=fill_db,
        provide_context=True,
        on_failure_callback=send_notify_on_failure,
    )

    # msg about ending dag to telegram bot @an_emil_bot
    last_task = TelegramOperator(
        task_id='finishing',
        telegram_conn_id='telegram_conn',
        chat_id='4084485',
        text='''STG layer is filled up. TIME: {{ execution_date }}''',
    )

    first_task >> save_posts >> prepare_table >> fill_post_table >> last_task
