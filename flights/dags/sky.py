import datetime as dt
import pandas as pd
import os
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.telegram.operators.telegram import TelegramOperator
from io import StringIO
from airflow.macros import ds_add


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


# get home path
def _get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)


# populate postgres database with chunks of data
def populate_postgres(**context):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    month_begin = context['templates_dict']['month_begin']
    month_end = ds_add(context['templates_dict']['next_month'], -1).replace('-', '')
    for chunk in pd.read_csv(_get_path(f'flights/flightlist_{month_begin}_{month_end}.csv'), chunksize=100000):
        df = chunk[
            ((chunk['origin'].notna()) | (chunk['destination'].notna())) & (chunk['origin'] != chunk['destination'])]
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t')
        buffer.seek(0)
        with conn.cursor() as cursor:
            cursor.copy_from(buffer, 'opensky', null='', sep='\t')
            conn.commit()
    conn.close()


default_args = {
    'owner': 'emil`',
    'start_date': dt.datetime(2019, 1, 1),
    'end_date': dt.datetime(2020, 10, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),

}

with DAG(
        dag_id='sky',
        schedule_interval='@monthly',
        default_args=default_args,
        catchup=True,
        template_searchpath="/home/user1/sql"
) as dag:

    # greeting to telegram bot @an_emil_bot
    first_task = TelegramOperator(
        task_id='greeting',
        telegram_conn_id='telegram_conn',
        chat_id='4084485',
        text='''I'm starting to populate opensky table. TIME: {{ execution_date }}''',
    )

    # create target table in postgres from ddl file
    create_table = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres_default',
        sql='ddl.sql',
        on_failure_callback=send_notify_on_failure,
    )

    # populate postgres table clear data by chunks of data
    populate_postgres = PythonOperator(
        task_id='populate_postgres',
        python_callable=populate_postgres,
        templates_dict={'month_begin': '{{ ds_nodash }}',
                        'next_month': '{{ next_ds }}',
                        },
        on_failure_callback=send_notify_on_failure,
    )

    # bye bye to telegram bot @an_emil_bot
    last_task = TelegramOperator(
        task_id='bye_bye',
        telegram_conn_id='telegram_conn',
        chat_id='4084485',
        text='''Postgres table if filled up. SEE MONTH OF LOADED DATA: {{ ds }}''',
    )

    first_task >> create_table >> populate_postgres >> last_task
