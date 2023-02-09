import datetime as dt
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
    'owner': 'emil',
    'start_date': dt.datetime(2023, 2, 8),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'on_failure_callback': send_notify_on_failure
}

with DAG(
        dag_id='fill_dds',
        schedule_interval='15 0 * * *',
        default_args=default_args,
        catchup=False,
) as dag:
    # greeting to telegram bot @an_emil_bot
    first_task = TelegramOperator(
        task_id='greeting',
        telegram_conn_id='telegram_conn',
        chat_id='4084485',
        text='''I'm starting to populate DDS. TIME: {{ execution_date }}''',
    )

    # create tables in dds layer
    create_tables = PostgresOperator(
        task_id='create_tables_in_dds',
        postgres_conn_id='postgres_default',
        sql='./ddl/dds_ddl.sql',
        on_failure_callback=send_notify_on_failure,
    )

    # populate hubs, link, satellites
    populate_dds = PostgresOperator(
        task_id='populate_dds',
        postgres_conn_id='postgres_default',
        sql='./ddl/dds_populate.sql',
        on_failure_callback=send_notify_on_failure,
    )

    # clear stg layer
    clear_stg = PostgresOperator(
        task_id='clear_stg',
        postgres_conn_id='postgres_default',
        sql='DROP TABLE IF EXISTS stg.post',
        on_failure_callback=send_notify_on_failure,
    )

    # msg about ending dag to telegram bot @an_emil_bot
    last_task = TelegramOperator(
        task_id='finishing',
        telegram_conn_id='telegram_conn',
        chat_id='4084485',
        text='''DDS layer is filled up. TIME: {{ execution_date }}''',
    )

    first_task >> create_tables >> populate_dds >> clear_stg >> last_task
