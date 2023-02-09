import datetime as dt
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'emil',
    'start_date': dt.datetime(2023, 2, 8),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG(
        dag_id='fill_dds',
        schedule_interval='15 0 * * *',
        default_args=default_args,
        catchup=False,
) as dag:
    # create tables in dds layer
    create_tables = PostgresOperator(
        task_id='create_tables_in_dds',
        postgres_conn_id='postgres_default',
        sql='./ddl/dds_ddl.sql',
    )

    # populate hubs, link, satellites
    populate_dds = PostgresOperator(
        task_id='populate_dds',
        postgres_conn_id='postgres_default',
        sql='./ddl/dds_populate.sql',
    )

    # clear stg layer
    clear_stg = PostgresOperator(
        task_id='clear_stg',
        postgres_conn_id='postgres_default',
        sql='DROP TABLE IF EXISTS stg.post',
    )

    create_tables >> populate_dds >> clear_stg
