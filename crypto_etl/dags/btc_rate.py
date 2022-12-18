import datetime as dt
from airflow.models import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
import requests
import pendulum
from airflow.utils.task_group import TaskGroup



def get_response(**kwargs):
    response = requests.get(kwargs['url'], params=kwargs['params'])
    data = response.json()
    print(f'MY RESPONSE {data}')
    return response.json()


def get_parse(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='get_response')
    print(f'HERE IS DATA {data}')
    code = list(data['rates'])[0]
    data['code'] = code
    data['rate'] = data['rates'].get(code)
    data = {key: value for (key, value) in data.items() if key not in ('motd', 'rates', 'success')}
    return data


def fill_table(**kwargs):
    connect = ClickHouseHook(clickhouse_conn_id='clickhouse', database='rate')
    row = kwargs['ti'].xcom_pull(task_ids='parse')
    print(f'MY ROW IS {row}')
    print(connect.run('SHOW TABLES'))
    row['date'] = dt.datetime.strptime(row['date'], '%Y-%m-%d').date()
    connect.run('INSERT INTO btc_rate (base, date, code, rate) VALUES', [row])


tz = pendulum.timezone("Europe/Moscow")

default_args = {
    'owner': 'exchange_team',
    'start_date': tz.convert(pendulum.datetime(2022, 12, 15)),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG(
        dag_id='collect_rates',
        schedule_interval='0 */3 * * *',
        default_args=default_args,
        catchup=True
) as dag:
    first_task = TelegramOperator(
        task_id='greeting',
        telegram_conn_id='telegram_conn_id',
        chat_id='4084485',
        text='''I'm starting to collect activities. TIME: {{ execution_date }}''',
    )

    get_response = PythonOperator(
        task_id='get_response',
        python_callable=get_response,
        op_kwargs={'url': 'https://api.exchangerate.host/{{ds}}',
                   'params': {'base': 'BTC',
                              'symbols': 'USD',
                              }
                   }
    )

    parse = PythonOperator(
        task_id='parse',
        python_callable=get_parse
    )

    with TaskGroup(group_id='prepare_table') as prepare_table:
        create_database = ClickHouseOperator(
            task_id='create_database',
            clickhouse_conn_id='clickhouse',
            sql="""create database if not exists rate"""
        )

        # drop = ClickHouseOperator(
        #     task_id='drop_table',
        #     clickhouse_conn_id='clickhouse',
        #     sql="""
        #             DROP TABLE IF EXISTS btc_rate
        #             """,
        #     database='rate'
        # )

        create_table = ClickHouseOperator(
            task_id='create_table',
            clickhouse_conn_id='clickhouse',
            sql="""
                CREATE TABLE IF NOT EXISTS btc_rate (code String,
                rate Float32,
                base String,
                date Date) 
                ENGINE = MergeTree 
                ORDER BY date
                PARTITION BY date;
                """,
            database='rate'
        )

        create_database >> create_table

    fill_table = PythonOperator(
        task_id='fill_table',
        python_callable=fill_table
    )

    send_result_telegram = TelegramOperator(
        task_id='send_success_message_telegram',
        telegram_conn_id='telegram_conn_id',
        chat_id='4084485',
        text='''The new rate added''',
        trigger_rule='one_success',
    )

    first_task >> get_response >> parse >> prepare_table >> fill_table >> send_result_telegram
