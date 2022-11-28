import datetime as dt
import pandas as pd
import os
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
import json



def add_row(**kwargs):
    index = int(dt.datetime.now().timestamp()*1000)
    data = json.loads((kwargs['ti'].xcom_pull(task_ids='get_json')))
    df = pd.DataFrame(data, index=[index])
    df.to_csv(kwargs['ti'].xcom_pull(task_ids='get_path'), encoding='utf-8', mode='a', header=False)
    return data['type']


def create_csv(**kwargs):
    index = int(dt.datetime.now().timestamp() * 1000)
    data = json.loads(kwargs['ti'].xcom_pull(task_ids='get_json'))
    df = pd.DataFrame(data, index=[index])
    df.to_csv(kwargs['ti'].xcom_pull(task_ids='get_path'), encoding='utf-8')


def branching(**kwargs):
    if os.path.isfile(kwargs['ti'].xcom_pull(task_ids='get_path')):
        return ['add_activities', 'send_success_message_telegram']
    else:
        return ['create_csv', 'send_success_message_telegram']

default_args = {
    'owner': 'bored',
    'start_date': dt.datetime(2022, 11, 28),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG(
        dag_id='collect_activities',
        schedule_interval='*/2 * * * *',
        default_args=default_args,
        catchup=False
) as dag:
    first_task = TelegramOperator(
        task_id='greeting',
        telegram_conn_id='telegram_conn_id',
        chat_id='4084485',
        text='''I'm starting to collect activities. TIME: {{ execution_date }}''',
    )

    get_json = BashOperator(
        task_id='get_json',
        bash_command='''wget -qO - http://www.boredapi.com/api/activity'''
    )

    get_path_csv = BashOperator(
        task_id='get_path',
        bash_command='''echo /home/hduser/activities_{{ ds_nodash }}.csv'''
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=branching,
    )

    create_csv_once = PythonOperator(
        task_id='create_csv',
        python_callable=create_csv
    )

    add_row = PythonOperator(
        task_id='add_activities',
        python_callable=add_row
    )

    # get_ex_date = BashOperator(
    #     task_id='get_ex_date',
    #     bash_command='''echo {{ ds }}'''
    # )

    send_result_telegram = TelegramOperator(
        task_id='send_success_message_telegram',
        telegram_conn_id='telegram_conn_id',
        chat_id='4084485',
        text='''The new activity added with type: {{ ti.xcom_pull(task_ids="add_activities") }}''',
        trigger_rule='one_success',
    )

    first_task >> get_json >> get_path_csv >> branching >> [create_csv_once, add_row] >> send_result_telegram
