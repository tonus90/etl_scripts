import datetime as dt
import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import glob
import json

default_args = {
    'owner': 'bored',
    'start_date': dt.datetime(2022, 11, 27),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}


def create_df():
    files = glob.glob('/home/hduser/daily_activity/*.json')
    df = pd.DataFrame(columns=['Activity', 'Type', 'Participants', 'Price', 'Link', 'Key', 'Accessibility'])
    i = 0
    for file in files:
        with open(file, 'r') as f:
            data = json.load(f)
        df.loc[i] = [data['activity'], data['type'], data['participants'], data['price'], data['link'], data['key'],
                     data['accessibility']]
        i += 1
    df.to_csv('/home/hduser/daily_activity.csv', encoding='utf-8')


with DAG(
        dag_id='daily_activity_csv',
        schedule_interval='* 0 * * *',
        default_args=default_args
) as dag:
    first_task = BashOperator(
        task_id='greeting',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run{{ dag_run }}"'
    )

    mk_dir_daily_json = BashOperator(
        task_id='buffer_dir',
        bash_command='mkdir /home/hduser/daily_activity'
    )

    get_json = BashOperator(
        task_id='get_json',
        bash_command='hdfs dfs -get /user/hduser/daily_activity_`date +%Y-%m-%d`/*.json /home/hduser/daily_activity'
    )

    create_data_frame = PythonOperator(
        task_id='data_frame',
        python_callable=create_df
    )

    df_to_hdfs = BashOperator(
        task_id='df_to_hdfs',
        bash_command='hdfs dfs -put -f /home/hduser/daily_activity.csv /user/hduser/daily_activity_`date +%Y-%m-%d`.csv'
    )

    rm_temp_files = BashOperator(
        task_id='rm_temp_files',
        bash_command='rm -r /home/hduser/daily_activity ; rm /home/hduser/daily_activity.csv'
    )

    rm_hdfs_temp_files = BashOperator(
        task_id='rm_hdfs_temp_files',
        bash_command='hdfs dfs -rm -r /user/hduser/daily_activity_`date +%Y-%m-%d`' #`date -d "yesterday 13:00" \'+%Y-%m-%d\'`  `date +%Y-%m-%d`
    )

    last_task = BashOperator(
        task_id='finish',
        bash_command='echo Pipeline finished! Execution date is {{ ds }}'
    )

    first_task >> mk_dir_daily_json >> get_json >> create_data_frame >> df_to_hdfs >> rm_temp_files >> rm_hdfs_temp_files >> last_task
