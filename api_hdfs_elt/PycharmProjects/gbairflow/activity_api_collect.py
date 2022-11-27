import datetime as dt
from airflow.models import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'bored',
    'start_date': dt.datetime(2022, 11, 27),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

with DAG(
        dag_id='collect_activities',
        schedule_interval='*/2 * * * *',
        default_args=default_args
) as dag:
    first_task = BashOperator(
        task_id='greeting',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run{{ dag_run }}"'
    )

    get_json = BashOperator(
        task_id='get_json',
        bash_command='wget -qO - http://www.boredapi.com/api/activity | hdfs dfs -put - \
        /user/hduser/daily_activity_`date +%Y-%m-%d`/activity_`date +%s`.json'
    )

    last_task = BashOperator(
        task_id='finish',
        bash_command='echo Pipeline finished! Execution date is {{ ds }}'
    )

    first_task >> get_json >> last_task
