import datetime as dt
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'bored',
    'start_date': dt.datetime(2022, 11, 27),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

with DAG(
        dag_id='daily_activity_mart',
        schedule_interval='* 3 * * *',
        default_args=default_args,
        catchup=False
) as dag:
    first_task = BashOperator(
        task_id='greeting',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run{{ dag_run }}"'
    )

    put_file_to_hdfs = BashOperator(
        task_id='move_file_to_hdfs',
        bash_command='''ACTIVITIES_FILE="activities_{{ ds_nodash }}.csv"
                        hdfs dfs -put -f /home/hduser/$ACTIVITIES_FILE /user/hduser/datasets/$ACTIVITIES
                        rm /home/hduser/$ACTIVITIES_FILE
                        echo "/user/hduser/datasets/$ACTIVITIES_FILE"''',
    )

    with TaskGroup("prepare_table") as prepare_table:
        drop_table_activity = HiveOperator(
            task_id='drop_table_in_hive',
            hql='''DROP TABLE IF EXISTS daily_activity_{{ ds_nodash }};'''
        )

        create_activity_table = HiveOperator(
            task_id='create_table_in_hive',
            hql='''CREATE TABLE IF NOT EXISTS daily_activity_{{ ds_nodash }} 
            (activiti_id int, Activity string, Type string, Participants int, Price float, Link string, Key string, 
            Accessibility float)
            row format delimited fields terminated by \',\' 
            null defined as \' \'
            tblproperties (\'skip.header.line.count\'=\'1\');'''
        )

        drop_table_activity >> create_activity_table

    load_data = HiveOperator(
        task_id='load_data_to_activity_table',
        hql='''LOAD DATA INPATH 
        \'{{ ti.xcom_pull(task_ids='move_file_to_hdfs', key='return_value') }}\' 
        OVERWRITE INTO TABLE daily_activity_{{ ds_nodash }};'''
    )

    drop_agg_table = HiveOperator(
        task_id='drop_agg_table',
        hql='''drop table if exists types_sum_price_{{ ds_nodash }};'''
    )

    make_agg_table = HiveOperator(
        task_id='table_oldest_passengers',
        hql='''CREATE TABLE  if not exists types_sum_price_{{ ds_nodash }}
        row format delimited
        fields terminated by '|'
        STORED AS Parquet
        AS SELECT type, round(sum(price), 2)*100 as sum_type
        FROM daily_activity_{{ ds_nodash }}
        GROUP BY type;'''
    )

    show_agg_info = BashOperator(
        task_id='show_agg_info',
        bash_command='''beeline -u jdbc:hive2://localhost:10000 \
        -e "SELECT * FROM types_sum_price_{{ ds_nodash }};" \
        | tr "\n" ";" ''',
    )

    def format_message(**kwargs):
        flat_message = kwargs['ti'].xcom_pull(task_ids='show_agg_info', key='return_value')
        flat_message = flat_message.replace(';', '\n')
        print(flat_message)
        kwargs['ti'].xcom_push(key='telegram_message', value=flat_message)


    prepare_message = PythonOperator(
        task_id='prepare_message',
        python_callable=format_message,
    )

    send_result_telegram = TelegramOperator(
        task_id='send_success_message_telegram',
        telegram_conn_id='telegram_conn_id',
        chat_id='4084485',
        text='''Pipeline {{ execution_date.int_timestamp }} is done. Result:
    {{ ti.xcom_pull(task_ids='prepare_message', key='telegram_message') }}''',
    )

    first_task >> put_file_to_hdfs >> prepare_table >> load_data >> drop_agg_table >> make_agg_table
    make_agg_table >> show_agg_info >> prepare_message >> send_result_telegram
