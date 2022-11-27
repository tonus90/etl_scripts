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
        dag_id='daily_activity_mart',
        schedule_interval='* 1 * * *',
        default_args=default_args
) as dag:
    first_task = BashOperator(
        task_id='greeting',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run{{ dag_run }}"'
    )

    drop_table_activity = BashOperator(
        task_id='drop_table_in_hive',
        bash_command='''curr_date=`date +%Y_%m_%d` ; 
        beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS daily_activity_$curr_date;"'''
    )

    create_activity_table = BashOperator(
        task_id='create_table_in_hive',
        bash_command='''curr_date=`date +%Y_%m_%d` ; 
        beeline -u jdbc:hive2://localhost:10000 -e "CREATE TABLE IF NOT EXISTS daily_activity_$curr_date 
        (activiti_id int, Activity string, Type string, Participants int, Price float, Link string, Key string, 
        Accessibility float) 
        row format delimited fields terminated by \',\' 
        null defined as \' \' 
        tblproperties (\'skip.header.line.count\'=\'1\');"'''
    )

    load_data = BashOperator(
        task_id='load_data_to_activity_table',
        bash_command='''curr_date=`date +%Y_%m_%d` ; 
        beeline -u jdbc:hive2://localhost:10000 -e "LOAD DATA INPATH 
        \'/user/hduser/daily_activity_`date +%Y-%m-%d`.csv\' 
        OVERWRITE INTO TABLE daily_activity_$curr_date;"'''
    )

    drop_agg_table = BashOperator(
        task_id='drop_agg_table',
        bash_command='''curr_date=`date +%Y_%m_%d` ; 
        beeline -u jdbc:hive2://localhost:10000 -e "drop table if exists types_sum_price_$curr_date;"'''
    )

    make_agg_table = BashOperator(
        task_id='table_oldest_passengers',
        bash_command='''curr_date=`date +%Y_%m_%d` ;
        beeline -u jdbc:hive2://localhost:10000 -e "CREATE TABLE  if not exists types_sum_price_$curr_date
        row format delimited
        fields terminated by '|'
        STORED AS Parquet
        AS SELECT type, round(sum(price), 2) as sum_type
        FROM daily_activity_$curr_date
        GROUP BY type;"'''
    )

    last_task = BashOperator(
        task_id='finish',
        bash_command='echo Pipeline finished! Execution date is {{ ds }}'
    )

    first_task >> drop_table_activity >> create_activity_table >> load_data >> drop_agg_table >> make_agg_table >> last_task
