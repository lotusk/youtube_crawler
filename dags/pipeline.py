"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

crawler_path = "/Users/lucifer/PycharmProjects/youtube_crawler/"
file_name = "ybi04.jl"

default_args = {
    'owner': 'kai',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 15),
    'email': ['lotusk@qq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('youtube_crawler', default_args=default_args, schedule_interval=timedelta(days=1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='scrapy',
    bash_command='cd {} && scrapy runspider crawler/search.py -o {}'.format(crawler_path, file_name),
    dag=dag)

t2 = BashOperator(
    task_id='index',
    bash_command='cd {} && python indexs/writer.py --file {}'.format(crawler_path, file_name),
    dag=dag)

t3 = BashOperator(
    task_id='wordcount',
    bash_command='cd {} && spark-submit analyzers/wordcount.py --file {}'.format(crawler_path, file_name),
    dag=dag)

t4 = BashOperator(
    task_id='report_status',
    bash_command='echo "I am Finished ,Please checkout ~"',
    dag=dag)
t2.set_upstream(t1)
t3.set_upstream(t1)

t4.set_upstream(t2)
t4.set_upstream(t3)
