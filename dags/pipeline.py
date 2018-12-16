"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/tutorial.py
"""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

crawler_path = "/Users/lucifer/PycharmProjects/youtube_crawler/"
file_name = "ybi05.jl"

default_args = {
    'owner': 'kai',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
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

dag = DAG('youtube_crawler', default_args=default_args)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='scrapy',
    bash_command='cd {} && scrapy runspider crawler/spiders/search.py -o {}'.format(crawler_path, file_name),
    dag=dag)

t2 = BashOperator(
    task_id='index',
    bash_command='cd {} && python indexs/writer.py --file {}'.format(crawler_path, file_name),
    dag=dag)

t3 = BashOperator(
    task_id='word_count',
    bash_command='cd {} && spark-submit analyzers/count.py --file {} --option word'.format(crawler_path, file_name),
    dag=dag)

t4 = BashOperator(
    task_id='channel_title_count',
    bash_command='cd {} && spark-submit analyzers/count.py --file {} --option channeltitle'.format(crawler_path,
                                                                                                   file_name),
    dag=dag)

t5 = BashOperator(
    task_id='report_status',
    bash_command='echo "I am Finished ,Please checkout ~"',
    dag=dag)
t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream(t1)
t5.set_upstream(t2)
t5.set_upstream(t3)
t5.set_upstream(t4)
