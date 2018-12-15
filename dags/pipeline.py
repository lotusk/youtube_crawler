"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


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
    bash_command='cd /Users/lucifer/PycharmProjects/youtube_crawler/;scrapy runspider crawler/search.py -o /Users/lucifer/PycharmProjects/youtube_crawler/ybi03.jl',
    dag=dag)

t2 = BashOperator(
    task_id='index',
    bash_command='cd /Users/lucifer/PycharmProjects/youtube_crawler/;python indexs/writer.py --file /Users/lucifer/PycharmProjects/youtube_crawler/ybi03.jl',
    dag=dag)


t3 = BashOperator(
    task_id='wordcount',
    bash_command='cd /Users/lucifer/PycharmProjects/youtube_crawler/;spark-submit analyzers/wordcount.py --file /Users/lucifer/PycharmProjects/youtube_crawler/ybi03.jl',
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
