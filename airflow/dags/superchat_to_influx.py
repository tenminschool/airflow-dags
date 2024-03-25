from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG

default_args = {
    "owner": "Papa Tiger",
    "start_date": datetime(2024,3,24),
    "retries": 1
}

@task()
def sync_super_chat_data(**kwargs):
    print("called sync_super_chat_data")

with DAG(
        dag_id="superchat_to_influx", 
        default_args=default_args, 
        schedule_interval=None) as dag:
    sync_super_chat_data()