from datetime import datetime

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup

from airflow.providers.mongo.hooks.mongo import MongoHook

default_args = {
    "owner": "Arnob",
    "start_date": datetime(2024, 2, 21),
    "retries": 1
}


@task()
def getSessionDataTesting(**kwargs):
    print("called")
    print("Remotely received value of {} for key=message".
          format(kwargs['dag_run'].conf['session_id']))
    hook = MongoHook(mongo_conn_id="stage_mongo_db_connection")
    client = hook.get_conn()
    print(client.list_database_names())


with DAG(dag_id="exam_session_dag", default_args=default_args, schedule_interval=None) as dag:
    getSessionData()
   





