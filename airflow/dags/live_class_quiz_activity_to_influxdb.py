import time
from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook

default_args = {
    "owner": "Md. Toufiqul Islam",
    "start_date": datetime(2024, 2, 21),
    "retries": 1
}


@task()
def syncLiveClassQuizToInfluxDB(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='stage_mysql_read_connection')  # Specify the connection id
    connection = mysql_hook.get_conn()
    print("ping res ", connection.ping())
    # cursor = connection.cursor()
    # cursor.execute("SELECT * FROM my_table")
    # result = cursor.fetchall()
    # for row in result:
    #     print(row)
    # cursor.close()
    # connection.close()


with DAG(dag_id="live_class_quiz_activity_to_influx_db_etl", default_args=default_args,
         schedule_interval=None) as dag:
    syncLiveClassQuizToInfluxDB()
