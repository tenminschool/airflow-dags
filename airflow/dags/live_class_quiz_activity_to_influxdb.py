import time
from datetime import datetime

from MySQLdb import Connection
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

default_args = {
    "owner": "Md. Toufiqul Islam",
    "start_date": datetime(2024, 2, 21),
    "retries": 0
}


def getQuizzes(liveClassId, cursor):
    sql = "SELECT * FROM liveclass WHERE live_class_id = " + liveClassId
    cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
        print(row)


@task()
def syncLiveClassQuizToInfluxDB(**kwargs):
    print("called")
    conf = kwargs['dag_run'].conf

    liveClassId = conf.get('live_class_id', None)
    catalogProductId = conf.get("catalog_product_id", None)
    catalogSkuId = conf.get("catalog_sku_id", None)
    programId = conf.get("program_id", None)
    courseId = conf.get("course_id", None)
    platform = conf.get("platform", None)

    mysql_hook = MySqlHook(mysql_conn_id='stage_mysql_read_connection',
                           schema=Variable.get("LIVE_CLASS_SERVICE_DB_NAME"))  # Specify the connection id
    print("ping res ", mysql_hook.test_connection())
    connection = mysql_hook.get_connection(conn_id="stage_mysql_read_connection")
    cursor = mysql_hook.get_cursor()
    getQuizzes(liveClassId, cursor)
    cursor.close()
    connection.close()


with DAG(dag_id="live_class_quiz_activity_to_influx_db_etl", default_args=default_args,
         schedule_interval=None) as dag:
    syncLiveClassQuizToInfluxDB()
