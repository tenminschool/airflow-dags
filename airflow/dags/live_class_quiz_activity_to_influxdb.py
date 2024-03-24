import time
from datetime import datetime

import pandas as pd
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
from influxdb_client.client.write_api import SYNCHRONOUS

default_args = {
    "owner": "Md. Toufiqul Islam",
    "start_date": datetime(2024, 2, 21),
    "retries": 0
}

INFLUXDB_BUCKET_NAME = "tracker_stage_db"
INFLUX_DB_MEASUREMENT = "quiz_participants"

DELAY_SEC = 3


def getQuizzes(liveClassId, connection):
    sql_query = f"SELECT * FROM liveclass WHERE live_class_id = '{liveClassId}'"
    df = pd.read_sql(sql_query, connection)
    if len(df) == 0:
        raise ValueError("No Live Class found for id {}".format(liveClassId))

    liveClassNumericId = df.iloc[0]["id"]

    sql_query = f"SELECT * FROM quizzes WHERE live_class_id = '{liveClassNumericId}'"
    df = pd.read_sql(sql_query, connection)
    return df


def transformQuizzes(df, liveClassId, catalogProductId, catalogSkuId, programId, courseId, platform):
    influxdbPoints = []

    for index, row in df.iterrows():
        createdAt: datetime = row["createdAt"]
        point = Point.measurement(INFLUX_DB_MEASUREMENT).tag("indentification_type", "live_class").tag(
            "identification_id", liveClassId).tag(
            "catalog_product_id", catalogProductId).tag("catalog_sku_id", catalogSkuId).tag("program_id",
                                                                                            programId).tag(
            "course_id", courseId).tag("platform", platform).tag("modality", row["quiz_modality"]).tag(
            "quiz_id", row["quiz_id"]).tag("auth_user_id", row["auth_user_id"]).field("participate_at",
                                                                                      int(createdAt.timestamp() * 1000)).field(
            "is_correct", row["is_correct"]).field("quiz_option_id", row["quiz_option_id"]).field("time_taken",
                                                                                                  row[
                                                                                                      "time_taken"]).time(
            row["createdAt"])
        influxdbPoints.append(point)

    return influxdbPoints


def writeToInfluxDb(transformedData: list, influxClient: InfluxDBClient):
    writeAPI = influxClient.write_api(options=SYNCHRONOUS)
    writeAPI.write(INFLUXDB_BUCKET_NAME, org=Variable.get("INFLUX_DB_ORG"), record=transformedData)
    print("Finished writing ", len(transformedData))
    time.sleep(DELAY_SEC)


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
    connection = mysql_hook.get_conn()

    influxClient = InfluxDBClient(url=Variable.get("INFLUX_DB_URL"),
                                  token=Variable.get("INFLUX_DB_TOKEN"),
                                  org=Variable.get("INFLUX_DB_ORG"))

    pingRes = influxClient.ping()
    if not pingRes:
        raise ValueError("Cannot connect to InfluxDB")

    quizzes = getQuizzes(liveClassId, connection)
    quizIds = quizzes["id"].values

    placeholders = ','.join(['%s' for _ in quizIds])

    # Construct the SQL query with the correct number of placeholders
    sql_query = f"SELECT * FROM quiz_responses LEFT JOIN users u on u.id = quiz_responses.user_id WHERE quiz_id IN ({placeholders})"

    # Print the SQL query for debugging
    print("sql_query:", sql_query)

    df = pd.read_sql_query(sql_query, params=quizIds, con=connection)

    transformedData = transformQuizzes(df, liveClassId, catalogProductId, catalogSkuId, programId, courseId, platform)

    writeToInfluxDb(transformedData, influxClient)


with DAG(dag_id="live_class_quiz_activity_to_influx_db_etl", default_args=default_args,
         schedule_interval=None) as dag:
    syncLiveClassQuizToInfluxDB()
