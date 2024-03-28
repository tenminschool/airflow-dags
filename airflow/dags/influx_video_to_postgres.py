from datetime import datetime

import pandas as pd
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from influxdb_client_3 import InfluxDBClient3
from pandas import DataFrame
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "Md. Toufiqul Islam",
    "start_date": datetime(2024, 2, 21),
}


def getVideoWatchedDuration(client: InfluxDBClient3):
    query = """SELECT auth_user_id,SUM(duration) as total_video_watched_duration FROM user_study_duration_logs 
WHERE media_type='video' 
AND auth_user_id!='' AND time >= now() - interval '365 day' GROUP BY auth_user_id"""
    reader = client.query(query=query, language="sql")
    return pd.DataFrame(reader.to_pandas())


def getDay():
    current_time = datetime.now()

    # Get start of the day (midnight)
    start_of_day = current_time.replace(hour=0, minute=0, second=0, microsecond=0)

    print("Start of the day:", start_of_day)
    return start_of_day


def getTransformedQuizData(videoWatchedDf: DataFrame):
    data = {'auth_user_id': [],
            'day': [],
            'total_video_watched_duration': []}

    for index, row in videoWatchedDf.iterrows():
        data['auth_user_id'].append(row['auth_user_id'])
        data['day'].append(getDay())
        data['total_video_watched_duration'].append(row["total_video_watched_duration"])

    return pd.DataFrame(data)


def writeQuizData(transformedDf: DataFrame, postgresConnection):
    cursor = postgresConnection.cursor()

    for index, row in transformedDf.iterrows():
        query = """INSERT INTO user_learning_reports (day, auth_user_id, total_quiz_submitted, total_quiz_corrected)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (day, auth_user_id) DO UPDATE SET total_quiz_submitted =user_learning_reports.total_quiz_submitted +
                                                                            EXCLUDED.total_quiz_submitted,
                                                      total_quiz_corrected = user_learning_reports.total_quiz_corrected +
                                                                             EXCLUDED.total_quiz_corrected"""
        cursor.execute(query,
                       [row["day"], row["auth_user_id"], row["total_quiz_submitted"], row["total_quiz_corrected"]])

    postgresConnection.commit()
    cursor.close()
    postgresConnection.close()


def writePollData(transformedDf: DataFrame, postgresConnection):
    cursor = postgresConnection.cursor()

    for index, row in transformedDf.iterrows():
        query = """INSERT INTO user_learning_reports (day, auth_user_id, total_poll_submitted)
        VALUES (%s, %s, %s)
        ON CONFLICT (day, auth_user_id) DO UPDATE SET total_poll_submitted =user_learning_reports.total_poll_submitted +
                                                                            EXCLUDED.total_poll_submitted"""
        cursor.execute(query,
                       [row["day"], row["auth_user_id"], row["total_poll_submitted"]])

    postgresConnection.commit()
    cursor.close()
    postgresConnection.close()


@task()
def syncInfluxVideoToPostgres(**kwargs):
    print("called syncInfluxVideoToPostgres")
    influxClient = InfluxDBClient3(host=Variable.get("INFLUX_DB_URL"), token=Variable.get("INFLUX_DB_TOKEN"),
                                   org=Variable.get("INFLUX_DB_ORG"), database="tracker_stage_db")

    postgresHook = PostgresHook().get_hook(conn_id="postgres_tenlytics_write_connection_stage")
    postgresConnection = postgresHook.get_conn()

    result = postgresHook.get_first("SELECT 1")

    if result:
        print("PostgreSQL database is reachable.")

        videoDf = getVideoWatchedDuration(influxClient)
        transformedData = getTransformedQuizData(videoDf)
        writeQuizData(transformedData, postgresConnection)

    else:
        raise ValueError("PostgreSQL database did not respond.")


with DAG(dag_id="influx_video_to_postgres_etl", default_args=default_args, schedule_interval='@hourly') as dag:
    syncInfluxVideoToPostgres()
