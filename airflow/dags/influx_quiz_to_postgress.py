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
    'schedule_interval': '0 * * * *'  # every hour
}


def getQuizData(client: InfluxDBClient3):
    query = """SELECT auth_user_id, COUNT(quiz_id) as total_quiz_submitted, SUM(is_correct) as total_quiz_corrected
       FROM quiz_participants
       WHERE time >= now() - interval '365 day'
         AND (modality='m1' OR modality='m5')
         AND auth_user_id IS NOT NULL
        AND quiz_option_id!=0
       GROUP BY auth_user_id"""
    reader = client.query(query=query, language="sql")
    return pd.DataFrame(reader.to_pandas())


def getPollData(client: InfluxDBClient3):
    query = """SELECT auth_user_id, COUNT(quiz_id) as quiz_submitted
    FROM quiz_participants
    WHERE time >= now() - interval '365 day'
      AND (modality='m2' OR modality='m3' OR modality='m13')
      AND auth_user_id IS NOT NULL
      AND quiz_option_id!=0
    GROUP BY auth_user_id"""

    reader = client.query(query=query, language="sql")
    return pd.DataFrame(reader.to_pandas())


def getDay():
    current_time = datetime.now()

    # Get start of the day (midnight)
    start_of_day = current_time.replace(hour=0, minute=0, second=0, microsecond=0)

    print("Start of the day:", start_of_day)
    return start_of_day


def getTransformedData(quizDf: DataFrame):
    data = {'auth_user_id': [],
            'day': [],
            'total_quiz_submitted': [],
            'total_quiz_corrected': []}

    for index, row in quizDf.iterrows():
        data['auth_user_id'].append(row['auth_user_id'])
        data['day'].append(getDay())
        data['total_quiz_submitted'].append(row["total_quiz_submitted"])
        data['total_quiz_corrected'].append(row["total_quiz_corrected"])

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


@task()
def syncInfluxQuizDataToPostgres(**kwargs):
    print("called syncInfluxQuizDataToPostgres")
    influxClient = InfluxDBClient3(host=Variable.get("INFLUX_DB_URL"), token=Variable.get("INFLUX_DB_TOKEN"),
                                   org=Variable.get("INFLUX_DB_ORG"), database="tracker_stage_db")

    postgresHook = PostgresHook().get_hook(conn_id="postgres_tenlytics_write_connection_stage")
    postgresConnection = postgresHook.get_conn()

    result = postgresHook.get_first("SELECT 1")

    if result:
        print("PostgreSQL database is reachable.")

        quizDf = getQuizData(influxClient)
        transformedQuizDf = getTransformedData(quizDf)
        writeQuizData(transformedQuizDf, postgresConnection)

    else:
        raise ValueError("PostgreSQL database did not respond.")


@task()
def syncInfluxPollDataToPostgres(**kwargs):
    print("called syncInfluxPollDataToPostgres")


with DAG(dag_id="influx_quiz_to_postgres_etl", default_args=default_args,
         schedule_interval=None) as dag:
    syncInfluxQuizDataToPostgres(), syncInfluxPollDataToPostgres()
