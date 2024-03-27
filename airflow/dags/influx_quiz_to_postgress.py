from datetime import datetime

from airflow.decorators import task
from airflow.models import Variable
from airflow.models.dag import DAG
from influxdb_client_3 import InfluxDBClient3
import pandas as pd
from pandas import DataFrame
import numpy as np

default_args = {
    "owner": "Md. Toufiqul Islam",
    "start_date": datetime(2024, 2, 21),
    'schedule_interval': '0 * * * *'  # every hour
}


def getQuizData(client: InfluxDBClient3):
    query = """SELECT auth_user_id, COUNT(quiz_id) as quiz_submitted, SUM(is_correct) as quiz_corrected
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


def getTransformedData(quizDf: DataFrame, pollDf: DataFrame):
    userIdSet = np.concatenate(quizDf['auth_user_id'].unique(), pollDf['auth_user_id'].unique())
    return userIdSet


@task()
def syncInfluxQuizDataToPostgres(**kwargs):
    print("called")
    client = InfluxDBClient3(host=Variable.get("INFLUX_DB_URL"), token=Variable.get("INFLUX_DB_TOKEN"),
                             org=Variable.get("INFLUX_DB_ORG"), database="tracker_stage_db")

    quizDf = getQuizData(client)
    pollDf = getPollData(client)
    transformedDf = getTransformedData(quizDf, pollDf)
    print(transformedDf)


with DAG(dag_id="influx_quiz_to_postgres_etl", default_args=default_args,
         schedule_interval=None) as dag:
    syncInfluxQuizDataToPostgres()
