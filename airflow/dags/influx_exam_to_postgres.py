from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from influxdb_client_3 import InfluxDBClient3
from pandas import DataFrame

default_args = {
    "owner": "Md. Toufiqul Islam",
    "start_date": datetime(2024, 2, 21),
}


def getDay():
    current_time = datetime.now()

    # Get start of the day (midnight)
    start_of_day = current_time.replace(hour=0, minute=0, second=0, microsecond=0)

    print("Start of the day:", start_of_day)
    return start_of_day


def getExamData(client: InfluxDBClient3):
    query = """SELECT auth_user_id,type,COUNT(*) as total_submitted, SUM(time_taken) as total_duration FROM exam_users_new WHERE
 auth_user_id!='' AND type !='' AND time >= now() - interval '665 day'
  GROUP BY auth_user_id,type"""
    reader = client.query(query=query, language="sql")
    return pd.DataFrame(reader.to_pandas())


def getTransformedExamData(examDf: DataFrame):
    data = {'auth_user_id': [],
            'day': [],
            'total_cq_submitted': [],
            'total_question_exam_submitted': [],
            'total_practice_question_exam_submitted': [],
            'total_ielts_academic_reading_submitted': [],
            'total_ielts_academic_writing_submitted': [],
            'total_ielts_general_writing_submitted': [],
            'total_ielts_general_reading_submitted': [],
            'total_ielts_listening_submitted': [],
            'total_cq_duration': [],
            'total_question_exam_duration': [],
            'total_practice_question_exam_duration': [],
            'total_ielts_academic_reading_duration': [],
            'total_ielts_academic_writing_duration': [],
            'total_ielts_general_writing_duration': [],
            'total_ielts_general_reading_duration': [],
            'total_ielts_listening_duration': [],
            }

    uniqueAuthUserIds = examDf['auth_user_id'].unique()

    for userId in uniqueAuthUserIds:
        filteredDf = examDf[examDf['auth_user_id'] == userId]

        for index, row in filteredDf.iterrows():
            data['auth_user_id'].append(userId)
            data['day'].append(getDay())

            if row['type'] == 'cq':
                data['total_cq_submitted'].append(row['total_submitted'])
                data['total_cq_duration'].append(row['total_duration'])

            if row['type'] == 'question':
                data['total_question_exam_submitted'].append(row['total_submitted'])
                data['total_question_exam_duration'].append(row['total_duration'])

            if row['type'] == 'practice_question':
                data['total_practice_question_exam_submitted'].append(row['total_submitted'])
                data['total_practice_question_exam_duration'].append(row['total_duration'])

            if row['type'] == 'ielts_academic_reading':
                data['total_ielts_academic_reading_submitted'].append(row['total_submitted'])
                data['total_ielts_academic_reading_duration'].append(row['total_duration'])

            if row['type'] == 'ielts_academic_writing':
                data['total_ielts_academic_writing_submitted'].append(row['total_submitted'])
                data['total_ielts_academic_writing_duration'].append(row['total_duration'])

            if row['type'] == 'ielts_general_writing':
                data['total_ielts_general_writing_submitted'].append(row['total_submitted'])
                data['total_ielts_general_writing_duration'].append(row['total_duration'])

            if row['type'] == 'ielts_general_reading':
                data['total_ielts_general_reading_submitted'].append(row['total_submitted'])
                data['total_ielts_general_reading_duration'].append(row['total_duration'])

            if row['type'] == 'ielts_listening':
                data['total_ielts_listening_submitted'].append(row['total_submitted'])
                data['total_ielts_listening_duration'].append(row['total_duration'])

    return pd.DataFrame(data)


@task()
def syncInfluxExamDataToPostgres(**kwargs):
    influxClient = InfluxDBClient3(host=Variable.get("INFLUX_DB_URL"), token=Variable.get("INFLUX_DB_TOKEN"),
                                   org=Variable.get("INFLUX_DB_ORG"), database="tracker_stage_db")

    postgresHook = PostgresHook().get_hook(conn_id="postgres_tenlytics_write_connection_stage")
    postgresConnection = postgresHook.get_conn()

    result = postgresHook.get_first("SELECT 1")

    if result:
        print("PostgreSQL database is reachable.")

        examDf = getExamData(influxClient)
        transformedExamData = getTransformedExamData(examDf)
        print("transformedExamData ", transformedExamData)

    else:
        raise ValueError("PostgreSQL database did not respond.")


with DAG(dag_id="influx_exam_to_postgres_etl", default_args=default_args, schedule_interval='@hourly') as dag:
    syncInfluxExamDataToPostgres()