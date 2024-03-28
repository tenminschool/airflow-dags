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
 auth_user_id!='' AND type !='' AND time >= now() - interval '665 day' AND time_taken >0
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
        total_cq_submitted = 0
        total_question_exam_submitted = 0
        total_practice_question_exam_submitted = 0
        total_ielts_academic_reading_submitted = 0
        total_ielts_academic_writing_submitted = 0
        total_ielts_general_writing_submitted = 0
        total_ielts_general_reading_submitted = 0
        total_ielts_listening_submitted = 0
        total_cq_duration = 0
        total_question_exam_duration = 0
        total_practice_question_exam_duration = 0
        total_ielts_academic_reading_duration = 0
        total_ielts_academic_writing_duration = 0
        total_ielts_general_writing_duration = 0
        total_ielts_general_reading_duration = 0
        total_ielts_listening_duration = 0

        for index, row in filteredDf.iterrows():
            if row['type'] == 'cq':
                total_cq_submitted = row['total_submitted']
                total_cq_duration = row['total_duration']

            if row['type'] == 'question':
                total_question_exam_submitted = row['total_submitted']
                total_question_exam_duration = row['total_duration']

            if row['type'] == 'practice_question':
                total_practice_question_exam_submitted = row['total_submitted']
                total_practice_question_exam_duration = row['total_duration']

            if row['type'] == 'ielts_academic_reading':
                total_ielts_academic_reading_submitted = row['total_submitted']
                total_ielts_academic_reading_duration = row['total_duration']

            if row['type'] == 'ielts_academic_writing':
                total_ielts_academic_writing_submitted = row['total_submitted']
                total_ielts_academic_writing_duration = row['total_duration']

            if row['type'] == 'ielts_general_writing':
                total_ielts_general_writing_submitted = row['total_submitted']
                total_ielts_general_writing_duration = row['total_duration']

            if row['type'] == 'ielts_general_reading':
                total_ielts_general_reading_submitted = row['total_submitted']
                total_ielts_general_reading_duration = row['total_duration']

            if row['type'] == 'ielts_listening':
                total_ielts_listening_submitted = row['total_submitted']
                total_ielts_listening_duration = row['total_duration']

        data['auth_user_id'].append(userId)
        data['day'].append(getDay())
        data['total_cq_submitted'].append(total_cq_submitted)
        data['total_question_exam_submitted'].append(total_question_exam_submitted)
        data['total_practice_question_exam_submitted'].append(total_practice_question_exam_submitted)
        data['total_ielts_academic_reading_submitted'].append(total_ielts_academic_reading_submitted)
        data['total_ielts_academic_writing_submitted'].append(total_ielts_academic_writing_submitted)
        data['total_ielts_general_writing_submitted'].append(total_ielts_general_writing_submitted)
        data['total_ielts_general_reading_submitted'].append(total_ielts_general_reading_submitted)
        data['total_ielts_listening_submitted'].append(total_ielts_listening_submitted)
        data['total_cq_duration'].append(total_cq_duration)
        data['total_question_exam_duration'].append(total_question_exam_duration)
        data['total_practice_question_exam_duration'].append(total_practice_question_exam_duration)
        data['total_ielts_academic_reading_duration'].append(total_ielts_academic_reading_duration)
        data['total_ielts_academic_writing_duration'].append(total_ielts_academic_writing_duration)
        data['total_ielts_general_writing_duration'].append(total_ielts_general_writing_duration)
        data['total_ielts_general_reading_duration'].append(total_ielts_general_reading_duration)
        data['total_ielts_listening_duration'].append(total_ielts_listening_duration)

    return pd.DataFrame(data)


def writeExamData(transformedDf: DataFrame, postgresConnection):
    cursor = postgresConnection.cursor()

    for index, row in transformedDf.iterrows():
        query = """INSERT INTO user_learning_reports (day, auth_user_id, total_cq_submitted, total_question_exam_submitted,
                                   total_practice_question_exam_submitted, total_ielts_academic_reading_submitted,
                                   total_ielts_academic_writing_submitted, total_ielts_general_writing_submitted,
                                   total_ielts_general_reading_submitted, total_ielts_listening_submitted,
                                   total_cq_duration, total_question_exam_duration,
                                   total_practice_question_exam_duration, total_ielts_academic_reading_duration,
                                   total_ielts_academic_writing_duration, total_ielts_general_writing_duration,
                                   total_ielts_general_reading_duration, total_ielts_listening_duration)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (day, auth_user_id) DO UPDATE SET total_cq_submitted =user_learning_reports.total_cq_submitted + EXCLUDED.total_cq_submitted,
total_question_exam_submitted =user_learning_reports.total_question_exam_submitted + EXCLUDED.total_question_exam_submitted,
total_practice_question_exam_submitted =user_learning_reports.total_practice_question_exam_submitted + EXCLUDED.total_practice_question_exam_submitted,
total_ielts_academic_reading_submitted =user_learning_reports.total_ielts_academic_reading_submitted + EXCLUDED.total_ielts_academic_reading_submitted,
total_ielts_academic_writing_submitted =user_learning_reports.total_ielts_academic_writing_submitted + EXCLUDED.total_ielts_academic_writing_submitted,
total_ielts_general_writing_submitted =user_learning_reports.total_ielts_general_writing_submitted + EXCLUDED.total_ielts_general_writing_submitted,
total_ielts_general_reading_submitted =user_learning_reports.total_ielts_general_reading_submitted + EXCLUDED.total_ielts_general_reading_submitted,
total_ielts_listening_submitted =user_learning_reports.total_ielts_listening_submitted + EXCLUDED.total_ielts_listening_submitted,
total_cq_duration =user_learning_reports.total_cq_duration + EXCLUDED.total_cq_duration,
total_question_exam_duration =user_learning_reports.total_question_exam_duration + EXCLUDED.total_question_exam_duration,
total_practice_question_exam_duration =user_learning_reports.total_practice_question_exam_duration + EXCLUDED.total_practice_question_exam_duration,
total_ielts_academic_reading_duration =user_learning_reports.total_ielts_academic_reading_duration + EXCLUDED.total_ielts_academic_reading_duration,
total_ielts_academic_writing_duration =user_learning_reports.total_ielts_academic_writing_duration + EXCLUDED.total_ielts_academic_writing_duration,
total_ielts_general_writing_duration =user_learning_reports.total_ielts_general_writing_duration + EXCLUDED.total_ielts_general_writing_duration,
total_ielts_general_reading_duration =user_learning_reports.total_ielts_general_reading_duration + EXCLUDED.total_ielts_general_reading_duration,
total_ielts_listening_duration =user_learning_reports.total_ielts_listening_duration + EXCLUDED.total_ielts_listening_duration
"""
        cursor.execute(query,
                       [row["day"], row["auth_user_id"], row["total_cq_submitted"],
                        row['total_question_exam_submitted'], row['total_practice_question_exam_submitted'],
                        row['total_ielts_academic_reading_submitted'], row['total_ielts_academic_writing_submitted'],
                        row['total_ielts_general_writing_submitted'], row['total_ielts_general_reading_submitted'],
                        row['total_ielts_listening_submitted'], row['total_cq_duration'],
                        row['total_question_exam_duration'], row['total_practice_question_exam_duration'],
                        row['total_ielts_academic_reading_duration'], row['total_ielts_academic_writing_duration'],
                        row['total_ielts_general_writing_duration'], row['total_ielts_general_reading_duration'],
                        row['total_ielts_listening_duration']])

    postgresConnection.commit()
    cursor.close()
    postgresConnection.close()


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
        print("transformedExamData ", transformedExamData.to_markdown())
        writeExamData(transformedExamData, postgresConnection)


    else:
        raise ValueError("PostgreSQL database did not respond.")


with DAG(dag_id="influx_exam_to_postgres_etl", default_args=default_args, schedule_interval='@hourly') as dag:
    syncInfluxExamDataToPostgres()
