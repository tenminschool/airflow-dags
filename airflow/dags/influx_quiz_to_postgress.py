from datetime import datetime

from airflow.decorators import task
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.providers.influxdb.hooks.influxdb import InfluxDBClient
from airflow.providers.influxdb.operators.influxdb import InfluxDBOperator
from airflow.providers.influxdb.operators.influxdb import InfluxDBHook
from influxdb_client_3 import InfluxDBClient3, Point

default_args = {
    "owner": "Md. Toufiqul Islam",
    "start_date": datetime(2024, 2, 21),
    'schedule_interval': '0 * * * *'  # every hour
}


@task()
def syncInfluxQuizDataToPostgres(**kwargs):
    print("called")

    influxClient = InfluxDBClient(url=Variable.get("INFLUX_DB_URL"),
                                  token=Variable.get("INFLUX_DB_TOKEN"),
                                  org=Variable.get("INFLUX_DB_ORG"))
    pingRes = influxClient.ping()
    print("INFLUX PING RESPONSE", pingRes)

    # Step 2 : Query on measurement Initialize
    if not pingRes:
        raise ValueError("Cannot connect to InfluxDB")
    else:
        query = '''
          from(bucket: "tracker_stage_db")
          |> range(start: -365d)
          |> filter(fn: (r) =>
            r._measurement == "quiz_participants" and
            (r.modality == "m1" or r.modality == "m5") and
            r.auth_user_id != ""
          )
          |> group(columns: ["auth_user_id"])
          |> count(column: "quiz_id")
          |> sum(column: "is_correct")
          |> rename(columns: {count: "quiz_submitted", sum: "quiz_corrected"})
    '''
        query_api = influxClient.query_api()
        result = query_api.query(query=query)
        print("ROWS : ", result)


with DAG(dag_id="influx_quiz_to_postgres_etl", default_args=default_args,
         schedule_interval=None) as dag:
    query = """SELECT auth_user_id, COUNT(quiz_id) as quiz_submitted, SUM(is_correct) as quiz_corrected
    FROM quiz_participants
    WHERE time >= now() - interval '365 day'
      AND (modality='m1' OR modality='m5')
      AND auth_user_id IS NOT NULL
    GROUP BY auth_user_id"""
    execute_influxql_query = InfluxDBOperator(
        task_id='execute_influxql_query',
        influx_conn_id='your_influx_conn_id',  # Specify your InfluxDB connection ID
        query=query,
        do_xcom_push=True,  # Set to True if you want to push the query result to XCom
    )
