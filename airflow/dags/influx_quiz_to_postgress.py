from datetime import datetime

from airflow.decorators import task
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.providers.influxdb.hooks.influxdb import InfluxDBClient
from airflow.providers.influxdb.operators.influxdb import InfluxDBOperator
from airflow.providers.influxdb.operators.influxdb import InfluxDBHook
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
          |> rename(columns: {count: "quiz_submitted", sum: "quiz_corrected"})
    '''
        query_api = influxClient.query_api()
        result = query_api.query(query=query)
        print("ROWS : ", result)


with DAG(dag_id="influx_quiz_to_postgres_etl", default_args=default_args,
         schedule_interval=None) as dag:
    syncInfluxQuizDataToPostgres()
