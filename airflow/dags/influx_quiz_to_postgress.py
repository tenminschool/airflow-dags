from datetime import datetime

from airflow.decorators import task
from airflow.models import Variable
from airflow.models.dag import DAG
from influxdb_client.client.influxdb_client import InfluxDBClient

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
        query = f"SELECT auth_user_id,COUNT(quiz_id) as quiz_submitted, SUM(is_correct) as quiz_corrected FROM quiz_participants WHERE time >= now() - interval '365 day' AND modality='m1' AND auth_user_id is not null GROUP BY auth_user_id;"
        query_api = influxClient.query_api()
        result = query_api.query(query)
        print("ROWS : ", result)


with DAG(dag_id="influx_quiz_to_postgres_etl", default_args=default_args,
         schedule_interval=None) as dag:
    syncInfluxQuizDataToPostgres()
