from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG
from influxdb_client_3 import InfluxDBClient3

default_args = {
    "owner": "Md. Toufiqul Islam",
    "start_date": datetime(2024, 2, 21),
    'schedule_interval': '0 * * * *'  # every hour
}


@task()
def syncInfluxQuizDataToPostgres(**kwargs):
    print("called")

    client = InfluxDBClient3(token="your-token",
                             host="your-host",
                             org="your-org",
                             database="your-database")

    print(client)


with DAG(dag_id="influx_quiz_to_postgres_etl", default_args=default_args,
         schedule_interval=None) as dag:
    syncInfluxQuizDataToPostgres()
