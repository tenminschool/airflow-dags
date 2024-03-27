from datetime import datetime

from airflow.decorators import task
from airflow.models import Variable
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
    influxClient = InfluxDBClient3(url=Variable.get("INFLUX_DB_URL"),
                                   token=Variable.get("INFLUX_DB_TOKEN"),
                                   org=Variable.get("INFLUX_DB_ORG"))
    query = f"""SELECT * FROM quiz_participants"""
    reader = influxClient.query(query=query, language="sql")
    table = reader.read_all()
    print(table.to_pandas().to_markdown())


with DAG(dag_id="influx_quiz_to_postgres_etl", default_args=default_args,
         schedule_interval=None) as dag:
    syncInfluxQuizDataToPostgres()
