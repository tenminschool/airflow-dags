import logging
from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.influxdb.hooks.influxdb import InfluxDBClient, Point
from influxdb_client.client.write_api import WriteOptions, SYNCHRONOUS
from airflow.models import Variable
from airflow.operators.email import EmailOperator

INFLUXDB_BUCKET_NAME = "tracker_stage_db"
INFLUX_DB_MEASUREMENT = "doubt_solve_records"
BATCH_SIZE = 100
DELAY_SEC = 3

default_args = {
    "owner": "Papa Tiger",
    "start_date": datetime(2024, 3, 24),
    "retries": 1,
    "email": ["osman@10minuteschool.com"],
    "email_on_failure": True,
}

'''
TO DO: 
1. Make dynamic the live class data fetching
2. ping influx db in different
3. make different function for influx data insert
'''

def init_syncing_super_chat_data(**kwargs):
    conf = kwargs['dag_run'].conf
    live_class_id = conf.get('live_class_id', None)
    catalog_product_id = conf.get("catalog_product_id", None)
    catalog_sku_id = conf.get("catalog_sku_id", None)
    program_id = conf.get("program_id", None)
    course_id = conf.get("course_id", None)
    platform = conf.get("platform", None)

    kwargs['ti'].xcom_push(key='live_class_id', value=live_class_id)
    kwargs['ti'].xcom_push(key='catalog_product_id', value=catalog_product_id)
    kwargs['ti'].xcom_push(key='catalog_sku_id', value=catalog_sku_id)
    kwargs['ti'].xcom_push(key='program_id', value=program_id)
    kwargs['ti'].xcom_push(key='course_id', value=course_id)
    kwargs['ti'].xcom_push(key='platform', value=platform)

def generate_postgres_query(live_class_id):
    sql_query = f"""
    SELECT 
        sessions."createdAt" as start_at, 
        sessions.id, 
        conversation_id, 
        identification_type, 
        identification_id, 
        resolved_at as end_at,
        thread_id, 
        initiated_member_id, 
        members.auth_user_id, 
        rating_type, 
        rating_value, 
        sessions.status
 
    FROM sessions
    INNER JOIN members ON sessions.initiated_member_id = members.id
    WHERE resolved_at is not null 
    AND sessions.identification_type = 'live_class' 
    AND identification_id = '{live_class_id}';
    """
    return sql_query


def execute_query_and_fetch_result(**kwargs):
    points = []
    count = 0

    try:
        live_class_id = kwargs['ti'].xcom_pull(key='live_class_id', task_ids='init_task')
        catalog_product_id = kwargs['ti'].xcom_pull(key='catalog_product_id', task_ids='init_task')
        catalog_sku_id = kwargs['ti'].xcom_pull(key='catalog_sku_id', task_ids='init_task')
        program_id = kwargs['ti'].xcom_pull(key='program_id', task_ids='init_task')
        course_id = kwargs['ti'].xcom_pull(key='course_id', task_ids='init_task')
        platform = kwargs['ti'].xcom_pull(key='platform', task_ids='init_task')

        sql_query = generate_postgres_query(live_class_id)
        postgres_hook = PostgresHook(postgres_conn_id="postgres_connection_stage")
        
        results = postgres_hook.get_records(sql_query)
        
        influxClient = InfluxDBClient(url=Variable.get("INFLUX_DB_URL"),token=Variable.get("INFLUX_DB_TOKEN"),org=Variable.get("INFLUX_DB_ORG"))

        ping_res = influxClient.ping()
        if not ping_res:
            raise ValueError("Cannot connect to InfluxDB")
																																		
        for row in results:
            start_at = row[0]
            session_id = row[1]
            conversation_id = row[2]
            identification_type = row[3]
            identification_id = row[4]
            end_at = row[5]
            thread_id = row[6]
            initiated_member_id = row[7]
            auth_user_id = row[8]
            rating_type = row[9]
            rating_value = row[10]
            status = row[11]
                
            point = Point.measurement(INFLUX_DB_MEASUREMENT) \
                .tag("liveclass_id", identification_id) \
                .tag("auth_user_id", auth_user_id) \
                .tag("thread_id", thread_id) \
                .tag("catalog_product_id", catalog_product_id) \
                .tag("catalog_sku_id", catalog_sku_id) \
                .tag("program_id", program_id) \
                .tag("course_id", course_id) \
                .tag("platform", platform) \
                .tag("status", status) \
                .tag("initiated_member_id", initiated_member_id) \
                .tag("conversation_id", conversation_id) \
                .field("session_id", session_id) \
                .field("start_at", int(start_at.timestamp() * 1000)) \
                .field("end_at", int(end_at.timestamp() * 1000)) \
                .field("resolved_at", int(start_at.timestamp() * 1000)) \
                .field("rating_type", rating_type) \
                .field("rating_value", rating_value) \
                .time(start_at)
            points.append(point)
            count += 1
            if len(points) == BATCH_SIZE:
                writeAPI = influxClient.write_api(options=SYNCHRONOUS)
                result = writeAPI.write(INFLUXDB_BUCKET_NAME, org=Variable.get("INFLUX_DB_ORG"), record=points)
                points = []
                print("Finished writing ", count, result)
                time.sleep(3)

        if len(points) > 0:
            writeAPI = influxClient.write_api(options=SYNCHRONOUS)
            writeAPI.write(INFLUXDB_BUCKET_NAME, org=Variable.get("INFLUX_DB_ORG"), record=points)
            print("Finished writing ", count)
            time.sleep(DELAY_SEC)

        logging.info("POINTS:::::::: ", points)
        logging.info("COUNTs:::: ", count)

    except Exception as e:
        logging.error(f"Error executing SQL query: {e}")

def ping_postgres():
    try: 
        postgres_hook = PostgresHook(postgres_conn_id="postgres_connection_stage0")
        result = postgres_hook.get_first("SELECT 1")

        if result: 
            logging.info("PostgreSQL database is reachable.")
        else:
            logging.error("PostgreSQL database did not respond.")
    except Exception as e:
        logging.error(f"Error pinging PostgreSQL database: {e}")


with DAG("super_chat_to_influx", default_args=default_args, schedule_interval=None) as dag:
    init_task = PythonOperator(
        task_id='init_task',
        python_callable=init_syncing_super_chat_data,
        provide_context=True,
    )
    
    ping_db = PythonOperator(
        task_id='ping_db',
        python_callable=ping_postgres, 
    )

    execute_query_task = PythonOperator(
        task_id='execute_query_task',
        python_callable=execute_query_and_fetch_result,
        provide_context=True,
    )

    email_on_failure = EmailOperator(
        task_id='email_on_failure',
        to=default_args["email"],
        subject='Airflow Alert: Task Failed',
        html_content='<p>Superchat to InfluxDB task in our DAG has failed. Please check the Airflow UI for more details.</p>',
        trigger_rule='one_failed', 
    )

    init_task >> ping_db 
    ping_db >> execute_query_task >> email_on_failure
