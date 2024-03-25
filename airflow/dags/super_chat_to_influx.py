import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    "owner": "Papa Tiger",
    "start_date": datetime(2024, 3, 24),
    "retries": 1
}

def init_syncing_super_chat_data(**kwargs):
    logging.info("Called sync_super_chat_data")
    conf = kwargs['dag_run'].conf
    live_class_id = conf.get('live_class_id', None)
    catalog_product_id = conf.get("catalog_product_id", None)
    catalog_sku_id = conf.get("catalog_sku_id", None)
    program_id = conf.get("program_id", None)
    course_id = conf.get("course_id", None)
    media_type = "live_class"
    platform = conf.get("platform", None)
    identification_type = "live_class"
    identification_id = live_class_id

    logging.info(f"Parameters: {live_class_id}, {catalog_product_id}, {catalog_sku_id}, {program_id}, {course_id}, "
                 f"{media_type}, {platform}, {identification_type}, {identification_id}")

def generate_postgres_query():
    sql_query = """
    SELECT sessions."createdAt" as start_at, sessions.id, conversation_id, identification_type, identification_id, resolved_at as end_at,
       thread_id, initiated_member_id, members.auth_user_id, rating_type, rating_value, sessions.status
    FROM sessions
    INNER JOIN members ON sessions.initiated_member_id = members.id
    WHERE sessions.identification_type = 'live_class' AND identification_id = '1naPAnx4w1';
    """

    logging.info("Executing SQL query:")
    logging.info(sql_query)
    return sql_query

def ping_postgres():
    try: 
        postgres_hook = PostgresHook(postgres_conn_id="postgres_connection_stage")
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
    )
    
    ping_db = PythonOperator(
        task_id='ping_db',
        python_callable=ping_postgres, 
    )
    
    query_task = PythonOperator(
        task_id='query_task',
        python_callable=generate_postgres_query,
    )

    init_task >> ping_db >> query_task 
