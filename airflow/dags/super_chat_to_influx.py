from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG

default_args = {
    "owner": "Papa Tiger",
    "start_date": datetime(2024,3,24),
    "retries": 1
}

@task()
def init_syncing_super_chat_data(**kwargs):
    print("called sync_super_chat_data")
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

    print(live_class_id, catalog_product_id, catalog_sku_id, program_id, course_id,
          media_type, platform, identification_type, identification_id)

@task()
def generatePostgresQuery():
    sql_query = f"""
    SELECT sessions."createdAt" as start_at, sessions.id, conversation_id, identification_type, identification_id, resolved_at as end_at,
       thread_id, initiated_member_id, members.auth_user_id, rating_type, rating_value, sessions.status
    FROM sessions
    INNER JOIN members ON sessions.initiated_member_id = members.id
    WHERE sessions.identification_type = 'live_class' AND identification_id = '1naPAnx4w1';
    """

    # WHERE
    # sessions.identification_type = 'live_class'
    # AND
    # identification_id = '{live_class_id}';
    print("Executing SQL query:")
    print(sql_query)
    return sql_query

with DAG(
        dag_id="super_chat_to_influx", 
        default_args=default_args, 
        schedule_interval=None) as dag:
    init_syncing_super_chat_data()
    query = generatePostgresQuery()
    print("Query Printing from DAG: ", query)