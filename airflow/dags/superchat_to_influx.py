from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG

default_args = {
    "owner": "Papa Tiger",
    "start_date": datetime(2024,3,24),
    "retries": 1
}

@task()
def sync_super_chat_data(**kwargs):
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
    identification_id = conf.get('live_class_id', None)

    print(live_class_id, catalog_product_id, catalog_sku_id, program_id, course_id,
          media_type, platform, identification_type, identification_id)

with DAG(dag_id="superchat_to_influx", default_args=default_args, schedule_interval=None) as dag:
    sync_super_chat_data()