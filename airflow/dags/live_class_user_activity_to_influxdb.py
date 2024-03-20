from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.influxdb.hooks.influxdb import InfluxDBClient, Point
from airflow.providers.mongo.hooks.mongo import MongoHook

default_args = {
    "owner": "Md. Toufiqul Islam",
    "start_date": datetime(2024, 2, 21),
    "retries": 1
}

liveClassId = "rtfXrc_UO"
catalogProductId = "rtfXrc_UO"
catalogSkuId = ""
programId = "1"
courseId = "23"
mediaType = "live_class"
platform = "k12"
identificationType = "live_class"
identificationId = "rtfXrc_UO"
auth_user_id = "sdfds"

MONGO_DB_NAME = "stage_liveclass_user_activity_db"
INFLUXDB_BUCKET_NAME = "tracker_stage_db"
INFLUX_DB_MEASUREMENT = "user_study_duration_logs"


@task()
def syncMongoDataToInflux(**kwargs):
    print("called")
    # print("Remotely received value of {} for key=message".
    # format(kwargs['dag_run'].conf['session_id']))
    mongoHook = MongoHook(mongo_conn_id="stage_mongo_db_connection")
    influxClient = InfluxDBClient(url="https://us-east-1-1.aws.cloud2.influxdata.com",
                                  token="-Eag6lpWIVBzsm8K1z3PtnSQxbLS8LOBNmIc1IYgcT6Y2RrMIFJtJv7LFZmHOYWkDMeye7oYiaolM8J8AtMAcA==",
                                  org="10MS")

    pingRes = influxClient.ping()
    print("pingRes ", pingRes)

    mongoClient = mongoHook.get_conn()
    userActivityMongoDb = mongoClient[MONGO_DB_NAME]
    userActivitiesCollection = userActivityMongoDb.get_collection("users_watch_activities")
    print("user activity mongo")

    for userActivity in userActivitiesCollection.find(
            {"live_class_id": liveClassId, "joining_at": {"$ne": None}, "leaving_at": {"$ne": None}}):
        print("called ", catalogSkuId)
        playHeadStartAt: datetime = userActivity["joining_at"]
        playHeadEndAt: datetime = userActivity["leaving_at"]

        point = Point.measurement(INFLUX_DB_MEASUREMENT).tag("media_id", liveClassId).tag("auth_user_id",
                                                                                          auth_user_id).tag(
            "catalog_product_id", catalogProductId).tag("catalog_sku_id", catalogSkuId).tag("program_id",
                                                                                            programId).tag(
            "course_id", courseId).tag("media_type", mediaType).tag("identification_id", identificationId).tag(
            "identification_type", identificationType).field("playhead_start_at",
                                                             int(playHeadStartAt.timestamp() * 1000)).field(
            "playhead_end_at", int(playHeadEndAt.timestamp() * 1000)).field("duration", userActivity["watch_time"])

        writeAPI = influxClient.write_api()
        writeAPI.write(INFLUXDB_BUCKET_NAME, org="10MS", record=point)


with DAG(dag_id="sync_live_class_user_activity_data_to_influxdb", default_args=default_args,
         schedule_interval=None) as dag:
    syncMongoDataToInflux()
