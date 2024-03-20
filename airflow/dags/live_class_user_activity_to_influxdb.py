import time
from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.influxdb.hooks.influxdb import InfluxDBClient, Point
from airflow.providers.mongo.hooks.mongo import MongoHook
from influxdb_client.client.write_api import WriteOptions, SYNCHRONOUS
from airflow.models import Variable

default_args = {
    "owner": "Md. Toufiqul Islam",
    "start_date": datetime(2024, 2, 21),
    "retries": 1
}

MONGO_DB_NAME = "stage_liveclass_user_activity_db"
INFLUXDB_BUCKET_NAME = "tracker_stage_db"
INFLUX_DB_MEASUREMENT = "user_study_duration_logs"

BATCH_SIZE = 100
DELAY_SEC = 3

options = WriteOptions(
    batch_size=100,
    flush_interval=10_000,
    jitter_interval=2_000,
    retry_interval=5_000,
    max_retries=5,
    max_retry_delay=30_000,
    exponential_base=2
)


@task()
def syncMongoDataToInflux(**kwargs):
    print("called")

    liveClassId = ""
    catalogProductId = ""
    catalogSkuId = ""
    programId = ""
    courseId = ""
    mediaType = "live_class"
    platform = ""
    identificationType = "live_class"
    identificationId = ""

    # print("Remotely received value of {} for key=message".
    # format(kwargs['dag_run'].conf['session_id']))

    conf = kwargs['dag_run'].conf
    if conf["live_class_id"] is None:
        raise ValueError("live_class_id is required in conf")
    liveClassId = conf["live_class_id"]

    if not (conf["catalog_product_id"] is None):
        catalogProductId = conf["catalog_product_id"]

    if not (conf["catalog_sku_id"] is None):
        catalogSkuId = conf["catalog_sku_id"]

    if not (conf["program_id"] is None):
        programId = conf["program_id"]

    if not (conf["couse_id"] is None):
        couse_id = conf["couse_id"]

    if not (conf["platform"] is None):
        platform = conf["platform"]

    print("running for liveclass id ", liveClassId)
    print("catalog product id ", catalogProductId)
    print("catalog sku id ", catalogSkuId)
    print("program id ", programId)
    print("course id ", programId)
    print("platform ", platform)

    mongoHook = MongoHook(mongo_conn_id="stage_mongo_db_connection")
    influxClient = InfluxDBClient(url=Variable.get("INFLUX_DB_URL"),
                                  token=Variable.get("INFLUX_DB_TOKEN"),
                                  org=Variable.get("INFLUX_DB_ORG"))

    pingRes = influxClient.ping()
    if not pingRes:
        raise ValueError("Cannot connect to InfluxDB")

    testConnectionRes = mongoHook.connection.test_connection()
    print("test connection ", testConnectionRes)

    mongoClient = mongoHook.get_conn()
    # if not testConnectionRes[0]:
    #     raise Exception("Cannot connect to Mongodb")

    userActivityMongoDb = mongoClient[MONGO_DB_NAME]
    userActivitiesCollection = userActivityMongoDb.get_collection("users_watch_activities")

    points = []
    count = 0

    for userActivity in userActivitiesCollection.find(
            {"live_class_id": liveClassId, "joining_at": {"$ne": None}, "leaving_at": {"$ne": None}}):
        playHeadStartAt: datetime = userActivity["joining_at"]
        playHeadEndAt: datetime = userActivity["leaving_at"]

        point = Point.measurement(INFLUX_DB_MEASUREMENT).tag("media_id", liveClassId).tag("auth_user_id",
                                                                                          userActivity[
                                                                                              "auth_user_id"]).tag(
            "catalog_product_id", catalogProductId).tag("catalog_sku_id", catalogSkuId).tag("program_id",
                                                                                            programId).tag(
            "course_id", courseId).tag("media_type", mediaType).tag("identification_id", identificationId).tag(
            "identification_type", identificationType).field("playhead_start_at",
                                                             int(playHeadStartAt.timestamp() * 1000)).field(
            "playhead_end_at", int(playHeadEndAt.timestamp() * 1000)).field("duration",
                                                                            userActivity["watch_time"]).time(
            playHeadStartAt)
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


with DAG(dag_id="live_class_user_activity_to_influx_db_etl", default_args=default_args,
         schedule_interval=None) as dag:
    syncMongoDataToInflux()
