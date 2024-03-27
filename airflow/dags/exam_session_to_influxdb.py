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
    "retries": 0
}

MONGO_DB_NAME = Variable.get("EXAM_DB_NAME")
INFLUXDB_BUCKET_NAME = Variable.get("INFLUX_DB_TRACKER_DB_NAME")
INFLUX_DB_MEASUREMENT = "exam_users_new"

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
    conf = kwargs['dag_run'].conf

    examId = conf.get('exam_id', None)
    examType = conf.get('exam_type', None)
    catalogProductId = conf.get("catalog_product_id", None)
    catalogSkuId = conf.get("catalog_sku_id", None)
    programId = conf.get("program_id", None)
    courseId = conf.get("course_id", None)
    platform = conf.get("platform", None)

    # print("Remotely received value of {} for key=message".
    # format(kwargs['dag_run'].conf['session_id']))

    print("type ", type(conf))
    if examId is None:
        raise ValueError("exam_id is required in conf")

    print("running for exam id ", examId)
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

    examServiceDB = mongoClient[MONGO_DB_NAME]
    userSessionsCollection = examServiceDB.get_collection("user-sessions")

    points = []
    count = 0

    for examSession in userSessionsCollection.find(
            {"exam_id": examId, "status": "end"}):
        startAtTime: datetime = examSession["start_at"]
        endAtTime: datetime = examSession["end_at"]

        point = Point.measurement(INFLUX_DB_MEASUREMENT).tag("exam_id", examId).tag("auth_user_id",
                                                                                    examSession[
                                                                                        "user_id"]).tag(
            "catalog_product_id", catalogProductId).tag("catalog_sku_id", catalogSkuId).tag("program_id",
                                                                                            programId).tag(
            "course_id", courseId).tag("platform", platform).tag("session", examSession["_id"]).tag(
            "type", examType).field("time_taken",
                                    examSession["time_taken"]).field(
            "total_answers", examSession["total_answers"]).field("total_correct_answers",
                                                                 examSession["total_correct_answers"]).field(
            "total_questions", examSession["total_questions"]).field("total_score", examSession["total_score"]).field(
            "start_at", int(startAtTime.timestamp() * 1000)).field("end_at", int(endAtTime.timestamp() * 1000)).time(
            startAtTime)
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


with DAG(dag_id="exam_session_to_influx_db_etl", default_args=default_args,
         schedule_interval=None) as dag:
    syncMongoDataToInflux()
