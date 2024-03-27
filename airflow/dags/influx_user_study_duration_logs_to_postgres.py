from airflow import DAG
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.influxdb.hooks.influxdb import InfluxDBClient, Point
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 24),
    'schedule_interval': '0 * * * *' # every hour
}


# Instantiate the DAG object
@dag(dag_id='influx_to_postgres_01', default_args=default_args, catchup=False, tags=['user_study_duration_logs'])
def influx_to_postgres_01():
    # Define tasks
    def print_hello():
        print("Hello, Airflow!")
        final_payload = [{'auth_user_id': 'auth_user_2', 'total_live_class_attended': 1}, {'auth_user_id': 'auth_user_3', 'total_live_class_attended': 2}]

        # Step 1 : Initialize InfluxDb Client
        influxClient = InfluxDBClient(url=Variable.get("INFLUX_DB_URL"),
                                      token=Variable.get("INFLUX_DB_TOKEN"),
                                      org=Variable.get("INFLUX_DB_ORG_NAME"))

        pingRes = influxClient.ping()
        print("INFLUX PING RESPONSE",pingRes)

        # Step 2 : Query on measurement Initialize
        if not pingRes:
            raise ValueError("Cannot connect to InfluxDB")
        else:
            query = f"""from(bucket: "tracker_stage_db")
            |> range(start: -2400h)
            |> filter(fn: (r) => r._measurement == "user_study_duration_logs") """

            query_api = influxClient.query_api()
            result = query_api.query(query)
            print("ROWS : ",len(result))

            # Step 3 : Transform Data
            for table in result:
                for record in table.records:
                    print(record.values)
                    print(record.values['auth_user_id'])
                    object1 = {"auth_user_id": record.values['auth_user_id'], "total_live_class_attended": 1}
                    final_payload.append(object1)

        # raise ValueError("Query API returned")

        # Step 4 : Create connection with Postgres

        # Instantiate PostgresHook with the connection ID
        pg_hook = PostgresHook(postgres_conn_id="postgres_tenlytics_db")

        # Ping the database
        pg_hook.get_conn()
        print("Successfully pinged PostgreSQL database")

        # Step 4 : Prepare Insert Values

        date_string = datetime.now().strftime('%Y-%m-%d 00:00:00.000000')
        datetime_string = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        print("OK")

        # Define the SQL command for bulk insert
        # Construct the bulk insert SQL command with string formatting
        values_str = ', '.join([f"('{datetime_string}', '{date_string}', '{item['auth_user_id']}', {item['total_live_class_attended']})"
                                for item in final_payload])
        print("values_str",values_str)

        insert_command = f"""
                INSERT INTO user_learning_reports (updated_at,day, auth_user_id, total_live_class_attended)
                VALUES {values_str}
                ON CONFLICT (day, auth_user_id) DO UPDATE
                SET total_live_class_attended = user_learning_reports.total_live_class_attended + EXCLUDED.total_live_class_attended,
                updated_at = NOW()
            """

        # Step 5: bulk insert using PostgreSQLHook
        rows_inserted = pg_hook.run(insert_command)
        print("rows_inserted",rows_inserted)

        # Log the number of rows inserted
        if rows_inserted is not None:
            print(f"Inserted {rows_inserted} rows into user_learning_reports.")


    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=print_hello
    )

    # Define task dependencies
    task_1


# Instantiate the DAG
dag = influx_to_postgres_01()
