from influxdb_client_3 import InfluxDBClient3
import pandas as pd

INFLUX_TOKEN = "-Eag6lpWIVBzsm8K1z3PtnSQxbLS8LOBNmIc1IYgcT6Y2RrMIFJtJv7LFZmHOYWkDMeye7oYiaolM8J8AtMAcA=="
URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
ORG = "10MS"
client = InfluxDBClient3(host=URL, token=INFLUX_TOKEN,
                         org=ORG, database="tracker_stage_db")

query = """SELECT auth_user_id, COUNT(quiz_id) as quiz_submitted, SUM(is_correct) as quiz_corrected
    FROM quiz_participants
    WHERE time >= now() - interval '365 day'
      AND (modality='m1' OR modality='m5')
      AND auth_user_id IS NOT NULL
    GROUP BY auth_user_id"""
reader = client.query(query=query, language="sql")
df = pd.DataFrame(reader.to_pandas())
print("df ", df)
