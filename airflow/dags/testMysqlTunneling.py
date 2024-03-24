import pandas as pd
import pymysql
import logging
import sshtunnel
from sshtunnel import SSHTunnelForwarder
from airflow.models import Variable
import base64

default_args = {
    "owner": "Arnob",
    "start_date": datetime(2024, 2, 21),
    "retries": 1
}

ssh_host = Variable.get("STAGE_BESTION_IP")
ssh_username = 'ubuntu'
ssh_password = ''
database_username = Variable.get("STAGE_MYSQL_USERNAME")
database_password = Variable.get("STAGE_MYSQL_PASSWORD")
database_name = 'affiliate_v3'
localhost = Variable.get("STAGE_MYSQL_DB")

def open_ssh_tunnel(verbose=False):
    """Open an SSH tunnel and connect using a username and password.
    
    :param verbose: Set to True to show logging
    :return tunnel: Global SSH tunnel connection
    """
    
    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
    
    global tunnel
    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username = ssh_username,
        ssh_pkey="10-minschool-stage-k8s-new.pem",
        remote_bind_address = (Variable.get("STAGE_MYSQL_DB"), 3306)
    )
    
    tunnel.start()

def mysql_connect():
    """Connect to a MySQL server using the SSH tunnel connection
    
    :return connection: Global MySQL database connection
    """
    
    global connection
    
    connection = pymysql.connect(
        host=Variable.get("STAGE_MYSQL_DB"),
        user=database_username,
        passwd=database_password,
        db=database_name,
        port=tunnel.local_bind_port
    )

def run_query(sql):
    """Runs a given SQL query via the global database connection.
    
    :param sql: MySQL query
    :return: Pandas dataframe containing results
    """
    
    return pd.read_sql_query(sql, connection)

def mysql_disconnect():
    """Closes the MySQL database connection.
    """
    
    connection.close()

def close_ssh_tunnel():
    """Closes the SSH tunnel connection.
    """
    
    tunnel.close

@task()
def testMysqlTunneling(**kwargs):
    open_ssh_tunnel()
    mysql_connect()
    df = run_query("SELECT * FROM orders ORDER BY id DESC LIMIT 100")
    df.head()
    mysql_disconnect()
    close_ssh_tunnel()


with DAG(dag_id="testing_mysql_connect_dag", default_args=default_args, schedule_interval=None) as dag:
    testMysqlTunneling()
   


