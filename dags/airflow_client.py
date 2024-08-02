# Import airflow library
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow import DAG

# Import modules
import module.retry_logic as retry
from module.object_client import MinioClient

# Import libraries
# from dotenv import dotenv_values
import json
import pandas as pd
from io import StringIO
import boto3
import psycopg2
from sqlalchemy import create_engine
import os

access_key = "minio"
secret_key = "minio123"

# Default arguments
default_args = {
    'owner': 'airflow',
    'email': ['scarletmoon2003@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id="airflow_client",
    start_date=datetime(year=2024,
                        month=7,
                        day=23),
    schedule_interval=timedelta(days=1),
    # on_success_callback=None,
    # on_failure_callback=,
    default_args=default_args
)

# Task functions

# Extract function

def extract_data_alpha_vantage(**kwargs):
    symbol = kwargs.get("symbol")
    API_key = kwargs.get("API_key")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={API_key}"
    response = retry.make_requests(url)
    data = response.json()["Time Series (Daily)"]
    return data

# Extract HTML content

# Load function

def load_data(**kwargs):
    data = kwargs.get("data")
    bucket_name = kwargs.get("bucket_name")
    object_name = kwargs.get("object_name")
    minio_client = MinioClient("minio", "9000", access_key=access_key, secret_key=secret_key)
    with open(f"data_{object_name}_{bucket_name}.json", "w") as f:
        json.dump(data, f)
    minio_client.put_object(
        bucket_name=bucket_name, obj_name=f'data_{object_name}_{bucket_name}.json', obj_file=f"data_{object_name}_{bucket_name}.json")

# Transform data

def transform_data(**kwargs):
    bucket_name_1 = kwargs.get("bucket_name_1")
    bucket_name_2 = kwargs.get("bucket_name_2")
    object_name = kwargs.get("object_name")
    minio_client = MinioClient("minio", "9000", access_key=access_key, secret_key=secret_key)
    object_data = minio_client.get_object(bucket_name_1, f'data_{object_name}_{bucket_name_1}.json').json()
    df = pd.DataFrame.from_dict(object_data, orient='index').reset_index()
    df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    df['Date'] = pd.to_datetime(df['Date'])
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, sep=",", index=False)
    s3_resource = boto3.resource(
        's3',
        endpoint_url="http://minio:9000",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=boto3.session.Config(signature_version='s3v4')
        )
    s3_resource.Object(bucket_name_2, f'df_{object_name}_{bucket_name_2}.csv').put(Body=csv_buffer.getvalue())
    
# Backup data

def backup_data(**kwargs):
    pass

# Export data

def export_data(**kwargs):
    bucket_name = kwargs.get("bucket_name")
    object_name = kwargs.get("object_name")
    
    minio_client = MinioClient("minio", "9000", access_key=access_key, secret_key=secret_key)
    list_objects = minio_client.list_objects(bucket_name=bucket_name)
    
    filename = f"df_{object_name}_{bucket_name}.csv"
    if filename in list_objects:
        df = pd.read_csv(minio_client.get_object(bucket_name=bucket_name, obj_name=filename), index_col=0)
    
    conn_string = "postgresql://username:password@host.docker.internal:5439/userdb"
    db = create_engine(conn_string)
    try:
        conn = db.connect()
        conn1 = psycopg2.connect(
            host="host.docker.internal",
            database="userdb",
            user="username",
            password="password",
            port="5439"
        )
    except Exception as e:
        raise ConnectionError(e)
    
    conn1.autocommit = True
    cursor = conn1.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {object_name}")
        cursor.execute(f"CREATE TABLE {object_name} (Date DATE, Open FLOAT, High FLOAT, Low FLOAT, Close FLOAT, Volume FLOAT)")
        df.to_sql(name=object_name, con=conn, if_exists='replace', index=False)
    except Exception as e:
        raise ValueError(e)
    conn1.commit()
    conn1.close()

# Task definition

extract_dag = PythonOperator(
    task_id="extract_dag",
    python_callable=extract_data_alpha_vantage,
    op_kwargs={
        "symbol": "IBM",
        "API_key": 'KFLTLHTFNSZS1BBJ'
    },
    dag=dag
)

load_dag = PythonOperator(
    task_id="load_dag",
    python_callable=load_data,
    op_kwargs={
        "data": extract_dag.output,
        "bucket_name": "bronze",
        "object_name": "IBM"
    },
    dag=dag
)

export_dag = PythonOperator(
    task_id="export_dag",
    python_callable=export_data,
    op_kwargs={
        "bucket_name": "silver",
        "object_name": "IBM"
    },
    dag=dag,
    on_success_callback=[retry.callback_on_success]
)

transform_dag = PythonOperator(
    task_id = "transform_dag",
    python_callable=transform_data,
    op_kwargs={
        "bucket_name_1": "bronze",
        "bucket_name_2": "silver",
        "object_name": "IBM"
    },
    dag=dag
)

extract_dag >> load_dag >> transform_dag >> export_dag