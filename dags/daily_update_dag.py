# Import airflow library
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow import DAG

# Import modules
import module.retry_logic as retry
from module.object_client import MinioClient

# Import libraries
from dotenv import dotenv_values
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import requests
import time
from datetime import datetime, timezone
import csv
import io

# Load environment variables
config = dotenv_values("dags/.env")
access_key = config['MINIO_ROOT_USER']
secret_key = config['MINIO_ROOT_PASSWORD']
api_key = config['API_KEY']

# Connect to database
conn_string = 'postgresql://username:password@host.docker.internal:5439/userdb'
db = create_engine(conn_string)
conn_1 = db.connect()
conn_1.autocommit = True

conn = psycopg2.connect(
    host="host.docker.internal",
    database="userdb",
    user="username",
    password="password",
    port=5439
)

cursor = conn.cursor()
conn.autocommit = True

# Default arguments
default_args = {
    'owner': 'airflow',
    'email': ['scarletmoon2003@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
}

# DAG definition
dag = DAG(
    dag_id="daily_update_dag",
    start_date=datetime(year=2024,
                        month=7,
                        day=23),
    schedule_interval=timedelta(days=1),
    on_success_callback=None,
    on_failure_callback=retry.callback_on_failure,
    default_args=default_args
)

# Extract symbols
def extract_symbol_list():
    key_df = pd.DataFrame(columns=['Symbol'])
    with requests.Session() as s:
        CSV_URL = f'https://www.alphavantage.co/query?function=LISTING_STATUS&state=active&apikey={api_key}'
        download = s.get(CSV_URL).content.decode('utf-8')
        csv_content = csv.reader(download.splitlines(), delimiter=',')
    for line in csv_content:
        if line[0] == 'symbol':
            continue
        key_df = pd.concat([key_df, pd.DataFrame([line[0]], columns=['Symbol'])], ignore_index=True)
    return key_df['Symbol'].tolist()

# Extract daily data, up to 7 backups
def extract_data_weekly(**kwargs):
    # Arguments for the function
    symbols = kwargs.get("symbols")
    bucket_name = kwargs.get("bucket_name")
    
    # Retry strategy
    max_retries = 5
    delay = 60
    
    # Header for requests
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    # Minio Client
    minio_client = MinioClient('minio', 9000, access_key, secret_key)
    
    # Get timestamp
    midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    a_week_ago = midnight - timedelta(days=7)
    current_day = int(midnight.timestamp())
    a_week_ago = int(a_week_ago.timestamp())
        
    # Extract CSV content 
    for symbol in symbols:
        
        # Filename 
        filename = f'backup_{datetime.today().strftime('%Y-%m-%d')}/{symbol}.csv'
        # list_objects = minio_client.list_objects(bucket_name=bucket_name)
        # if filename in list_objects:
        #     continue
        
        # Retry logic attempt
        for attempt in range(max_retries):
            
            # URL for requests
            url = lambda x: f"https://query{x}.finance.yahoo.com/v7/finance/download/{symbol}?period1={a_week_ago}&period2={current_day}&interval=1d&events=history&includeAdjustedClose=true"
            
            # Check query1
            response = requests.get(url(1), headers=headers)
            
            # Check response status code
            if response.status_code == 200:
                with open(f'{symbol}.csv', 'wb') as f:
                    f.write(response.content)
                minio_client.put_object(bucket_name=bucket_name, obj_name=filename, obj_file=f'{symbol}.csv')
                print(f'Successfully downloaded {symbol} to {filename}')
                break
            elif response.status_code == 429:
                time.sleep(delay)
            # query1 failed
            else:
                # Check query2
                response = requests.get(url(2), headers=headers)
                # Check response status code
                if response.status_code == 200:
                    with open(f'{symbol}.csv', 'wb') as f:
                        f.write(response.content)
                    minio_client.put_object(bucket_name=bucket_name, obj_name=filename, obj_file=f'{symbol}.csv')
                    print(f'Successfully downloaded {symbol} to {filename}')
                    break
                elif response.status_code == 429:
                    time.sleep(delay)
                # query2 failed
                else:
                    print(f'Failed to download {symbol} to {filename}')
                    break
        # Pass retry limit
        else:
            print(f"Exceeded maximum retries ({max_retries}). File could not be downloaded.")

# HTML content backup

def backup_html_content(**kwargs):
    symbols = kwargs.get("symbols")
    bucket_name = kwargs.get("bucket_name")
    
    pass

def backup_full(**kwargs):
    # Arguments for the function
    bucket_name_1 = kwargs.get("bucket_name_1")
    bucket_name_2 = kwargs.get("bucket_name_2")
    
    # Minio client
    minio_client = MinioClient('minio', 9000, access_key, secret_key)
    
    # Extract full historical data as of today as backup
    list_filenames = minio_client.list_objects(bucket_name=bucket_name_1)

    # Secure full backup 
    for filename in list_filenames:
        csv_content = minio_client.get_object(bucket_name=bucket_name_1, obj_name=filename).data.decode('utf-8')
        csv_reader = csv.reader(io.StringIO(csv_content))
        
        rows = [line for line in csv_reader]
        symbol = filename.split('.')[0]
        new_filename = f"backup_{datetime.today().strftime('%Y-%m-%d')}_full/{symbol}.csv"
        with open(f'{symbol}.csv', 'w') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerows(rows)
        minio_client.put_object(bucket_name=bucket_name_2, obj_name=new_filename, obj_file=f'{symbol}.csv')
        print(f"Backup file {filename} successfully.")
        
def daily_update(**kwargs):
    # Arguments for the function
    bucket_name = kwargs.get("bucket_name")
    
    # Minio client
    minio_client = MinioClient('minio', 9000, access_key, secret_key)
    
    # List symbols
    list_objects = minio_client.list_objects(bucket_name=bucket_name)
    symbols = [x.split(".")[0] for x in list_objects]
    
    # Retry strategy
    max_retries = 5
    
    midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    a_day_ago = midnight - timedelta(days=1)
    current_day = int(midnight.timestamp())
    a_day_ago = int(a_day_ago.timestamp())
    
    for symbol in symbols:
        
        # Extract CSV content from bucket
        csv_content_raw = minio_client.get_object(bucket_name=bucket_name, obj_name=f'{symbol}.csv').data.decode('utf-8')
        
        # Header for requests
        headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        # Start reading the CSV content
        csv_reader = csv.reader(io.StringIO(csv_content_raw))
        temp_row_holder = [row for row in csv_reader]
        latest_row = temp_row_holder[-1]
        if latest_row[0] == (midnight - timedelta(days=1)).strftime("%Y-%m-%d"):
            continue
        
        for attempt in range(max_retries):
        
            # Extract the latest row from website
            url = lambda x: f"https://query{x}.finance.yahoo.com/v7/finance/download/{symbol}?period1={a_day_ago}&period2={current_day}&interval=1d&events=history&includeAdjustedClose=true"
            
            # Check response
            response = requests.get(url(1), headers=headers)
            
            # Check response status code
            if response.status_code == 200:
                new_csv_content = response.content.decode('utf-8').split("\n")[1].split(",")
                temp_row_holder.append(new_csv_content)
                with open(f'{symbol}.csv', 'w') as f:
                    csv_writer = csv.writer(f)
                    csv_writer.writerows(temp_row_holder)
                minio_client.put_object(bucket_name = bucket_name, obj_name=f'{symbol}.csv', obj_file=f'{symbol}.csv')
                print(f"Successfully updated {symbol}.csv")
                break
            else:
                response = requests.get(url(2), headers=headers)
                if response.status_code == 200:
                    new_csv_content = response.content.decode('utf-8').split("\n")[1].split(",")
                    temp_row_holder.append(new_csv_content)
                    with open(f'{symbol}.csv', 'w') as f:
                        csv_writer = csv.writer(f)
                        csv_writer.writerows(temp_row_holder)
                    minio_client.put_object(bucket_name = bucket_name, obj_name=f'{symbol}.csv', obj_file=f'{symbol}.csv')
                    print(f"Successfully updated {symbol}.csv")
                    break
                else:
                    print(f"Failed to update latest rows for {symbol}.csv")
                    break
        else:
            print(f"Exceeded maximum retries. File {symbol}.csv could not be updated.")

extract_symbol_dag = PythonOperator(
    task_id="extract_symbol",
    python_callable=extract_symbol_list,
    dag=dag
)

weekly_data_backup_dag = PythonOperator(
    task_id="weekly_data_backup",
    python_callable=extract_data_weekly,
    op_kwargs={
        "bucket_name": 'backup-weekly',
        "symbols": extract_symbol_dag.output
    },
    dag=dag
)

backup_full_dag = PythonOperator(
    task_id="full_backup",
    python_callable=backup_full,
    op_kwargs={
        "bucket_name_1": "hist-data",
        "bucket_name_2": "backup-weekly",
    },
    dag=dag
)

update_daily_dag = PythonOperator(
    task_id="daily_update",
    python_callable=daily_update,
    op_kwargs={
        "bucket_name": 'hist-data'
    },
    dag=dag
)
    
extract_symbol_dag >> weekly_data_backup_dag >> update_daily_dag >> backup_full_dag