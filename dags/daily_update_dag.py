# Import airflow library
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow import DAG

# Import modules
import dags.module.mail as retry
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
import json
import logging
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
import random
from concurrent.futures import ThreadPoolExecutor

# Load environment variables
config = dotenv_values("dags/.env")
access_key = config['MINIO_ROOT_USER']
secret_key = config['MINIO_ROOT_PASSWORD']
api_key = config['API_KEY']
header_csv = json.loads(config['header_csv'])
header_html = json.loads(config['header_html'])

# Connect to database
conn_string = config['conn_string']
db = create_engine(conn_string)
conn_1 = db.connect()
# conn_1.autocommit = True

conn = psycopg2.connect(
    host=config['host'],
    database=config['POSTGRESQL_DB'],
    user=config['POSTGRESQL_USER'],
    password=config['POSTGRESQL_PASSWORD'],
    port=config['PG_PORT']
)

cursor = conn.cursor()
conn.autocommit = True

# Default arguments
default_args = {
    'owner': 'airflow',
    'email': ['scarletmoon2003@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_success_callback': retry.email_on_success,
    'on_failure_callback': retry.email_on_failure
}

# DAG definition
dag = DAG(
    dag_id="daily_update_dag",
    start_date=datetime(year=2024,
                        month=7,
                        day=23),
    schedule_interval=timedelta(days=1),
    on_failure_callback=retry.email_on_failure,
    on_success_callback=retry.email_on_success,
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
        key_df = pd.concat([key_df, pd.DataFrame(
            [line[0]], columns=['Symbol'])], ignore_index=True)
    return key_df['Symbol'].tolist()

# def download_url(url):


def extract_data_weekly(**kwargs):
    # Arguments for the function
    symbols = kwargs.get("symbols")
    bucket_name = kwargs.get("bucket_name")

    # Retry strategy
    max_retries=5
    delay = 60

    # Header for requests
    headers = header_csv

    # Minio Client
    minio_client = MinioClient('minio', 9000, access_key, secret_key)

    # Get timestamp
    midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    a_week_ago = midnight - timedelta(days=7)
    current_day = int(midnight.timestamp())
    a_week_ago = int(a_week_ago.timestamp())
    
    url_1 = lambda symbol: f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={a_week_ago}&period2={current_day}&interval=1d&events=history&includeAdjustedClose=true"
    
    url_2 = lambda symbol: f"https://query2.finance.yahoo.com/v7/finance/download/{symbol}?period1={a_week_ago}&period2={current_day}&interval=1d&events=history&includeAdjustedClose=true"
    
    filename_func = lambda symbol: f"backup_{datetime.today().strftime('%Y-%m-%d')}/{symbol}.csv"
        
    urls_1 = [url_1(symbol) for symbol in symbols]
    urls_2 = [url_2(symbol) for symbol in symbols]
    inputs = zip(urls_1, urls_2, symbols)
    
    def download_url(args):
        url_1, url_2, symbol = args[0], args[1], args[2]
        start = time.time()
        for attempt in range(max_retries):
            try:
                response = requests.get(url_1, headers=headers)
                if response.status_code == 200:
                    with open(f'{symbol}.csv', 'wb') as f:
                        f.write(response.content)
                    minio_client.put_object(
                        bucket_name=bucket_name, obj_name=filename_func(symbol), obj_file=f'{symbol}.csv')
                    logging.info(f"Successful backup. Symbol: {symbol}. Runtime: {time.time() - start}")
                    return (symbol, time.time() - start)
                elif response.status_code == 429:
                    time.sleep(delay)
            except Exception as e:
                try:
                    response = requests.get(url_2, headers=headers)
                    if response.status_code == 200:
                        with open(f'{symbol}.csv', 'wb') as f:
                            f.write(response.content)
                        minio_client.put_object(
                            bucket_name=bucket_name, obj_name=filename_func(symbol), obj_file=f'{symbol}.csv')
                        logging.info(f"Successful backup. Symbol: {symbol}. Runtime: {time.time() - start}")
                        return (symbol, time.time() - start)
                    elif response.status_code == 429:
                        time.sleep(delay)
                except Exception as e:
                    logging.info(f"Failed backup. Symbol: {symbol}")
                    with open("dags/error_log_backup/error_log_weekly_data/error_log.txt", "a") as f:
                        f.write(f"{symbol}\n")
                    return (symbol, None)

    with ThreadPoolExecutor(max_workers=5) as executors:
        results = list(executors.map(download_url, inputs))
        


def backup_full(**kwargs):
    # Arguments for the function
    bucket_name_1 = kwargs.get("bucket_name_1")
    bucket_name_2 = kwargs.get("bucket_name_2")

    # Minio client
    minio_client = MinioClient('minio', 9000, access_key, secret_key)

    # Extract full historical data as of today as backup
    list_filenames = minio_client.list_objects(bucket_name=bucket_name_1)
        
    def update_backup(args):
        filename = args
        try:
            start = time.time()
            minio_client.copy_object(bucket_name_from=bucket_name_1, bucket_name_to=bucket_name_2,
                                        obj_name=filename, prefix_2=f"backup_{datetime.today().strftime('%Y-%m-%d')}_full")
            logging.info(f"Successful backup. Filename: {filename}. Runtime: {time.time() - start}")
            return(time.time() - start)
        except Exception as e:
            logging.info(f"Failed to backup file {filename}. Error: {e}")    
            
    with ThreadPoolExecutor(max_workers=5) as executors:
        results = list(executors.map(update_backup, list_filenames))

def daily_update(**kwargs):
    # # Arguments for the function
    bucket_name = kwargs.get("bucket_name")
    symbols = kwargs.get('symbols')

    # Minio client
    minio_client = MinioClient('minio', 9000, access_key, secret_key)

    # List symbols
    if symbols == None:
        symbols = [x.split(".")[0] for x in minio_client.list_objects(bucket_name=bucket_name)]

    # Retry strategy

    midnight = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    a_day_ago = midnight - timedelta(days=1)
    current_day = int(midnight.timestamp())
    a_day_ago = int(a_day_ago.timestamp())
    
    url_1 = lambda symbol: f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={a_day_ago}&period2={current_day}&interval=1d&events=history&includeAdjustedClose=true"
    url_2 = lambda symbol: f"https://query2.finance.yahoo.com/v7/finance/download/{symbol}?period1={a_day_ago}&period2={current_day}&interval=1d&events=history&includeAdjustedClose=true"

    urls_1 = [url_1(x) for x in symbols]
    urls_2 = [url_2(x) for x in symbols]
    inputs = zip(urls_1, urls_2, symbols)    

    # Download url
    def download_url(args):
        url_1, url_2, symbol = args[0], args[1], args[2]
        start = time.time()
        try:
            response = requests.get(url_1, headers=header_csv)
            # Check response status code
            if response.status_code == 200:
                latest_row_content = response.content.decode('utf-8').split("\n")[1].split(",").append(symbol)
                upsert_query = '''
                                INSERT INTO dim_hist_data ("Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "Symbol")
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT ("Symbol", "Date")
                                DO UPDATE SET
                                "Open" = EXCLUDED."Open",
                                "High" = EXCLUDED."High",
                                "Low" = EXCLUDED."Low",
                                "Close" = EXCLUDED."Close",
                                "Adj Close" = EXCLUDED."Adj Close",
                                "Volume" = EXCLUDED."Volume"    
                                '''
                cursor.execute(upsert_query, tuple(latest_row_content))
                return(time.time() - start)
            elif response.status_code == 429:
                logging.info(f"Failed to update latest rows for {symbol}. Status code: {response.status_code}.")
        except Exception as e:
            conn.rollback()
            try:
                response = requests.get(url_2, headers=header_csv)
                if response.status_code == 200:
                    latest_row_content = response.content.decode(
                        'utf-8').split("\n")[1].split(",").append(symbol)
                    upsert_query = '''
                                    INSERT INTO dim_hist_data ("Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "Symbol")
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT ("Symbol", "Date")
                                    DO UPDATE SET
                                    "Open" = EXCLUDED."Open",
                                    "High" = EXCLUDED."High",
                                    "Low" = EXCLUDED."Low",
                                    "Close" = EXCLUDED."Close",
                                    "Adj Close" = EXCLUDED."Adj Close",
                                    "Volume" = EXCLUDED."Volume"    
                                    '''
                    cursor.execute(upsert_query, tuple(latest_row_content))
                    return(time.time() - start)
                elif response.status_code == 429:
                    logging.info(f"Failed to update latest rows for {symbol}. Status code: {response.status_code}.")
            except Exception as e:
                conn.rollback()
                logging.info(
                        f"Failed to update latest rows for {symbol}. File {f'{symbol}.csv'} could not be downloaded.")
                with open("dags/error_log_backup/error_log_daily_update/error_log.txt", "a") as f:
                    f.write(f"{symbol}\n")

    with ThreadPoolExecutor(max_workers=5) as executors:
        results = list(executors.map(download_url, inputs))
    
    conn_1.close()
    conn.close()


def daily_update_staging(**kwargs):

    # Arguments for the function
    symbols = kwargs.get("symbols")
    bucket_name = kwargs.get("bucket_name")

    if symbols == None:
        symbols = [x.split(".")[0] for x in minio_client.list_objects(
            bucket_name=bucket_name)]

    midnight = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    current_day = int(midnight.timestamp())

    # Retry strategy
    max_retries = 5
    delay = 60

    # Header for requests
    headers = header_csv

    # Minio Client
    minio_client = MinioClient('minio', 9000, access_key, secret_key)
    
    def url_1(symbol): return f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1=942935400&period2={current_day}&interval=1d&events=history&includeAdjustedClose=true"
    
    def url_2(symbol): return f"https://query2.finance.yahoo.com/v7/finance/download/{symbol}?period1=942935400&period2={current_day}&interval=1d&events=history&includeAdjustedClose=true"
    
    filename_func = lambda symbol: f'{symbol}.csv'
    
    urls_1 = [url_1(symbol) for symbol in symbols]
    urls_2 = [url_2(symbol) for symbol in symbols]
    inputs = zip(urls_1, urls_2, symbols)
    
    def daily_update(args):
        start = time.time()
        url_1, url_2, symbol = args[0], args[1], args[2]
        filename = filename_func(symbol)
        for attempt in range(max_retries):
            try:
                response = requests.get(url_1, headers=headers)
                if response.status_code == 200:
                    with open(filename, 'wb') as f:
                        f.write(response.content)
                    minio_client.put_object(
                        bucket_name=bucket_name, obj_name=filename, obj_file=filename)
                    logging.info(
                        f'Successfully downloaded {filename}. Runtime: {time.time() - start}')
                    return (symbol, time.time() - start)
                elif response.status_code == 429:
                    time.sleep(delay)
            except Exception as e:
                try:
                    response = requests.get(url_2, headers=headers)
                    if response.status_code == 200:
                        with open(filename, 'wb') as f:
                            f.write(response.content)
                        minio_client.put_object(
                            bucket_name=bucket_name, obj_name=filename, obj_file=filename)
                        logging.info(
                            f'Successfully downloaded {filename}. Runtime: {time.time() - start}')
                        return (symbol, time.time() - start)
                    elif response.status_code == 429:
                        time.sleep(delay)
                except Exception as e:
                    logging.info(f'Failed to download {filename}. Error: {e}')
                    return (symbol, None)
        else:
            logging.info(
                f"Exceeded maximum retries ({max_retries}). File {f'{symbol}.csv'} could not be downloaded.")
            with open("dags/error_log_backup/error_log_daily_update/error_log_staging.txt", "a") as f:
                f.write(f"{symbol}\n")
            return (symbol, None)
        
    with ThreadPoolExecutor(max_workers=10) as executors:
        results = list(executors.map(daily_update, inputs))


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

update_daily_staging_dag = PythonOperator(
    task_id="daily_staging_update",
    python_callable=daily_update_staging,
    op_kwargs={
        "symbols": extract_symbol_dag.output,
        "bucket_name": 'hist-data'
    },
    dag=dag
)

update_daily_dag = PythonOperator(
    task_id="daily_update",
    python_callable=daily_update,
    op_kwargs={
        "symbols": extract_symbol_dag.output,
        "bucket_name": 'hist-data'
    },
    dag=dag
)

extract_symbol_dag >> weekly_data_backup_dag >> [
    update_daily_dag, update_daily_staging_dag] >> backup_full_dag
