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

# Extract daily data, up to 7 backups


def extract_data_weekly(**kwargs):
    # Arguments for the function
    symbols = kwargs.get("symbols")
    bucket_name = kwargs.get("bucket_name")

    # Retry strategy
    max_retries = 5
    delay = 60

    # Header for requests
    headers = header_csv

    # Minio Client
    minio_client = MinioClient('minio', 9000, access_key, secret_key)

    # Get timestamp
    midnight = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    a_week_ago = midnight - timedelta(days=7)
    current_day = int(midnight.timestamp())
    a_week_ago = int(a_week_ago.timestamp())

    # Checkpoint for first error symbol
    check = False
    first_error = None

    # List error symbols
    with open("dags/error_log_backup/error_log_weekly_data/error_log.txt", "r") as f:
        lines = f.readlines()
        if len(lines) == 0:
            check = True
        else:
            first_error = lines[0]

    # Extract CSV content
    for symbol in symbols:

        if symbol == first_error:
            check = True

        if check == False:
            continue

        # Filename
        filename = f'backup_{datetime.today().strftime(' % Y-%m-%d')}/{symbol}.csv'

        # Retry logic attempt
        for attempt in range(max_retries):

            # URL for requests
            def url(
                x): return f"https://query{x}.finance.yahoo.com/v7/finance/download/{symbol}?period1={a_week_ago}&period2={current_day}&interval=1d&events=history&includeAdjustedClose=true"

            # Check query1
            try:
                response = requests.get(url(1), headers=headers)
                # Check response status code
                if response.status_code == 200:
                    with open(f'{symbol}.csv', 'wb') as f:
                        f.write(response.content)
                    minio_client.put_object(
                        bucket_name=bucket_name, obj_name=filename, obj_file=f'{symbol}.csv')
                    print(f'Successfully downloaded {symbol} to {filename}')
                    break
                elif response.status_code == 429:
                    time.sleep(delay)
            except Exception as e:
                # query1 failed
                try:
                    response = requests.get(url(2), headers=headers)
                    # Check response status code
                    if response.status_code == 200:
                        with open(f'{symbol}.csv', 'wb') as f:
                            f.write(response.content)
                        minio_client.put_object(
                            bucket_name=bucket_name, obj_name=filename, obj_file=f'{symbol}.csv')
                        print(
                            f'Successfully downloaded {symbol} to {filename}')
                        break
                    elif response.status_code == 429:
                        time.sleep(delay)
                    # query2 failed
                except Exception as e:
                    logging.info(
                        f"Failed to download {symbol} to {filename}. Error: {e}")
                    with open("dags/error_log_backup/error_log_weekly_data/error_log.txt", "a") as f:
                        f.write(f"{symbol}\n")
                    break
        # Pass retry limit
        else:
            logging.info(
                f"Exceeded maximum retries ({max_retries}). File {f'{symbol}.csv'} could not be downloaded.")
            with open("dags/error_log_backup/error_log_weekly_data/error_log.txt", "a") as f:
                f.write(f"{symbol}\n")


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
        try:
            start = time.time()
            minio_client.copy_object(bucket_name_from=bucket_name_1, bucket_name_to=bucket_name_2,
                                        obj_name=filename, prefix_2=f"backup_{datetime.today().strftime('%Y-%m-%d')}")
            logging.info(
                f"Backup file {filename} successfully. Runtime: {time.time() - start} seconds.")
        except Exception as e:
            logging.info(f"Failed to backup file {filename}. Error: {e}")
            continue


def daily_update(**kwargs):
    # # Arguments for the function
    bucket_name = kwargs.get("bucket_name")
    symbols = kwargs.get('symbols')

    # Minio client
    minio_client = MinioClient('minio', 9000, access_key, secret_key)

    # List symbols
    if symbols == None:
        symbols = [x.split(".")[0] for x in minio_client.list_objects(
            bucket_name=bucket_name)]

    # Retry strategy
    max_retries = 5

    midnight = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    a_day_ago = midnight - timedelta(days=1)
    current_day = int(midnight.timestamp())
    a_day_ago = int(a_day_ago.timestamp())

    for symbol in symbols:

        # Extract CSV content from bucket
        csv_content_raw = minio_client.get_object(
            bucket_name=bucket_name, obj_name=f'{symbol}.csv').data.decode('utf-8')

        # Header for requests
        headers = header_csv

        # Start reading the CSV content
        csv_reader = csv.reader(io.StringIO(csv_content_raw))
        temp_row_holder = [row for row in csv_reader]
        latest_row = temp_row_holder[-1]
        if latest_row[0] == (midnight - timedelta(days=1)).strftime("%Y-%m-%d"):
            continue

        for attempt in range(max_retries):

            # Extract the latest row from website
            url = lambda x: f"https://query{x}.finance.yahoo.com/v7/finance/download/{symbol}?period1={a_day_ago}&period2={current_day}&interval=1d&events=history&includeAdjustedClose=true"

            # Check query1
            try:
                response = requests.get(url(1), headers=headers)
                # Check response status code
                if response.status_code == 200:
                    latest_row_content = response.content.decode('utf-8').split("\n")[1].split(",")
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
                    break
            except Exception as e:
                conn.rollback()
                try:
                    response = requests.get(url(2), headers=headers)
                    if response.status_code == 200:
                        latest_row_content = response.content.decode('utf-8').split("\n")[1].split(",")
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
                        break
                    else:
                        print(f"Failed to update latest rows for {symbol}.csv")
                        break
                except Exception as e:
                    conn.rollback()
                    logging.info(
                        f"Exceeded maximum retries ({max_retries}). File {f'{symbol}.csv'} could not be downloaded.")
                    with open("dags/error_log_backup/error_log_daily_update/error_log.txt", "a") as f:
                        f.write(f"{symbol}\n")
                    continue
        else:
            logging.info(
                f"Exceeded maximum retries ({max_retries}). File {f'{symbol}.csv'} could not be downloaded.")
            with open("dags/error_log_backup/error_log_daily_update/error_log.txt", "a") as f:
                f.write(f"{symbol}\n")


def daily_update_staging(**kwargs):

    # Arguments for the function
    symbols = kwargs.get("symbols")
    bucket_name = kwargs.get("bucket_name")
    
    if symbols == None:
        symbols = [x.split(".")[0] for x in minio_client.list_objects(
            bucket_name=bucket_name)]

    midnight = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    a_day_ago = midnight - timedelta(days=1)
    current_day = int(midnight.timestamp())
    a_day_ago = int(a_day_ago.timestamp())

    # Retry strategy
    max_retries = 5
    delay = 60

    # Header for requests
    headers = header_csv

    # Minio Client
    minio_client = MinioClient('minio', 9000, access_key, secret_key)

    # Extract CSV content
    for symbol in symbols:

        # Filename
        filename = f'{symbol}.csv'

        # Retry logic attempt
        for attempt in range(max_retries):

            # URL for requests
            url = lambda x: f"https://query{x}.finance.yahoo.com/v7/finance/download/{symbol}?period1=942935400&period2={current_day}&interval=1d&events=history&includeAdjustedClose=true"

            # Check query1
            try:
                response = requests.get(url(1), headers=headers)
                # Check response status code
                if response.status_code == 200:
                    with open(filename, 'wb') as f:
                        f.write(response.content)
                    minio_client.put_object(
                        bucket_name=bucket_name, obj_name=filename, obj_file=filename)
                    logging.info(f'Successfully downloaded {symbol} to {filename}')
                    break
                elif response.status_code == 429:
                    time.sleep(delay)
            except Exception as e:
                # query1 failed
                # Check query2
                try:
                    response = requests.get(url(2), headers=headers)
                    # Check response status code
                    if response.status_code == 200:
                        with open(filename, 'wb') as f:
                            f.write(response.content)
                        minio_client.put_object(
                            bucket_name=bucket_name, obj_name=filename, obj_file=filename)
                        logging.info(
                            f'Successfully downloaded {symbol} to {filename}')
                        break
                    elif response.status_code == 429:
                        time.sleep(delay)
                    # query2 failed
                    else:
                        logging.info(
                            f"Exceeded maximum retries ({max_retries}). File {f'{symbol}.csv'} could not be downloaded.")
                        with open("dags/error_log_backup/error_log_daily_update/error_log_staging.txt", "a") as f:
                            f.write(f"{symbol}\n")
                        break
                except Exception as e:
                    logging.info(
                        f'Failed to download {symbol} to {filename}. Error: {e}')
                    continue
        # Pass retry limit
        else:
            logging.info(
                f"Exceeded maximum retries ({max_retries}). File {f'{symbol}.csv'} could not be downloaded.")
            with open("dags/error_log_backup/error_log_daily_update/error_log_staging.txt", "a") as f:
                f.write(f"{symbol}\n")


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

extract_symbol_dag >> weekly_data_backup_dag >> update_daily_dag >> backup_full_dag
