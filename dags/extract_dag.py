# Import airflow library
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow import DAG

# Import modules
import module.retry_logic as retry
from module.object_client import MinioClient

# Import libraries
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import dotenv_values
import json
import time
from lxml import html
from sqlalchemy import create_engine
import psycopg2
import csv
import pandas as pd
from datetime import datetime, timezone

# Load environment variables
config = dotenv_values("dags/.env")
api_key = config['API_KEY']
access_key = config['MINIO_ROOT_USER']
secret_key = config['MINIO_ROOT_PASSWORD']

# Setup database connections
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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id="extract_dag",
    start_date=datetime(year=2024,
                        month=7,
                        day=23),
    schedule_interval=timedelta(days=1),
    on_success_callback=None,
    on_failure_callback=retry.callback_on_failure,
    default_args=default_args
)

# Stock symbol list

midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
current_day = int(midnight.timestamp())

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
        
# Extract HTML content task

def extract_csv_content(**kwargs):
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
    
    # Extract CSV content 
    for symbol in symbols:
        
        # Filename 
        filename = f'{symbol}.csv'
        
        # Retry logic attempt
        for attempt in range(max_retries):
            
            # URL for requests
            url = lambda x: f"https://query{x}.finance.yahoo.com/v7/finance/download/{symbol}?period1=942935400&period2={current_day}&interval=1d&events=history&includeAdjustedClose=true"
            
            # Check query1
            response = requests.get(url(1), headers=headers)
            
            # Check response status code
            if response.status_code == 200:
                with open(filename, 'wb') as f:
                    f.write(response.content)
                minio_client.put_object(bucket_name=bucket_name, obj_name=filename, obj_file=filename)
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
                    with open(filename, 'wb') as f:
                        f.write(response.content)
                    minio_client.put_object(bucket_name=bucket_name, obj_name=filename, obj_file=filename)
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
        

def extract_HTML_content(**kwargs):
    # Arguments for the function
    symbols = kwargs.get("symbols")
    bucket_name = kwargs.get("bucket_name")
    bucket_name_2 = kwargs.get("bucket_name_2")
    
    # print(symbols)
    
    # Minio Client
    minio_client = MinioClient("minio", "9000", access_key=access_key, secret_key=secret_key)
    
    # Existing objects in the bucket
    list_objects = minio_client.list_objects(bucket_name=bucket_name, prefix="yahoo_data_info/")
    
    for symbol in symbols:
        # Filename
        filename = f"yahoo_data_info/{symbol}.html"
        # if os.path.exists(filename):
        #     return
        
        # Check if existing objects
        if filename in list_objects:
            continue

        # URL
        url = f"https://finance.yahoo.com/quote/{symbol}/profile"

        # Retry strategy
        retries = Retry(total=10, backoff_factor=0.1)
        adapter = HTTPAdapter(max_retries=retries)

        # Session
        session = requests.Session()
        session.mount('http://', adapter=adapter)
        session.mount('https://', adapter=adapter)

        # Requests
        r = session.get(
            url, headers={"user-agent": "Mozilla/5.0", "from": "youremail@gmail.com"}, stream=True, allow_redirects=False)

        # Parse HTML content using BS4
        tree = html.fromstring(r.content)

        # HTML content
        data = tree.xpath('//*[@id="nimbus-app"]/section/section/section/article')

        # Check if HTML content is empty
        
        if data == []:
            with open(f'{symbol}.html', 'w', encoding='utf-8') as f:
                f.write("None")
                print(f"File {filename} is empty. Put into the {bucket_name_2} bucket.")
            minio_client.put_object(bucket_name=bucket_name_2,
                                    obj_name=filename, obj_file=f'{symbol}.html')
        else:
            html_content = str(html.tostring(data[0], pretty_print=True)).replace("b\'", "").replace(" \\n\'", "")
            # Write into a HTML file
            with open(f'{symbol}.html', 'w', encoding='utf-8') as f:
                f.write(html_content)
                print(f"File {filename} written successfully. Put into the {bucket_name} bucket.")
            minio_client.put_object(bucket_name=bucket_name,
                                    obj_name=filename, obj_file=f'{symbol}.html')
        time.sleep(10)

# Extract data from HTML content task

def transform_data_from_HTML(**kwargs):
    
    bucket_name_1 = kwargs.get("bucket_name_1")
    bucket_name_2 = kwargs.get("bucket_name_2")
    
    # print(symbols)
    minio_client = MinioClient("minio", "9000", access_key=access_key, secret_key=secret_key)
    symbols = extract_symbol_list()
    list_objects = minio_client.list_objects(bucket_name=bucket_name_2, prefix="yahoo_data_info/")
    for symbol in symbols:
        
        # Filename
        filename = f"yahoo_data_info/{symbol}.json"
        if filename in list_objects:
            continue
        
        # Extract HTML content from landing zone
        try:
            html_content = minio_client.get_object(bucket_name=bucket_name_1, obj_name=f"yahoo_data_info/{symbol}.html", prefix="yahoo_data_info/").data.decode('utf-8')
        except Exception as e:
            filename_html = f"yahoo_data_info/{symbol}.html"
            if filename_html not in minio_client.list_objects(bucket_name=bucket_name_1, prefix="yahoo_data_info/"):
                continue
            minio_client.delete_objects(bucket_name=bucket_name_1, obj_name_list=[f"yahoo_data_info/{symbol}.html"], prefix="yahoo_data_info/")
            continue
        
        # Parse HTML content
        tree = html.fromstring(html_content)
        
        # Extract element using XPATH
        # Type of stock: KeyExecutives or ETFSummary
        try:
            type = tree.xpath("//article/section[2]/section[1]/header[1]/h3[1]/text()")[0].replace('\n', '').replace(' ', '')
        except Exception as e:
            minio_client.delete_objects(bucket_name=bucket_name_1, obj_name_list=[f"yahoo_data_info/{symbol}.html"], prefix="yahoo_data_info/")
            continue

        # Info of stock as a dictionary
        result = dict()
        
        # Company stock
        if type == 'KeyExecutives':
            result['Type'] = 'Corpo stock'
            description = tree.xpath("//article/section[2]/section[1]/div[1]/text()")
            # Description check
            if len(description) == 3:
                result['Sector'] = None
                result['Industry'] = None
                result['Description'] = None
                
            else:
                sector = tree.xpath("//article/section[2]/section[2]/div[1]/dl[1]/div[1]/dd[1]/a/text()")
                if len(sector) == 0:
                    result['Sector'] = None
                else:
                    result['Sector'] = sector[0].replace('\n   ', '')
                industry = tree.xpath("//article/section[2]/section[2]/div[1]/dl[1]/div[2]/a/text()")
                if len(industry) == 0:
                    result['Industry'] = None
                else:
                    result['Industry'] = industry[0].replace('\n   ', '')
                result['Description'] = description[0].replace('\n   ', '')
        
        # ETF stock    
        elif type == 'ETFSummary':
            result['Type'] = 'ETF stock'
            description = tree.xpath("//article/section[2]/section[1]/p/text()")
            if len(description) == 0:
                result['Category'] = None
                result['Fund Family'] = None
                result['Net Assets'] = None
                result['Legal Type'] = None
                result['Description'] = None
            else:
                result['Category'] = tree.xpath('//article/section[2]/section[2]/div/table/tbody/tr[1]/td[2]/text()')[0].replace('\n   ', '')
                result['Fund Family'] = tree.xpath('//article/section[2]/section[2]/div/table/tbody/tr[2]/td[2]/text()')[0].replace('\n   ', '')
                result['Net Assets'] = tree.xpath('//article/section[2]/section[2]/div/table/tbody/tr[3]/td[2]/text()')[0].replace('\n   ', '')
                result['Legal Type'] = tree.xpath('//article/section[2]/section[2]/div/table/tbody/tr[6]/td[2]/text()')[0].replace('\n   ', '')
                result['Description'] = description[0].replace('\n   ', '')
        with open(f'{symbol}.json', 'w') as f:
            json.dump(result, f, indent=4, ensure_ascii=False)
        minio_client.put_object(bucket_name=bucket_name_2, obj_name=filename, obj_file=f'{symbol}.json')
        print(f"File {filename} written successfully. Put into the {bucket_name_2} bucket.")

    
def load_data(**kwargs):
    bucket_name_1 = kwargs.get("bucket_name_1")
    bucket_name_2 = kwargs.get("bucket_name_2")
    max_retries = kwargs.get("max_retries")
    
    # Minio client
    minio_client = MinioClient('minio', 9000, access_key, secret_key)
    
    # List symbols
    list_objects = minio_client.list_objects(bucket_name_1, prefix='yahoo_data_info/')
    list_symbols = [x.split("/")[1].split('.')[0] for x in list_objects]
    
    # Check through every symbol
    for symbol in list_symbols:
        
        # Start time
        start_time = time.time()
        
        # ID
        
        # Check if record exists within the database
        cursor.execute(f'''
                        SELECT * 
                        FROM temp_dim_hist_data
                        WHERE "Symbol" = '{symbol}';
                        ''')
        result = cursor.fetchall()
        if len(result) > 0:
            continue
        
        # Filename to extract data
        filename_json = f"yahoo_data_info/{symbol}.json"
        filename_csv = f"{symbol}.csv"

        # Iterate through attempt, max_retries = 5
        retries = 0
        while retries < max_retries:
            try:
                json_content = minio_client.get_object(bucket_name=bucket_name_1, obj_name=filename_json, prefix="yahoo_data_info/").json()
                # Stock info
                info_id = f'INFO_{symbol}'
                type = json_content['Type']
                # Categorize according to type
                if type == 'Corpo stock':
                    sector = json_content['Sector']
                    industry = json_content['Industry']
                    cursor.execute(f"""
                                    INSERT INTO dim_info_stock (info_id, sector, industry)
                                    VALUES ('{info_id}', '{sector}', '{industry}')
                                    ON CONFLICT (info_id)
                                    DO UPDATE SET sector = '{sector}', industry = '{industry}';
                                    """)
                elif type == 'ETF stock':
                    category = json_content['Category']
                    fund_family = json_content['Fund Family']
                    net_assets = json_content['Net Assets']
                    legal_type = json_content['Legal Type']
                    cursor.execute(f"""
                                    INSERT INTO dim_info_stock (info_id, category, fund_family, net_assets, legal_type)
                                    VALUES ('{info_id}', '{category}', '{fund_family}', '{net_assets}', '{legal_type}')
                                    ON CONFLICT (info_id)
                                    DO UPDATE SET category = '{category}', fund_family = '{fund_family}', net_assets = '{net_assets}', legal_type = '{legal_type}';
                                    """)

                description = json_content['Description']
                status = None
                if description is None:
                    status = "Disabled"
                    cursor.execute(f"""
                                INSERT INTO fact_stock_data (symbol, info_id, type,description, status)
                                VALUES ('{symbol}',  '{info_id}', '{type}', '{description}', '{status}')
                                ON CONFLICT (symbol)
                                DO UPDATE SET type = '{type}', description = '{description}', status = '{status}';
                                """)
                else:
                    status = "Active"
                    cursor.execute(f"""
                                INSERT INTO fact_stock_data (symbol, info_id, type,description, status)
                                VALUES ('{symbol}',  '{info_id}', '{type}', '{description.replace("'", '"')}', '{status}')
                                ON CONFLICT (symbol)
                                DO UPDATE SET type = '{type}', description = '{description.replace("'", '"')}', status = '{status}';
                                """)
                
                # Historical data
                csv_content = minio_client.get_object(bucket_name=bucket_name_2, obj_name=filename_csv)
                df = pd.read_csv(csv_content, index_col=0)
                df["Symbol"] = symbol
                df.to_sql('temp_dim_hist_data', conn_1, if_exists='append', index=True, method="multi", chunksize=1000)
                print(f"Runtime: {time.time() - start_time} seconds")
            except psycopg2.OperationalError as oe:
                retries += 1
                print(f"Operation failed. Error: {oe}. Retry again.")
            except Exception as e:
                print(e)
    
    cursor.execute(
        """
        INSERT into dim_hist_data
        SELECT * FROM temp_dim_hist_data
        ON CONFLICT ("Symbol", "Date")
        DO UPDATE SET
        "Open" = temp_dim_hist_data."Open",
        "High" = temp_dim_hist_data."High",
        "Low" = temp_dim_hist_data."Low",
        "Close" = temp_dim_hist_data."Close",
        "Adj Close" = temp_dim_hist_data."Adj Close",
        "Volume" = temp_dim_hist_data."Volume"
        """
    )
    conn_1.close()
    conn.close()
    

# Task definition

extract_symbol_dag = PythonOperator(
    task_id="extract_symbol",
    python_callable=extract_symbol_list,
    dag=dag
)

extract_csv_dag = PythonOperator(
    task_id='extract_csv_dag',
    python_callable=extract_csv_content,
    op_kwargs={
        "symbols": extract_symbol_dag.output,
        "bucket_name": "hist-data"
    }
)

extract_HTML_dag = PythonOperator(
    task_id="extract_HTML_content",
    python_callable=extract_HTML_content,
    op_kwargs={
        "symbols": extract_symbol_dag.output,
        "bucket_name": "bronze",
        "bucket_name_2": "trashbin"   
    },
    dag=dag
)

transform_HTML_dag = PythonOperator(
    task_id="transform_HTML_content",
    python_callable=transform_data_from_HTML,
    op_kwargs={
        "bucket_name_1": "bronze",
        "bucket_name_2": "silver"
    },
    dag=dag
)

load_data_dag = PythonOperator(
    task_id="load_data_dag",
    python_callable=load_data,
    op_kwargs={
        "bucket_name_1": "silver",
        "bucket_name_2": "hist-data",
        "max_retries": 5
    },
    dag=dag
)

# Task pipeline
extract_symbol_dag >> [extract_HTML_dag, extract_csv_dag] >> transform_HTML_dag >> load_data_dag
