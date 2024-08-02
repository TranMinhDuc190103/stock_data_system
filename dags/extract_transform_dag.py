# Import airflow library
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow import DAG

# Import modules
import module.retry_logic as retry
from module.object_client import MinioClient

# Import libraries
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from dotenv import dotenv_values
import json
import time

# Load environment variables
config = dotenv_values("dags/.env")
access_key = config['MINIO_ROOT_USER']
secret_key = config['MINIO_ROOT_PASSWORD']

# Default arguments
default_args = {
    'owner': 'airflow',
    'email': ['scarletmoon2003@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id="extract_and_transform",
    start_date=datetime(year=2024,
                        month=7,
                        day=23),
    schedule_interval=timedelta(days=1),
    on_success_callback=None,
    on_failure_callback=retry.callback_on_failure,
    default_args=default_args
)

# Stock symbol list

def extract_symbol_list():
    # Arguments for the function
    minio_client = MinioClient("minio", "9000", access_key=access_key, secret_key=secret_key)
    list_objects = minio_client.list_objects(bucket_name="symbol")
    list_symbols = [x.split(".")[0] for x in list_objects]
    print(list_symbols[:1000])
    return list_symbols[:1000]
# Extract HTML content task

def extract_HTML_content(**kwargs):
    # Arguments for the function
    symbols = kwargs.get("symbols")
    bucket_name = kwargs.get("bucket_name")
    bucket_name_2 = kwargs.get("bucket_name_2")
    
    # print(symbols)
    minio_client = MinioClient("minio", "9000", access_key=access_key, secret_key=secret_key)
    for symbol in symbols:
        # Filename
        filename = f"yahoo_data_info/{symbol}.html"
        # if os.path.exists(filename):
        #     return
        list_objects = minio_client.list_objects(bucket_name=bucket_name, prefix="yahoo_data_info/")
        if filename in list_objects:
            return

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
            url, headers={"user-agent": "Mozilla/5.0", "from": "youremail@gmail.com"}, stream=True)

        # Parse HTML content using BS4
        soup = BeautifulSoup(r.text, "html.parser")

        # HTML content
        data = soup.find("article")

        # Check if HTML content is empty
        if data == None:
            with open(f'{symbol}.html', 'w', encoding='utf-8') as f:
                f.write(str(data))
                print(
                    f"File {filename} is empty. Put into the {bucket_name_2} bucket.")
            minio_client.put_object(bucket_name=bucket_name_2,
                                    obj_name=filename, obj_file=f'{symbol}.html')
        else:
            # Write into a HTML file
            with open(f'{symbol}.html', 'w', encoding='utf-8') as f:
                f.write(str(data.prettify()))
                print(
                    f"File {filename} written successfully. Put into the {bucket_name} bucket.")
            minio_client.put_object(bucket_name=bucket_name,
                                    obj_name=filename, obj_file=f'{symbol}.html')
        time.sleep(20)
    
    return symbols

# Extract data from HTML content task

def transform_data_from_HTML(**kwargs):
    
    symbols = kwargs.get("symbols")
    bucket_name_1 = kwargs.get("bucket_name_1")
    bucket_name_2 = kwargs.get("bucket_name_2")
    
    # print(symbols)
    minio_client = MinioClient("minio", "9000", access_key=access_key, secret_key=secret_key)
    for symbol in symbols:
        
        # Extract HTML content from landing zone
        html_content = minio_client.get_object(bucket_name=bucket_name_1, obj_name=f"yahoo_data_info/{symbol}.html", prefix="yahoo_data_info/").data.decode('utf-8')
        
        # Setup BS4
        soup = BeautifulSoup(html_content, "html.parser")
        
        # print(html_content)
        
        # Filename of object
        filename = f"yahoo_data_info/{symbol}.json"
        
        # Profile type variable
        type_text = soup.find("article").find("section", class_="yf-16025kh reverseColumn twothirds").find("section", class_="yf-1hj9jti").find("header").find("h3")
        type = type_text.text.replace('\n', '').replace(' ', '')
        
        # Data extraction holder
        result = dict()
        
        # Corpo stock
        if type == "KeyExecutives":
            description = soup.find("article").\
                find("section", "yf-16025kh reverseColumn twothirds").\
                find("section", "yf-1hj9jti").\
                find("div").text.replace('\n', ' ').replace('                 ', '').replace('             ', '')
            
            # Check if the profile is empty
            if description == None:
                result = {
                        "Sector": "NaN",
                        "Industry": "NaN",
                        "Description": "NaN"
                    }
                
            # Profile not empty, proceed to extract data from elements
            else:
                content = soup.find("article").\
                        find("section", "yf-16025kh reverseColumn twothirds").\
                        find_all("section")[1].find("dl").\
                        find_all("div")
                sector = content[0].find("dd").find("a").text.replace(' ', '').replace('\n', '')
                industry = content[1].find("a").text.replace(' ', '').replace('\n', '')
                
                # Data into a dictionary
                result = {
                    "Sector": sector,
                    "Industry": industry,
                    "Description": description
                }
                
        # ETF stock
        else:
            summary = soup.find("article").find("section", class_="yf-16025kh").find_all("section")[0].find("p").text.replace('\n    ', '')
            
            overview = soup.find("article").\
                find("section", class_="yf-16025kh").\
                find_all("section")[1].find("div").\
                find("table").find("tbody").find_all("tr")
                
            category = overview[0].find_all("td")[1].text.replace('\n       ', '')
            fund_family = overview[1].find_all("td")[1].text.replace('\n       ', '')
            net_assets = overview[2].find_all("td")[1].text.replace('\n       ', '')
            legal_type = overview[5].find_all("td")[1].text.replace('\n       ', '')
            
            result = {
                "Category": category,
                "Fund Family": fund_family,
                "Net Assets": net_assets,
                "Legal Type": legal_type,
                "Summary": summary
            }
        
        # Write data into a JSON file into MinIO staging zone
        with open(f'{symbol}.json', 'w') as f:
            json.dump(result, f, indent=4, ensure_ascii=False)
        minio_client.put_object(bucket_name=bucket_name_2, obj_name=filename, obj_file=f'{symbol}.json')
    
# Task definition

extract_symbol_dag = PythonOperator(
    task_id="extract_symbol",
    python_callable=extract_symbol_list,
    dag=dag
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
        "symbols": extract_HTML_dag.output,
        "bucket_name_1": "bronze",
        "bucket_name_2": "silver"
    },
    dag=dag
)

# Task pipeline
extract_symbol_dag >> extract_HTML_dag >> transform_HTML_dag