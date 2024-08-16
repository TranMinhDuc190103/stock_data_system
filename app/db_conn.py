import psycopg2 
from dotenv import dotenv_values

class DbConnection():
    def __init__(self):
        pass
    
    def get_connection(self):
    # Config variables
        config = dotenv_values("dags/.env")

        # Connect to db
        host="localhost"
        username=config['POSTGRESQL_USER']
        password=config['POSTGRESQL_PASSWORD']
        database=config['POSTGRESQL_DB']
        port=config['PG_PORT']

        conn = psycopg2.connect(f"host={host} user={username} dbname={database} password={password} port={port}")
        return conn