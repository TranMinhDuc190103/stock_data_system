import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="userdb",
    user="username",
    password="password",
    port=5439
)

cursor = conn.cursor()

# Fact table queries

fact_table_sql = '''
    CREATE TABLE IF NOT EXISTS fact_stock_data(
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(30) NOT NULL UNIQUE,
        info_id VARCHAR(30) NOT NULL,
        type VARCHAR(30) NOT NULL,
        description VARCHAR(1000),
        status VARCHAR(30)
    )
'''

# Dim tables

# dim_info_corpo_sql = '''
#     CREATE TABLE IF NOT EXISTS dim_info_corpo(
#         info_id VARCHAR(30) NOT NULL PRIMARY KEY,
#         sector VARCHAR(50),
#         industry VARCHAR(50)
#     );
# '''

dim_info_stock_sql = '''
    CREATE TABLE IF NOT EXISTS dim_info_stock(
        info_id VARCHAR(20) NOT NULL PRIMARY KEY,
        category VARCHAR(50),
        fund_family VARCHAR(100),
        net_assets VARCHAR(10),
        legal_type VARCHAR(50),
        sector VARCHAR(50),
        industry VARCHAR(50)
    );
'''

dim_hist_data_sql = '''
    CREATE TABLE IF NOT EXISTS dim_hist_data(
        record_id SERIAL,
        "Date" DATE NOT NULL,
        "Open" DECIMAL(8,1),
        "High" DECIMAL(8,1),
        "Low" DECIMAL(8,1),
        "Close" DECIMAL(8,1),
        "Adj Close" DECIMAL(8,1),
        "Volume" DECIMAL(20,0),
        "Symbol" VARCHAR(30) NOT NULL,
        PRIMARY KEY ("Symbol", "Date")
    );
'''

fk_setup_sql = '''
    ALTER TABLE dim_hist_data ADD CONSTRAINT fk_fact_stock_data FOREIGN KEY ("Symbol") REFERENCES fact_stock_data(symbol);
    ALTER TABLE fact_stock_data ADD CONSTRAINT fk_dim_info_stock FOREIGN KEY (info_id) REFERENCES dim_info_stock(info_id);
'''

cursor.execute(fact_table_sql)
cursor.execute(dim_info_stock_sql)
cursor.execute(dim_hist_data_sql)
cursor.execute(fk_setup_sql)

index_sql = """
    CREATE UNIQUE INDEX index_stock_symbol on fact_stock_data(symbol);
    CREATE UNIQUE INDEX index_stock_info_id on fact_stock_data(info_id);  
"""

cursor.execute(index_sql)
conn.commit()