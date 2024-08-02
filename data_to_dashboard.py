from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import psycopg2

# Connection to PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="userdb",
    user="username",
    password="password",
    port=5439,
)



# Query to get the data
sql = '''
    SELECT * FROM public."IBM";
'''
# Cursor
cursor = conn.cursor()

# Execute the query
cursor.execute(sql)

# Query result
data = cursor.fetchall()

# Convert data to pandas dataframe
df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])