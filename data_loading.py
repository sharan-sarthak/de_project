# data_loading.py

import pandas as pd
from sqlalchemy import create_engine

# Load the transformed Airbnb dataset
df = pd.read_csv('AB_NYC_2019_transformed.csv')

# Database connection parameters
db_user = 'postgres'
db_password = 'admin'
db_host = 'localhost'
db_port = '5432'
db_name = 'local_db'

# Create a database engine
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Load data into PostgreSQL
df.to_sql('airbnb_listings', engine, index=False, if_exists='replace')
print("Data loaded successfully into PostgreSQL.")
