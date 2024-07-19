# data_ingestion.py

import pandas as pd

# Load the Airbnb dataset
df = pd.read_csv('AB_NYC_2019.csv')
print(df.head())
