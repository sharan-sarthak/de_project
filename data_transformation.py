# data_transformation.py

import pandas as pd

# Load the Airbnb dataset
df = pd.read_csv('AB_NYC_2019.csv')

# Fill missing values for 'reviews_per_month' with 0
df['reviews_per_month'].fillna(0, inplace=True)

# Convert 'last_review' to datetime format
df['last_review'] = pd.to_datetime(df['last_review'], errors='coerce')

# Handle missing values in 'last_review' by filling with a default date
df['last_review'].fillna(pd.to_datetime('1900-01-01'), inplace=True)

print(df.info())
