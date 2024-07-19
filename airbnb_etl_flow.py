# airbnb_etl_flow.py

from metaflow import FlowSpec, step

class AirbnbETLFlow(FlowSpec):

    @step
    def start(self):
        import pandas as pd
        self.df = pd.read_csv('AB_NYC_2019.csv')
        self.next(self.clean_data)

    @step
    def clean_data(self):
        self.df['reviews_per_month'].fillna(0, inplace=True)
        self.df['last_review'] = pd.to_datetime(self.df['last_review'], errors='coerce')
        self.df['last_review'].fillna(pd.to_datetime('1900-01-01'), inplace=True)
        self.next(self.load_data)

    @step
    def load_data(self):
        from sqlalchemy import create_engine
        db_user = 'your_db_user'
        db_password = 'your_db_password'
        db_host = 'your_db_host'
        db_port = 'your_db_port'
        db_name = 'your_db_name'
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        self.df.to_sql('airbnb_listings', engine, index=False, if_exists='replace')
        print("Data loaded successfully into PostgreSQL.")
        self.next(self.end)

    @step
    def end(self):
        print("ETL Flow is completed.")

if __name__ == '__main__':
    AirbnbETLFlow()
