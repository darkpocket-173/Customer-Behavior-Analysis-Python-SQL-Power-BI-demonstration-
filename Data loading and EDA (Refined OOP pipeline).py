import pandas as pd
from sqlalchemy import create_engine


class CustomerDataPipeline:
    def __init__(self, csv_path):
        self.df = None
    
    def load_data(self):
        self.df = pd.read_csv(self.csv_path)
        
    def fill_missing_review_ratings(self):
        self.df['Review Rating'] = (
            self.df
            .groupby('Category')['Review Rating']
            .transform(lambda x: x.fillna(x.median()))
            )
        
    def standardize_columns(self):
        self.df.columns = (
            self.df.columns
            .str.lower()
            .str.replace(' ', '_')
        )            
        self.df = self.df.rename(
            columns = {'purchase_amount_(usd)' : 'purchase_amount'}
        )
        
    def create_age_group(self):
        labels = ['Young Adult', 'Adult', 'Middle-Aged', 'Senior']
        self.df['age_group'] = pd.qcut(self.df['age'], q=4, labels = labels)
        
    def convert_purchase_frequency(self):
        frequency_mapping = {
            'Fortnightly' : 14,
            'Weekly' : 7,
            'Monthly' : 30,
            'Quarterly' : 90,
            'Bi-Weekly' : 14,
            'Annually' : 365,
            'Every 3 Months' : 90
        }
        self.df['purchase_frequency_days'] = (
            self.df['frequency_of_purchases'].map(frequency_mapping)
        )
        
    def drop_redundant_columns(self):
        self.df = self.df.drop('promo_code_used', axis = 1)
        
    def load_to_postgres(self, username, password, host, port, database, table_name):
        engine = create_engine(
            f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
        )
        self.df.to_sql(table_name, engine, if_exists = 'replace', index = False)
        
        
        
# Running Pipeline
pipeline = CustomerDataPipeline("customer_shopping_behavior.csv")
pipeline.load_data()
pipeline.fill_missing_review_ratings()
pipeline.standardize_columns()
pipeline.create_age_group()
pipeline.convert_purchase_frequency()
pipeline.drop_redundant_columns()
pipeline.load_to_postgres(
    username = "postgres",
    password = "2474",
    host = "localhost", 
    port = "5432",
    database = "Customer Behavior OOP", 
    table_name = "customer"
)       
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        