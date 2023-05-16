import yaml
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
from sqlalchemy import inspect
import datetime as dt
import numpy as np
# import module
from geopy.geocoders import Nominatim
from geopy.point import Point
import re
# uuid time
import uuid
import time_uuid
import tabula
# API
import requests
import sys
import pprint
import json
# s3
import boto3
import datetime

class DatabaseConnector():
    
    def __init__(self,filename):
        self.filename = filename
        self.cred = self.read_db_creds()
        self.engine = self.init_db_engine()

    def read_db_creds(self):
       with open(self.filename,'r') as file:
            service_prime = yaml.safe_load(file)
            return service_prime
    
    def read_upload_to_db(self):
       with open(self.filename,'r') as file:
            service_prime_1 = yaml.safe_load(file)
            return service_prime_1

    def init_db_engine(self):
        service_prime = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        USER = service_prime['RDS_USER']
        PASSWORD = service_prime['RDS_PASSWORD']
        HOST = service_prime['RDS_HOST']
        PORT = service_prime['RDS_PORT']
        DATABASE = service_prime['RDS_DATABASE']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine.connect
        return engine

    def upload_to_db(self):
        service_prime_1 = self.read_upload_to_db()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = service_prime_1['RDS_HOST']
        USER = service_prime_1['RDS_USER']
        PASSWORD = service_prime_1['RDS_PASSWORD']
        DATABASE = service_prime_1['RDS_DATABASE']
        PORT = 5432
        engine_Sales_Data = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine_Sales_Data.connect()

        legacy_users.to_sql('dim_user', engine_Sales_Data, if_exists='replace')
        pdf_data.to_sql('dim_card_details', engine_Sales_Data, if_exists='replace')
        store_data.to_sql('dim_store_details', engine_Sales_Data, if_exists='replace')
        product.to_sql('dim_products', engine_Sales_Data, if_exists='replace')
        order.to_sql('orders_table', engine_Sales_Data, if_exists='replace')
        date.to_sql('dim_date_times', engine_Sales_Data, if_exists='replace')


    
class DataExtractor(DatabaseConnector):

    def read_upload_to_db(self):
       with open(self.filename,'r') as file:
            service_prime_2 = yaml.safe_load(file)
            return service_prime_2

    def __init__(self,filename):
        super().__init__(filename)
        self.list_table = self.list_db_tables()
        self.read_table = self.read_rds_table()
        self.pdf_data = self.retrieve_pdf_data()
        self.list_store = self.list_number_of_stores()
        self.store_data = self.retrieve_stores_data()
        self.s3 = self.extract_from_s3()
        self.date = self.extract_json_from_s3()

    def list_db_tables(self):
        inspector = inspect(self.engine)
        list_table = inspector.get_table_names()
        return list_table
    
    def read_rds_table(self):
        # legacy_store_details = pd.read_sql_table('legacy_store_details', self.engine)
        self.legacy_users = pd.read_sql_table('legacy_users', self.engine)
        self.orders_table = pd.read_sql_table('orders_table', self.engine)
        return self.legacy_users, self.orders_table
    
    def retrieve_pdf_data(self):
        # dfs = tabula.read_pdf("card_details.pdf", pages='all')
        self.pdf_data = tabula.convert_into("card_details.pdf", "card_details.csv", output_format="csv", pages='all')
        self.pdf_data = pd.read_csv('card_details.csv')
        return self.pdf_data

    def list_number_of_stores(self):
        service_prime_2 = self.read_upload_to_db()
        self.headers = {service_prime_2['key']:service_prime_2['value']}
        n_s = requests.get(service_prime_2['adress'], headers= self.headers)
        return n_s.text

    def retrieve_stores_data(self):
        service_prime_2 = self.read_upload_to_db()
        self.base_url = service_prime_2['url']
        self.response_list = []
        self.df_list = []

        for i in range(451):
            self.url = self.base_url + str(i)
            self.response = requests.get(self.url, headers=self.headers)
            self.json_data = json.loads(self.response.content)
            self.temp_df = pd.json_normalize(self.json_data)
            self.df_list.append(self.temp_df)
            self.df = pd.concat(self.df_list,ignore_index=True)
        return self.df
    
    def extract_from_s3(self):
        self.s3_resource = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
        self.response = self.s3_client.download_file('data-handling-public','products.csv', 'products.csv')
        self.products_df = pd.read_csv('products.csv')
        return self.products_df
    
    def extract_json_from_s3(self):
        self.response_2 = self.s3_client.download_file('data-handling-public','date_details.json', 'date_details.json')
        self.date_details = pd.read_json('date_details.json')
        return self.date_details


conn = DatabaseConnector(filename = 'db_creds.yaml')


# ex = DataExtractor(filename = 'db_creds.yaml')
# ex.retrieve_pdf_data()
# ex.pdf_data
# ex.list_table
# ex.read_rds_table()
# ex.retrieve_stores_data()
# ex.df.head()
# ex.df.loc[0,'address'] = 0
# ex.df.loc[0,'latitude'] = 0
# ex.df.loc[0,'longitude'] = 0
# ex.df.loc[0,'locality'] = 'web'
# ex.df
# ex.df.isnull().sum()
# ex.df['store_type'].value_counts()
# pd.set_option('display.max_rows',1000)
# pd.set_option('display.max_column',1000)
# ex.df['store_type'].value_counts()
# ex.products_df["EAN"]
# ex.orders_table.drop(labels=['first_name','last_name','1'], axis= 1)
# ex.date_details['month'].value_counts()
# ex.date_details['timestamp'].info()
# ex.date_details['year'] + ex.date_details['month'] + ex.date_details['day'] + ex.date_details['timestamp'] 
# ex.date_details['timestamp']

class DataCleaning(DataExtractor):

    def __init__(self, filename):
        super().__init__(filename)

    def clean_user_data(self):
        # name
        self.legacy_users['first_name'] = self.legacy_users['first_name'].str.title()
        self.legacy_users['last_name'] = self.legacy_users['last_name'].str.title()
        # date_of_birth
        self.legacy_users['date_of_birth'] = pd.to_datetime(self.legacy_users['date_of_birth'], errors= 'coerce')
        n_data = self.legacy_users['date_of_birth'].isnull()
        self.legacy_users = self.legacy_users.loc[~n_data]
        # company
        self.legacy_users['company'].isnull().sum()
        self.legacy_users['company'] = self.legacy_users['company'].str.replace('KGaA', 'KG',regex=True).str.replace('Co.', 'Co',regex=True).str.replace('e.V.', 'e.V',regex=True).astype('category')
        du_com = self.legacy_users['company'].duplicated()
        self.legacy_users = self.legacy_users.loc[~du_com]
        self.legacy_users['company'].str.title()
        # email
        p = re.compile('^[a-zA-Z0-9+-_.]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')
        emails = []
        for index, email in enumerate(self.legacy_users['email_address']):
            ed = p.match(email)
            emails.append(ed.group())
            ({index : ed.group()})
        self.legacy_users['email_address'] = emails
        # address
        self.legacy_users['address'] = self.legacy_users['address'].str.replace('\n',',',regex=True)
        # country_code
        self.legacy_users['country_code'] = self.legacy_users['country_code'].str.replace('GGB','GB',regex=True)
        self.legacy_users['country_code'] = self.legacy_users['country_code'].astype('category')
        # phone_number
        self.legacy_users['phone_number'] = self.legacy_users['phone_number'].str.replace('\+\d{1,2}','',regex=True)\
            .str.replace('[()]','',regex=True).str.replace('[^0-9]','')\
                .str.replace('^[0][0]','',regex=True).str.replace('^[0]','',regex=True).astype('str')
        self.legacy_users['phone_number'] = '00' + self.legacy_users['phone_number'].str.zfill(2)

        # join_date
        self.legacy_users['join_date'] = pd.to_datetime(self.legacy_users['join_date'], errors= 'coerce')
        null_jd = self.legacy_users['join_date'].isnull()
        self.legacy_users = self.legacy_users.loc[~null_jd]
        legacy_users = self.legacy_users
        return legacy_users

    def clean_card_data(self):
        # card_number
        self.pdf_data.dropna(subset='card_number', inplace=True)
        self.pdf_data['card_number'] = self.pdf_data['card_number'].str.replace('?','',regex=True)
        self.pdf_data = self.pdf_data[self.pdf_data.card_number.astype('str').str.isnumeric()]
        self.pdf_data['card_number'] = self.pdf_data['card_number'].astype('int')
        # expiry_date
        self.pdf_data['expiry_date'] = pd.to_datetime(self.pdf_data['expiry_date'], errors= 'coerce')
        self.pdf_data.dropna(subset = 'expiry_date', inplace = True) 
        # card_provider
        self.pdf_data['date_payment_confirmed'].value_counts()
        self.pdf_data['date_payment_confirmed'] = pd.to_datetime(self.pdf_data['date_payment_confirmed'], errors= 'coerce')
        # index
        self.pdf_data = self.pdf_data.reset_index()
        self.pdf_data = self.pdf_data.drop('index', axis=1)
        pdf_data = self.pdf_data
        return pdf_data

        # called_clean_store_data
    def called_clean_store_data(self):
        self.store_data.info()
        self.store_data.loc[0,'address'] = 'web'
        self.store_data.loc[0,'longitude'] = 0
        self.store_data.loc[0,'latitude'] = 0
        self.store_data.loc[0,'locality'] = 'Web'
        self.store_data.isnull().sum()
        self.store_data.drop('lat', axis= 1, inplace= True)
        self.store_data.dropna(inplace=True)
        # longitude and latitude
        self.store_data[['longitude','latitude']] = self.store_data[['latitude','longitude']]
        self.store_data['longitude'] = pd.to_numeric(self.store_data['longitude'], errors= 'coerce')
        self.store_data['latitude'] = pd.to_numeric(self.store_data['latitude'], errors= 'coerce')
        self.store_data['longitude'].dropna(inplace=True)
        long_null = self.store_data['longitude'].isnull()
        self.store_data = self.store_data.loc[~long_null]
        # address_new
        geolocator = Nominatim(user_agent="geoapiExercises")
        def address(lat, lon):
            location = geolocator.reverse(Point(lat,lon))
            return location.raw['display_name']
        self.store_data['new_address'] = np.vectorize(address)(self.store_data['latitude'],self.store_data['longitude'])
        self.store_data.drop('address', axis=1, inplace= True)
        self.store_data.rename(columns={'new_address':'address'}, inplace= True)
        self.store_data['address'] = self.store_data['address'].astype('category')
        # country_code
        self.store_data['country_code'].value_counts()
        self.store_data['country_code'] = self.store_data['country_code'].astype('category')
        # continent
        self.store_data['continent'].value_counts()
        self.store_data['continent'] = self.store_data['continent'].str.strip('ee')
        self.store_data['continent']= self.store_data['continent'].astype('category')
        # locality
        self.store_data['locality'] =  self.store_data['locality'].str.title()
        self.store_data['locality']= self.store_data['locality'].astype('category')
        # store_type
        self.store_data['store_type']= self.store_data['store_type'].astype('category')
        # staff_numbers
        self.store_data['staff_numbers'].unique()
        self.store_data['staff_numbers'] = self.store_data['staff_numbers'].str.replace('n','').str.strip('JeRA')
        self.store_data['staff_numbers'] = self.store_data['staff_numbers'].astype('int')
        # opening_date
        self.store_data['opening_date'] = pd.to_datetime(self.store_data['opening_date'],errors= 'coerce')
        return self.store_data

    def convert_product_weights(self,unit_str):
        self.s3.dropna(inplace=True)
        # Define conversion factors for different units
        conversion_factors = {
        'kg': 1,
        'g': 0.001,
        'oz': 0.0283495,
        'ml': 0.001,
        }

    # Extract numerical value and unit from input string
        match = re.match(r'([\d\.]+)\s*(x|\*)?\s*([\d\.]+)?\s*([\w\.]+)', unit_str,)
        if match:
            num1 = float(match.group(1))
            num2 = float(match.group(3)) if match.group(3) else 1.0
            unit = match.group(4).lower()
        else:
            return None

    # Compute product of numerical values and convert unit to kilograms using conversion factors
        if unit in conversion_factors:
            return num1 * num2 * conversion_factors[unit]
        else:
            return None
    
    def add_new_weight(self):
        self.s3['weight_kg'] = self.s3['weight'].astype(str).apply(self.convert_product_weights)
        weight_kg_null = self.s3['weight_kg'].isnull()
        self.s3 = self.s3.loc[~weight_kg_null]
        self.s3['weight_kg'] = round(self.s3['weight_kg'],2)
        self.s3.drop('weight', axis=1, inplace=True)
         
    def clean_products_data(self):
        self.s3.drop('Unnamed: 0', axis= 1, inplace=True)
        # product_name
        self.s3['product_name'] = self.s3['product_name'].str.title()
        procheck = self.s3['product_name'].duplicated()
        self.s3 = self.s3.loc[~procheck]
        p_name_null = self.s3['product_name'].isnull()
        self.s3 = self.s3.loc[~p_name_null]

        # product_price
        self.s3['product_price'] = self.s3['product_price'].str.strip('£')
        self.s3['product_price'] = self.s3['product_price'].astype(float)

        # category
        self.s3['category'] = self.s3['category'].astype('category')

        # date_added
        self.s3['date_added'] = pd.to_datetime(self.s3['date_added'], errors= 'coerce')
        # removed
        self.s3['removed'] = self.s3['removed'].map({'Still_avaliable':True, 'Removed':False })
        return self.s3

    def clean_orders_data(self):
        self.orders_table.drop(labels=['first_name','last_name','1','level_0'], axis= 1,inplace=True)
        self.orders_table
        return self.orders_table
    
    def clean_date_details(self):
        # month
        self.date_details['month'] = self.date_details['month'].str.zfill(2)
        month = self.date_details['month'].isin(['09', '02', '04', '11', '12', '08', '01', '03', '07', '10', '05','06'])
        self.date_details = self.date_details.loc[month]
        # year
        self.date_details['year']
        # day
        self.date_details['day'] = self.date_details['day'].str.zfill(2)
        # calendar
        self.date_details['date'] = pd.to_datetime(self.date_details['year'] + \
                                                   self.date_details['month'] + \
                                                    self.date_details['day'] + \
                                                        self.date_details['timestamp'].str.replace(':',''))
        # day_name
        self.date_details['day_name'] = self.date_details['date'].dt.day_name()
        # time_period
        self.date_details['time_period'] = self.date_details['time_period'].astype('category')
        self.date_details
        return self.date_details



import sys
sys.setrecursionlimit(15000)

# sample = DataCleaning(filename = 'db_creds.yaml')
# sample.clean_user_data()
# legacy_users= sample.legacy_users
# legacy_users.info()
# pdf_data = sample.pdf_data
# pdf_data.info()
# sample.called_clean_store_data()
# sample.store_data.head()
# sample.store_data['opening_date']

# store_data = sample.store_data
# store_data['opening_date'] = pd.to_datetime(store_data['opening_date'],errors= 'coerce')
# store_data.info()
# sample.s3['product_name'].drop_duplicates(inplace=True)
# sample.add_new_weight()
# sample.clean_products_data()
# sample.s3.shape
# sample.s3['product_name'].drop_duplicates(inplace=True)
# sample.s3['product_name'].duplicated().sum()
# sample.s3.drop('Unnamed: 0', axis= 1, inplace=True)
# procheck = sample.s3['product_name'].duplicated()
# sample.s3 = sample.s3.loc[~procheck]
# product = sample.s3
# product['product_code'].duplicated().sum()
# sample.s3['removed'] = sample.s3['removed'].map({'Still_avaliable':True, 'Removed':False })
# sample.s3.reset_index().drop('index', axis= 1, inplace=True)
# check = (sample.s3['product_code']=='Q1-1813216e')
# sample.s3.loc[check]

# sample.clean_orders_data()
# sample.orders_table['product_quantity'].value_counts()
# sample.orders_table.info()
# sample.orders_table['date_uuid'].duplicated().sum()
# order = sample.orders_table
# sample.clean_date_details()
# sample.date_details.info()
# date = sample.date_details
# sample.date_details['month'].value_counts()
# sample.date_details['month'].unique()



# engine_Sales_Data = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
# engine_Sales_Data.connect()
# legacy_users.to_sql('dim_user', engine_Sales_Data, if_exists='replace')
# pdf_data.to_sql('dim_card_details', engine_Sales_Data, if_exists='replace')
# store_data.to_sql('dim_store_details', engine_Sales_Data, if_exists='replace')
# product.to_sql('dim_products', engine_Sales_Data, if_exists='replace')
# order.to_sql('orders_table', engine_Sales_Data, if_exists='replace')
# date.to_sql('dim_date_times', engine_Sales_Data, if_exists='replace')

