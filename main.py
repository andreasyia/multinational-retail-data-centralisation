import pandas as pd
import numpy as np
import sqlalchemy 
import requests
from sqlalchemy import create_engine
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Paths
creds_file = '/Users/andreasyianni/Desktop/multinational-retail-data-centralisation/db_creds.yaml'
pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
header_dict = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
s3_address = 's3://data-handling-public/products.csv'
data_events_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

# Instances
connector = DatabaseConnector(creds_file)
extractor = DataExtractor(connector)
data_cleaner = DataCleaning(connector)


def extract_and_clean_user_data(connector, extractor, data_cleaner):
    
    # Initiating database engine and listing the table names
    connector.read_db_creds(creds_file)
    connector.init_db_engine()

    # Extracting data from table
    table_names = connector.list_db_tables()
    df_user = extractor.read_rds_table('legacy_users')

    # Cleaning the extracted data
    cleaned_data = data_cleaner.clean_user_data(df_user)
    cleaned_data.to_string('users_data')

    # Uploading the dataframe to SQL
    connector.upload_to_db(cleaned_data, 'dim_users')

def extract_and_clean_card_data(connector, extractor, data_cleaner):

    # Extracting data from pdf
    df_card = extractor.retrieve_pdf_data(pdf_link)

    # Cleaning the extracted data
    cleaned_data = data_cleaner.clean_card_data(df_card)
    cleaned_data.to_string('card_data')

    # Uploading the dataframe to SQL
    connector.upload_to_db(cleaned_data, 'dim_card_details')

def extract_and_clean_store_data(connector, extractor, data_cleaner):

    # Extracting the number of tables 
    stores_number = extractor.list_number_of_stores(num_stores_endpoint, header_dict)

    # Extracting data 
    df_stores = extractor.retrieve_stores_data(store_endpoint, header_dict)

    # Cleaning the extracted data
    cleaned_data = data_cleaner.clean_store_data(df_stores)
    cleaned_data.to_string('store_details')

    # Uploading the dataframe to SQL
    connector.upload_to_db(cleaned_data, 'dim_store_details')
    
def extract_and_clean_product_data(connector, extractor, data_cleaner):

    # Extracting data from S3
    df_product = extractor.extract_s3(s3_address)

    # Cleaning the extracted data
    cleaned_data = data_cleaner.clean_products_data(df_product)
    cleaned_data_weight = data_cleaner.convert_product_weights(cleaned_data)
    cleaned_data_weight.to_string('product_details')

    # Uploading the dataframe to SQL
    connector.upload_to_db(cleaned_data_weight, 'dim_products')

def extract_and_clean_orders_data(connector, extractor, data_cleaner):

     # Initiating database engine and listing the table names
    connector.read_db_creds(creds_file)
    connector.init_db_engine()

    # Extracting data from table
    table_names = connector.list_db_tables()
    df_orders = extractor.read_rds_table('orders_table')

    # Cleaning the extracted data
    cleaned_data = data_cleaner.clean_orders_data(df_orders)
    cleaned_data.to_string('orders_table')
    
    # Uploading the dataframe to SQL
    connector.upload_to_db(cleaned_data, 'orders_table')

def extract_and_clean_date_events_data(connector, extractor, data_cleaner):

    # Extract data from AWS
    df_events_data = extractor.retrieve_date_events_data(data_events_path, header_dict)
    df_events_data.to_string('event_data_dirty')

    # Cleaning the extracted data
    cleaned_data = data_cleaner.clean_event_data(df_events_data)
    cleaned_data.to_string('event_data')

    # Uploading the dataframe to SQL
    connector.upload_to_db(cleaned_data, 'dim_date_times')

    return df_events_data

extract_and_clean_user_data(connector, extractor, data_cleaner)

# extract_and_clean_card_data(connector, extractor, data_cleaner)

# extract_and_clean_store_data(connector, extractor, data_cleaner)

# extract_and_clean_product_data(connector, extractor, data_cleaner)

# extract_and_clean_orders_data(connector, extractor, data_cleaner)

# extract_and_clean_date_events_data(connector, extractor, data_cleaner)