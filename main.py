import pandas as pd
import numpy as np
import sqlalchemy 
from sqlalchemy import create_engine
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


creds_file = '/Users/andreasyianni/Desktop/multinational-retail-data-centralisation/db_creds.yaml'
# Initiating database engine and listing the table names
connector = DatabaseConnector(creds_file)

connector.read_db_creds(creds_file)
connector.init_db_engine()
tables = connector.list_db_tables()

# Extracting data from table
extractor = DataExtractor(connector)

table_names = connector.list_db_tables()
df_user = extractor.read_rds_table('legacy_users')
# print(df_user)

# Cleaning the extracted data
data_cleaner = DataCleaning(connector)

cleaned_data = data_cleaner.clean_user_data(df_user)
# print(cleaned_data)

# Uploading the dataframe to SQL
connector.upload_to_db(cleaned_data, 'dim_users')