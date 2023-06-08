import yaml
import json
import sqlalchemy.exc
from sqlalchemy import create_engine, inspect
import pandas as pd
import tabula
import requests
import boto3


class DataExtractor():

    def __init__(self, connector):
        self.connector = connector

    def read_rds_table(self, table_name):
        if self.connector.engine:
            try:
                query = f"SELECT * FROM {table_name}"
                with self.connector.engine.connect() as connection:
                    result = connection.execute(query)
                    data = result.fetchall()
                    dataframe = pd.DataFrame(data, columns=result.keys())
                    return dataframe
            except (sqlalchemy.exc.SQLAlchemyError, Exception) as error:
                print("Error extracting data from table:", error)
        else:
            print("Database engine not initialized. Please initialize the engine first.")

    def retrieve_pdf_data(self, link):
        try:
            # Extract data from all pages of the PDF
            df_list = tabula.read_pdf(link, pages=all)
            # Concatenate the dataframes into a single dataframe
            df_card = pd.concat(df_list, ignore_index=True)
            return df_card             
        except Exception as error:
            print("Error retrieving PDF data:", error)
            return None 
    
    def list_number_of_stores(self, num_stores_endpoint, header_dict):
        try:
            response = requests.get(num_stores_endpoint, headers=header_dict)
            if response.status_code == 200:
                data = response.json()
                number_of_stores = data['number_of_stores']
                return number_of_stores
            else:
                print("Error retrieving the number of stores. Status code:", response.status_code)
        except requests.exceptions.RequestException as error:
            print("Error connecting to the API:", error)
        return None


    def retrieve_stores_date(self, store_endpoint, header_dict):
        try:
            response = requests.get(store_endpoint, headers=header_dict)
            if response.status_code == 200:
                data = response.json()
                stores = data['stores']
                df_stores = pd.DataFrame(stores)
                return df_stores
            else:
                print("Error retrieving stores data. Status code:", response.status_code)
        except requests.exceptions.RequestException as error:
            print("Error connecting to the API:", error)

        return None
    
    def extract_from_s3(self, s3_address):
        s3 = boto3.client('s3')
        bucket_name, key = s3_address.split('/', 3)[2:]

        try:
            response = s3.get_object(Bucket = bucket_name, Key= key)
            products_data = response['Body'].read().decode('utf-8')
            df_s3_products = pd.read_csv(pd.compat.StringIO(products_data))
            return df_s3_products
        except Exception as error:
            print("Error retrieving data from S3:", error)

        return None



# extractor = DataExtractor(connector)


# table_names = connector.list_db_tables()
# print("Tables in the database:")
# for table_name in table_names:
#     print(table_name)


# df_user = extractor.read_rds_table(table_name)
# print(df_user)

# pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# # Extract data from all pages of the PDF
# pdf_data = extractor.retrieve_pdf_data(pdf_link)
# if pdf_data is not None:
#     print(pdf_data)



#### for list_number_of_stores method
# creds_file = 'db_creds.yaml'
# connector = DatabaseConnector(creds_file)
# connector.init_db_engine()
# extractor = DataExtractor(connector)
# num_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
# header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
# number_of_stores = extractor.list_number_of_stores(num_stores_endpoint, header_dict)
# if number_of_stores is not None:
#     print("Number of stores:", number_of_stores)

#### retrive_stores_data
# store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
# stores_data = extractor.retrieve_stores_data(store_endpoint, header_dict)
# if stores_data is not None:
#     print(stores_data)

### AWS
# extractor = DataExtractor()
# s3_address = 's3://data-handling-public/products.csv'
# products_data = extractor.extract_from_s3(s3_address)
# if products_data is not None:
#     print(products_data)

