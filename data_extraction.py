from sqlalchemy import create_engine, inspect
import boto3
import json
import pandas as pd
import requests
import sqlalchemy.exc
import tabula
import yaml

class DataExtractor():
    """
    The class 'DataExtractor' is designed to extract data from various sources such as databases, files, 
    APIs and web pages.

    Methods
    -------
    read_rds_table(self, table_name, engine)
    retrieve_pdf_data(self, link)
    list_number_of_stores(self, num_stores_endpoint, header_dict)
    retrieve_stores_data(self, store_endpoint, header_dict, store_number)
    extract_from_s3(self, s3_address)
    retrieve_date_events_data(self, store_endpoint, header_dict)
    """

    
    def read_rds_table(self, table_name, engine):
        '''
        The method 'read_rds_table' is responsible for reading data from a specified table in a relational 
        database system.

        A query string is constructed which selects all the columns of the specified table and then 
        establishes a connection using the 'engine.connect()'. Once the connection it's established, the 
        query it's executed using 'pd.read_sql_query(query, connection)'. Moreover, if an exception occurs 
        during execution, an error message is printed regarding the issue arose.
        
            Parameters:
                    table_name(String): Specify the name of the database table
                    engine(SQLAlchemy Engine object): Establishing a connection to the database

            Returns:
                    df_user(Dataframe): A dataframe containing the data from the table returned
        '''
        if engine:
            try:
                query = sqlalchemy.text(f"SELECT * FROM {table_name}")
                with engine.connect() as connection:
                    df_user = pd.read_sql_query(query, connection)
                    return df_user
            except (sqlalchemy.exc.SQLAlchemyError, Exception) as error:
                print("Error extracting data from table:", error)
        else:
            print("Database engine not initialized. Please initialize the engine first.")

    def retrieve_pdf_data(self, link):
        '''
        The method 'retrive_pdf_data' is responsible for extracting data from a PDF file.

            Parameters:
                    link(String): Represents the link to the PDF file

            Returns:
                    df_card(Dataframe): A dataframe containing the data from the PDF file
        '''
        try:
            # Extract data from all pages of the PDF
            df_list = tabula.read_pdf(link, pages='all')
            # Concatenate the dataframes into a single dataframe
            df_card = pd.concat(df_list, ignore_index=True)
            return df_card             
        except Exception as error:
            print("Error retrieving PDF data:", error)
    
    def list_number_of_stores(self, num_stores_endpoint, header_dict):
        '''
        The method 'list_number_of_stores' is responsible for fetching the total number of stores from an 
        API endpoint using an HTTP GET request.

            Parameters:
                    num_stores_endpoint(String): The API endpoint URL
                    header_dict(Dictionary): A dictionary containing headers required for API requests
            
            Returns:
                    store_number(Int): The number of stores
        '''
        try:
            response = requests.get(num_stores_endpoint, headers=header_dict) # It sends an HTTP GET request to the API endpoint
            if response.status_code == 200: # Check if the status_code is 200 (indicating a successful response)
                data = response.json()
                store_number = data['number_stores'] 
                return store_number
            else:
                print("Error retrieving the number of stores. Status code:", response.status_code)
        except requests.exceptions.RequestException as error:
            print("Error connecting to the API:", error)

    def retrieve_stores_data(self, store_endpoint, header_dict, store_number):
        '''
        The method 'retrieve_stores_data' is responsible for retrieving data for multiple stores from an API 
        endpoint and combining the data into a single pandas DataFrame.

            Parameters:
                    store_endpoint(String): The base API endpoint for the stores 
                    header_dict(Dictionary): A dictionary containing headers required for API requests
                    store_number(Int): The number of stores

            Returns:
                    concat_df_stores(Dataframe): It returns the concatenated DataFrame 'concat_df_stores', 
                                                 which contains the combined data for all the stores 
                                                 retrieved from the API endpoint
        '''
        # Initialises two empty lists 'df_stores' & 'endpoints'
        df_stores = []
        endpoints = []
        # Generates individual endpoints based on 'store_endpoint' & 'store_number' using f-strings ranging from 0 to 'store_number' and appends them to the 'endpoints' list
        for number in range(0, store_number):
            endpoint = f"{store_endpoint}/{number}"
            endpoints.append(endpoint)
        try:
            for index, endpoint in enumerate(endpoints):
                response = requests.get(endpoint, headers=header_dict) # It sends an HTTP GET request to the API endpoint
                if response.status_code == 200: # Check if the status_code is 200 (indicating a successful response)
                    data = response.json()
                df_store = pd.DataFrame(data, index = [index])
                df_stores.append(df_store)  
            else:
                print("Error retrieving stores data. Status code:", response.status_code)
        except requests.exceptions.RequestException as error:
            print("Error connecting to the API:", error)
        concat_df_stores = pd.concat(df_stores) # Combining the data into a single pandas DataFrame  

        return concat_df_stores
    
    def extract_from_s3(self, s3_address):
        '''
        The method 'extract_from_s3' is responsible for extracting data from an Amazon S3 bucket.

            Parameters:
                    s3_address(String): It represents the S3 address where the data is located

            Returns:
                    df_products(Dataframe): A dataframe containing the data extracted from the CSV file 
                    stored in the specified S3 bucket
        '''
        s3 = boto3.client('s3') # It initialises an S3 client using 'boto3' library to interact with AWS S3
        bucket_name, key = s3_address.split('/', 3)[2:] # The split() function was used to separate the bucket name and key from the address
        df_products = s3.download_file(bucket_name,key,'products.csv') # It downloads the data in CSV format
        df_products_file = 'products.csv'
        df_products = pd.read_csv(df_products_file)

        return df_products  
        
    def retrieve_date_events_data(self, store_endpoint, header_dict):
        '''
        The method 'retrieve_date_events_data' is responsible for retrieving date events data from an API 
        endpoint and converting it into a pandas DataFrame.

            Parameters:
                    store_endpoint(String): The API endpoint URL for date events data
                    header_dict(Dictionary): A dictionary containing headers required for API requests

            Returns:
                    df_events_data(Dataframe): A dataframe containing the date events data extracted from the API endpoint
        '''
        try:
            response = requests.get(store_endpoint, headers=header_dict) # It sends an HTTP GET request to the API endpoint
            if response.status_code == 200: # Check if the status_code is 200 (indicating a successful response)
                data = response.json()
                df_events_data = pd.DataFrame(data)
            else:
                print("Error retrieving stores data. Status code:", response.status_code)
        except requests.exceptions.RequestException as error:
            print("Error connecting to the API:", error)

        return df_events_data
    
