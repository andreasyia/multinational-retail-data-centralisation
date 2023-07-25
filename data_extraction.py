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
        '''
        The method is the constructor of the class and it takes a parameter called connector, which is 
        used to establish a connection to an external system or resource. The method assigns the value of 
        the connector parameter to the instance variable self.connector. This allows the instance of the 
        class to access and use the provided connector for its operations.
        '''
        self.connector = connector

    def read_rds_table(self, table_name, engine):
        '''
        The method takes two parameters 'table_name' and 'engine'. A query string is constructed which selects
        all the columns of the specified table and then establishes a connection using the
        'engine.connect()'. Once the connection it's established, the query it's executed using 
        'pd.read_sql_query(query, connection)' and then , it returns the 'df_user'. Moreover, if an 
        exception occurs during execution, an error message is printed indicating the issue. Additionally, 
        if the database engine is not initialised, a message is printed, instructing the user to initialise 
        the engine before attempting to read data. By employing try-except blocks, the method ensures 
        graceful error handling.
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
        The method takes one parameter 'link' and is used for retrieving data from a PDF file
        using the 'tabula.read_pdf()' taking as parameters the link and pages will be extracted. The 
        'pd.concat()' function is used to concatenate the data frame in 'df_list' into a single data frame
        called 'df_card'. In any case of an exception during the data extraction, the except block will be 
        executed, printing out an error message.
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
        The method takes three parameters self, num_stores_endpoint(API endpoint) and header_dict(a dictionary 
        containing the headers required for API requests). Using the function 'requests.get()' the method 
        sends an HTTP GET request to the API endpoint. Then, by using 'response.status_code', the method 
        checks if the status code is 200(indicating a successful response) and it proceeds to process the
        data. The JSON response data is extracted using the 'response.json()' which contains the total 
        number of stores and is assigned to the variable named 'store_number'. If the response code 
        is not 200, an error message is printed indicating the status code. In case of exceptions 
        during the HTTP request, the 'except' block will be executed, which prints the error message 
        indicating the problem.
        '''
        try:
            response = requests.get(num_stores_endpoint, headers=header_dict)
            if response.status_code == 200:
                data = response.json()
                store_number = data['number_stores'] 
                return store_number
            else:
                print("Error retrieving the number of stores. Status code:", response.status_code)
        except requests.exceptions.RequestException as error:
            print("Error connecting to the API:", error)

    def retrieve_stores_data(self, store_endpoint, header_dict, store_number):
        '''
        The function 'retrieve_stores_data' takes three parameters, the 'store_endpoint', 'header_dict' and 
        'store_number'. The method initialises an empty list 'df_stores' to store the data frames for each 
        store and an empty list named 'endpoints' to store the individual endpoints. A loop was used to 
        generate individual store endpoints by iterating from 0 to 'store_number' and the URL of each store 
        is now constructed using f-strings and appended to the 'endpoints' list. Inside the try block, the 
        for loop iterates through the endpoints list and sends an HTTP GET request to each store's endpoint 
        using the 'request.get()' function. The method checks that the response status code is 200 by using 
        the function 'response.status_code' and the JSON response data is extracted using 'response.json()'. 
        A data frame was created using 'pd.DataFrame()' and the loop appends it to the 'df_stores' list. If 
        the response code is not 200, a message is printed indicating the status code and in case an exception 
        occurs, it prints the error message showing the problem. After all the iterations, the method 
        concatenates the 'df_stores' into a single data frame 'concat_df_stores' using 'pd.concat()'.
        
        '''
        df_stores = []
        endpoints = []
        
        for number in range(0, store_number):
            endpoint = f"{store_endpoint}/{number}"
            endpoints.append(endpoint)

        try:
            for index, endpoint in enumerate(endpoints):
                response = requests.get(endpoint, headers=header_dict)
                if response.status_code == 200:
                    data = response.json()
                df_store = pd.DataFrame(data, index = [index])
                df_stores.append(df_store)  
            else:
                print("Error retrieving stores data. Status code:", response.status_code)
        except requests.exceptions.RequestException as error:
            print("Error connecting to the API:", error)
        
        concat_df_stores = pd.concat(df_stores)
                
        return concat_df_stores
    
    def extract_from_s3(self, s3_address):
        '''
        The method takes one parameter 's3_address'. The method initialises an S3 client using 
        'boto3' library to interact with AWS S3 and then, it extracts the 'bucket_name' and 'key'. The data
        is downloaded in csv format named 'products.csv' and by using the method 'pd.read_csv(df_products_file)'
        the data is converted to a pandas Dataframe and the function returns the 'df_products'.
        '''
        s3 = boto3.client('s3')
        bucket_name, key = s3_address.split('/', 3)[2:]
        df_products = s3.download_file(bucket_name,key,'products.csv')
    
        df_products_file = 'products.csv'
        df_products = pd.read_csv(df_products_file)
        return df_products
            
        
    def retrieve_date_events_data(self, store_endpoint, header_dict):
        '''
        The method takes two parameters 'num_of_stores'(API endpoint) and 'header_dict'(a dictionary 
        containing the headers required for API requests). Using the function 'requests.get()' the method 
        sends an HTTP GET request to the API endpoint. Then, by using 'response.status_code', the method 
        checks if the status code is 200(indicating a successful response) and it proceeds to process the
        data. The JSON response data is extracted using the 'response.json()' which contains the data. Then
        a data frame was created named 'df_events_data' to store the data extracted. If the response code is
        not 200, a message is printed indicating the status code and in case an exception occurs, it prints 
        the error message showing the problem.
        '''
        try:
            response = requests.get(store_endpoint, headers=header_dict)
            if response.status_code == 200:
                data = response.json()
                df_events_data = pd.DataFrame(data)
            else:
                print("Error retrieving stores data. Status code:", response.status_code)
        except requests.exceptions.RequestException as error:
            print("Error connecting to the API:", error)
        return df_events_data
