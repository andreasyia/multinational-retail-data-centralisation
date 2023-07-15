import yaml
import json
import sqlalchemy.exc
from sqlalchemy import create_engine, inspect
import pandas as pd
import tabula
import requests
import boto3
<<<<<<< HEAD

=======
import io
import json
>>>>>>> 74c2085 (project added up to milestone_2)

class DataExtractor():

    def __init__(self, connector):
<<<<<<< HEAD
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
=======
        '''
        The method is the constructor of the class and it takes a parameter called connector, which is 
        used to establish a connection to an external system or resource. The method assigns the value of 
        the connector parameter to the instance variable self.connector. This allows the instance of the 
        class to access and use the provided connector for its operations.

        '''
        self.connector = connector

    def read_rds_table(self, table_name):
        '''
        The method takes two parameters self and table_name. A query string is constructed which selects
        all the columns of the specified table and then establishes a connection using the
        'self.connector.engine.connect'. Once the connection it's established, the query it's executed using 
        'connection.execute(query)'. Once the data was retrieved, it was converted to a pandas data frame 
        using the command 'pd.Dataframe()'. Moreover, if an exception occurs during execution, an error message 
        is printed indicating the issue. Additionally, if the database engine is not initialised, a message
        is printed, instructing the user to initialise the engine before attempting to read data. By 
        employing try-except blocks, the method ensures graceful error handling.
        '''
        if self.connector.engine:
            try:
                query = f"SELECT * FROM {table_name}"
                #query = sqlalchemy.text(f"SELECT * FROM {table_name}")
                with self.connector.engine.connect() as connection:
                    result = connection.execute(query)
                    data = result.fetchall()
                    df = pd.DataFrame(data, columns=result.keys())
                    return df
>>>>>>> 74c2085 (project added up to milestone_2)
            except (sqlalchemy.exc.SQLAlchemyError, Exception) as error:
                print("Error extracting data from table:", error)
        else:
            print("Database engine not initialized. Please initialize the engine first.")

    def retrieve_pdf_data(self, link):
<<<<<<< HEAD
        try:
            # Extract data from all pages of the PDF
            df_list = tabula.read_pdf(link, pages=all)
=======
        '''
        The method takes two parameters self and link and provides a way of retrieving data from a PDF file
        using the 'tabula.read_pdf()' taking as parameters the link and pages will be extracted. The 
        'pd.concat()' function is used to concatenate the data frame in 'df_list' into a single data frame
        called df_card. In any case of an exception during the data extraction, the 'except' block will be 
        executed, printing out an error message.
        '''
        try:
            # Extract data from all pages of the PDF
            df_list = tabula.read_pdf(link, pages='1-279')
>>>>>>> 74c2085 (project added up to milestone_2)
            # Concatenate the dataframes into a single dataframe
            df_card = pd.concat(df_list, ignore_index=True)
            return df_card             
        except Exception as error:
            print("Error retrieving PDF data:", error)
            return None 
    
    def list_number_of_stores(self, num_stores_endpoint, header_dict):
<<<<<<< HEAD
=======
        '''
        The method takes three parameters self, num_of_stores(API endpoint) and header_dict(a dictionary 
        containing the headers required for API requests). Using the function 'requests.get()' the method 
        sends an HTTP GET request to the API endpoint. Then, by using 'response.status_code', the method 
        checks if the status code is 200(indicating a successful response) and it proceeds to process the
        data. The JSON response data is extracted using the 'response.json()' which contains the total 
        number of stores and is assigned to the variable named 'store_number'. If the response code 
        is not 200, an error message is printed indicating the status code. In case of exceptions 
        during the HTTP request, the 'except' block will be executed, which prints the error message 
        indicating the problem.
        '''
>>>>>>> 74c2085 (project added up to milestone_2)
        try:
            response = requests.get(num_stores_endpoint, headers=header_dict)
            if response.status_code == 200:
                data = response.json()
<<<<<<< HEAD
                number_of_stores = data['number_of_stores']
                return number_of_stores
=======
                store_number = data['number_stores']
                return store_number
>>>>>>> 74c2085 (project added up to milestone_2)
            else:
                print("Error retrieving the number of stores. Status code:", response.status_code)
        except requests.exceptions.RequestException as error:
            print("Error connecting to the API:", error)
        return None

<<<<<<< HEAD

    def retrieve_stores_date(self, store_endpoint, header_dict):
        try:
            response = requests.get(store_endpoint, headers=header_dict)
            if response.status_code == 200:
                data = response.json()
                stores = data['stores']
                df_stores = pd.DataFrame(stores)
                return df_stores
=======
    def retrieve_stores_data(self, store_endpoint, header_dict):
        '''
        The method initialises an empty list 'df_stores' to store data frames for each store and an empty 
        list 'endpoints' to store the individual endpoints. A for loop was used to generate individual store
        endpoints by iterating from 0 to 450 and the URL of each store is now constructed using f-strings
        and appended to the 'endpoints' list. Inside the 'try' block, the for loop iterates through the 
        endpoints list and sends an HTTP GET request to each store's endpoint using the 'request.get()'
        function. The method checks that the response status code is 200 by using the function 
        'response.status_code' and the JSON response data is extracted using response.json(). A data frame 
        was created using pd.DataFrame() and the loop appends it to the 'df_stores' list. If the response 
        code is not 200, a message is printed indicating the status code and in case an exception occurs, 
        it prints the error message showing the problem. After all the iterations, the method concatenates
        the 'df_stores' into a single data frame 'concat_df_stores' using pd.concat().
        '''
        df_stores = []
        endpoints = []

        for store_number in range(0, 451):
            endpoint = f"{store_endpoint}/{store_number}"
            endpoints.append(endpoint)

        try:
            for index, endpoint in enumerate(endpoints):
                response = requests.get(endpoint, headers=header_dict)
                if response.status_code == 200:
                    data = response.json()
                df_store = pd.DataFrame(data, index = [index])
                df_stores.append(df_store)  
>>>>>>> 74c2085 (project added up to milestone_2)
            else:
                print("Error retrieving stores data. Status code:", response.status_code)
        except requests.exceptions.RequestException as error:
            print("Error connecting to the API:", error)
<<<<<<< HEAD

        return None
    
    def extract_from_s3(self, s3_address):
=======
        
        concat_df_stores = pd.concat(df_stores)
                
        return concat_df_stores
    
    def extract_s3(self, s3_address):
        '''
        The method takes two parameters self and s3_address. The method initialises an S3 client using 
        'boto3' library to interact with AWS S3. It then extracts the 'bucket_name' and 'key'. Inside the
        'try' block, the method uses the S3 client to get the object from the specified bucket and key
        using 's3.get_object()'. The response contains the data which is read as a UTF-8 encoded string
        using 'response['Body'].read().decode('utf-8')'. Then the data is read into a data frame 
        'df_s3_products' using 'pd.read_csv(io.StringIO(products_data))'. The 'io.StringIO()' function allows 
        reading the CSV data from the string. The method returns the 'df_s3_products' and if an exception 
        occurs the 'except' block will be executed, displaying the error message.
        '''
>>>>>>> 74c2085 (project added up to milestone_2)
        s3 = boto3.client('s3')
        bucket_name, key = s3_address.split('/', 3)[2:]

        try:
            response = s3.get_object(Bucket = bucket_name, Key= key)
            products_data = response['Body'].read().decode('utf-8')
<<<<<<< HEAD
            df_s3_products = pd.read_csv(pd.compat.StringIO(products_data))
=======
            df_s3_products = pd.read_csv(io.StringIO(products_data))
>>>>>>> 74c2085 (project added up to milestone_2)
            return df_s3_products
        except Exception as error:
            print("Error retrieving data from S3:", error)

        return None
<<<<<<< HEAD



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

=======
    
    def retrieve_date_events_data(self, store_endpoint, header_dict):
        '''
        The method takes three parameters self, num_of_stores(API endpoint) and header_dict(a dictionary 
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
>>>>>>> 74c2085 (project added up to milestone_2)
