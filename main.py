from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Paths
creds_file = 'db_creds.yaml'
pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
header_dict = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
s3_address = 's3://data-handling-public/products.csv'
data_events_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'

# Instances
connector = DatabaseConnector()
extractor = DataExtractor(connector)
data_cleaner = DataCleaning(connector)



def user_data(connector, extractor, data_cleaner, creds_file):
    
    # Initiating database engine and listing the table names
    creds = connector.read_db_creds(creds_file)
    engine = connector.init_db_engine(creds)

    # Extracting data from table
    connector.list_db_tables(engine) # tables names
    df_user = extractor.read_rds_table('legacy_users', engine)

    # Cleaning the extracted data
    cleaned_data = data_cleaner.clean_user_data(df_user)
    cleaned_data.to_string('users_data')

    # Uploading the dataframe to SQL
    connector.upload_to_db(cleaned_data, 'dim_users')

def card_data(connector, extractor, data_cleaner, pdf_link):

    # Extracting data from pdf
    df_card = extractor.retrieve_pdf_data(pdf_link)

    # Cleaning the extracted data
    cleaned_data = data_cleaner.clean_card_data(df_card)
    cleaned_data.to_string('card_data')

    # Uploading the dataframe to SQL
    connector.upload_to_db(cleaned_data, 'dim_card_details')

def store_data(connector, extractor, data_cleaner, num_stores_endpoint, header_dict, store_endpoint):

    # Extracting the number of tables 
    store_number = extractor.list_number_of_stores(num_stores_endpoint, header_dict)

    # Extracting data 
    df_stores = extractor.retrieve_stores_data(store_endpoint, header_dict, store_number)

    # Cleaning the extracted data
    cleaned_data = data_cleaner.clean_store_data(df_stores)
    cleaned_data.to_string('store_details')

    # Uploading the dataframe to SQL
    connector.upload_to_db(cleaned_data, 'dim_store_details')
    
def product_data(connector, extractor, data_cleaner, s3_address):

    # Extracting data from S3
    df_product = extractor.extract_from_s3(s3_address)

    # Cleaning the extracted data
    cleaned_data = data_cleaner.clean_products_data(df_product)
    cleaned_data = data_cleaner.convert_product_weights(cleaned_data)
    cleaned_data.to_string('product_details')

    # Uploading the dataframe to SQL
    connector.upload_to_db(cleaned_data, 'dim_products')

def orders_data(connector, extractor, data_cleaner, creds_file):

     # Initiating database engine and listing the table names
    creds = connector.read_db_creds(creds_file)
    engine = connector.init_db_engine(creds)

    # Extracting data from table
    connector.list_db_tables(engine) # tables names
    df_orders = extractor.read_rds_table('orders_table', engine)

    # Cleaning the extracted data
    cleaned_data = data_cleaner.clean_orders_data(df_orders)
    cleaned_data.to_string('orders_table')
    
    # Uploading the dataframe to SQL
    connector.upload_to_db(cleaned_data, 'orders_table')

def date_events_data(connector, extractor, data_cleaner, data_events_path):

    # Extract data from AWS
    df_events_data = extractor.retrieve_date_events_data(data_events_path, header_dict)

    # Cleaning the extracted data
    cleaned_data = data_cleaner.clean_event_data(df_events_data)
    cleaned_data.to_string('event_data')

    # Uploading the dataframe to SQL
    connector.upload_to_db(cleaned_data, 'dim_date_times')

# user_data(connector, extractor, data_cleaner, creds_file)
# card_data(connector, extractor, data_cleaner, pdf_link)
# store_data(connector, extractor, data_cleaner, num_stores_endpoint, header_dict, store_endpoint)
# product_data(connector, extractor, data_cleaner, s3_address)
# orders_data(connector, extractor, data_cleaner, creds_file)
# date_events_data(connector, extractor, data_cleaner, data_events_path)