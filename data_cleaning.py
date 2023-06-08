import pandas as pd
import numpy as np
import re

class DataCleaning():

    def __init__(self, connector):
        self.connector = connector

    def clean_user_data(self, df):
        print(df)
        df_new = df.copy()

        # Drop NULL values
        df_new.replace(['NULL', 'N/A', 'None'], np.nan, inplace=True)
        df_new = df_new.dropna()

        # Clean data by dropping rows with specified indexes
        indexes = [752, 1046, 2995, 3536, 5306, 6420, 8386, 9013, 10211, 10360, 11366, 12177, 13111, 14101, 14499]
        df_new = df_new.drop(indexes)

        # Replace [.,x] with nothing in 'Column'
        columns = ['first_name', 'phone_number']
        df_new[columns] = df_new[columns].apply(lambda x: x.str.replace('[\.,x]', '', regex=True))

        # Capitalize the columns first_name and last_name
        df_new[['first_name', 'last_name']] = df_new[['first_name', 'last_name']].apply(lambda x: x.str.capitalize())

        # Removes '+' and adds '00' on the start of phone_number
        df_new['phone_number'] = df_new['phone_number'].apply(lambda x: x[1:] if x.startswith('+') else x)
        df_new['phone_number'] = df_new['phone_number'].apply(lambda x: '00' + x if not x.startswith('00') else x)

        # Remove hyphens and spaces from 'phone_number'
        df_new['phone_number'] = df_new['phone_number'].apply(lambda x: x.replace(r'[-\s]', ''))

        # Replace [/,\n] with nothing in 'address'
        df_new['address'] = df_new['address'].str.replace(r'[/,\n]', ',', regex=True)

        # Convert to datetime format
        df_new['date_of_birth'] = pd.to_datetime(df_new['date_of_birth'],infer_datetime_format=True, errors='coerce') 
        df_new['join_date'] = pd.to_datetime(df_new['join_date'], infer_datetime_format=True, errors='coerce')

        return df_new
    

    def clean_card_data(self,dataframe ):

        # Drop rows with NULL values
        df_card = df_card.dropna()

        # Remove leading/trailing whitespaces
        df_card['card_number'] = df_card['card_number'].str.strip()
        df_card['expiry_date'] = df_card['expiry_date'].str.strip()
        df_card['card_provider'] = df_card['card_holder'].str.strip()
        df_card['date_payment_confirmed'] = df_card['date_payment_confirmed'].str.strip()

        # Remove non-numeric characters from card number
        df_card['card_number'] = df_card['card_number'].str.replace(r'\D+', '', regex=True)

        # Clean date columns
        df_card['expiry_date'] = pd.to_datetime(df_card['expiry_date'], errors='coerce')
        df_card['date_payment_confirmed'] = pd.to_datetime(df_card['date_payment_confirmed'], errors='coerce')

        return df_card
    
    def clean_store_data(self, df_stores):

        df_stores = df_stores.dropna()
        return df_stores
    
    def convert_product_weights(self, df_s3_products):

        # Clean up the weight column and convert weights to float
        df_s3_products['weight'] = df_s3_products['weight'].apply(self.clean_weight_value)

        # Convert ml to g using a rough estimate of 1:1 ratio
        df_s3_products.loc[df_s3_products['weight_unit'] == 'ml', 'weight'] *= 1

        # Convert weights to kg
        df_s3_products.loc[df_s3_products['weight_unit'] == 'g', 'weight'] /= 1000

        # Drop the weight_unit column
        df_s3_products.drop('weight_unit', axis=1, inplace=True)

        return df_s3_products
    
    def clean_weight_value(self, weight):
        # Remove excess characters from the weight value
        weight = re.sub(r'[^0-9.]+', '', weight)

        # Convert weight to float
        try:
            weight = float(weight)
        except ValueError:
            weight = None

        return weight
    
    def clean_products_data(self, df_s3_products, column_name):
        # Remove leading/trailing whitespaces
        df_s3_products[column_name] = df_s3_products[column_name].str.strip()

        # Remove duplicate values
        df_s3_products[column_name] = df_s3_products[column_name].drop_duplicates()

        return df_s3_products
    



#     # Define the rows to clean
# rows_to_clean = [1, 3]  # Rows 1 and 3 will be cleaned

# # Clean the specified rows in the 'text' column
# df.loc[rows_to_clean, 'text'] = df.loc[rows_to_clean, 'text'].str.replace('[A-Za-z0-9]', 