import pandas as pd
import numpy as np
import re

class DataCleaning():

    def __init__(self, connector):
<<<<<<< HEAD
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
=======
        '''
        This method is the constructor of the class and it takes a parameter called connector, which is 
        used to establish a connection to an external system or resource. The method assigns the value of 
        the connector parameter to the instance variable self.connector. This allows the instance of the 
        class to access and use the provided connector for its operations.
        '''

        self.connector = connector

    def clean_user_data(self, df):

        # Drop NULL values
        df = df.replace(['NULL', 'N/A', 'None'], np.nan)
        df = df.dropna()

        # Clean data by dropping rows with specified indexes
        indexes = [752, 1046, 2995, 3536, 5306, 6420, 8386, 9013, 10211, 10360, 11366, 12177, 13111, 14101, 14499]
        df = df.drop(indexes)

        # Replace [.,x] with nothing in 'Column'
        columns = ['first_name', 'phone_number']
        df[columns] = df[columns].apply(lambda x: x.str.replace('[\.,x]', '', regex=True))

        # Capitalize the columns first_name and last_name
        df[['first_name', 'last_name']] = df[['first_name', 'last_name']].apply(lambda x: x.str.capitalize())

        # Removes '+' and adds '00' at the start of phone_number
        df['phone_number'] = df['phone_number'].apply(lambda x: x[1:] if x.startswith('+') else x)
        df['phone_number'] = df['phone_number'].apply(lambda x: '00' + x if not x.startswith('00') else x)

        # Remove hyphens from 'phone_number'
        df['phone_number'] = df['phone_number'].astype('str').apply(lambda x: x.replace('-', ''))

        # Remove spaces from 'phone_number'
        df['phone_number'] = df['phone_number'].astype('str').apply(lambda x: x.replace(' ', ''))

        # Replace [/,\n] with comma in 'address'
        df['address'] = df['address'].str.replace(r'[/,\n]', ',', regex=True)

        # Convert to datetime format
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], infer_datetime_format=True, errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], infer_datetime_format=True, errors='coerce')

        return df

    def clean_card_data(self, df_card):

        # Replace NULL values with NaN
        df_card = df_card.replace(['NULL', 'N/A', 'None'], np.nan)

        # Convert card_number column to string type
        df_card['card_number'] = df_card['card_number'].astype(str)

        # Remove '?' from card_number
        df_card['card_number'] = df_card['card_number'].str.replace(r'\?', '', regex=True)

        # Convert the DataFrame to string type
        df_card = df_card.astype(str)

        # Remove alphabetic characters from card_number  
        df_card[['card_number', 'expiry_date']] = df_card[['card_number', 'expiry_date']].replace('[A-Za-z]', np.nan, regex=True) 

        # Get indices of rows with NaN in 'card_number' column
        indices = df_card[df_card['card_number'].isna()].index

        # Replace corresponding values in 'card_provider' column with NaN
        df_card.loc[indices, 'card_provider'] = np.nan

        # Clean date columns
        df_card['date_payment_confirmed'] = pd.to_datetime(df_card['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce')

        return df_card

    def clean_store_data(self, df_stores):

        # Replace NULL with NaN
        df_stores = df_stores.replace(['NULL', 'N/A', 'None'], np.nan)

        # Replacing '\n' with ','
        df_stores['address'] = df_stores['address'].astype(str)
        df_stores['address'] = df_stores['address'].str.replace('\n', ',')

        # Clean date columns
        df_stores['opening_date'] = pd.to_datetime(df_stores['opening_date'], errors='coerce')

        # Get indices of rows with NaT in 'opening_date' column
        indices = df_stores[df_stores['opening_date'].isna()].index

        # Replace corresponding values in dataframe  with NaN
        df_stores['index'] = df_stores['index'].astype(int)
        df_stores.iloc[indices, 1:] = np.nan

        # Clean 'ee' from continent column
        df_stores['continent'] = df_stores['continent'].astype(str)
        df_stores['continent'] = df_stores['continent'].apply(lambda x: x[2:] if x.startswith('ee') else x)

        # Convert 'longitude' column to numeric type
        df_stores[['longitude', 'latitude']] = df_stores[['longitude', 'latitude']].apply(pd.to_numeric, errors='coerce')

        # Round longitude to 5 decimal points
        df_stores['longitude'] = df_stores['longitude'].round(5)

        # Round latitude to 5 decimal points
        df_stores['latitude'] = df_stores['latitude'].round(5)

        # Remove all alphabetic characters from column 'staff_numbers'
        df_stores['staff_numbers'] = df_stores['staff_numbers'].astype(str)
        df_stores['staff_numbers'] = df_stores['staff_numbers'].str.replace('[A-Za-z]', '', regex=True)

        # Replace 'None' with 'NaN' in column 'lat'
        df_stores['lat'] = df_stores['lat'].astype(str).replace('None', np.nan)

        return df_stores

    def clean_products_data(self, df_products):
    
        # Find indices of entries starting with '£'
        df_products['product_price'] = df_products['product_price'].astype(str)
        indices = df_products[~df_products['product_price'].str.contains('£')].index

        # Replace unnecessary values with 'NaN
        df_products.iloc[indices, 1:] = np.nan

        # Replace '.' with '' and remove whitespace
        df_products['weight'] = df_products['weight'].apply(
            lambda x: x.replace('.', '').strip() 
            if isinstance(x, str) and x.endswith('.') 
            else x
            )
        
        # Clean date columns
        df_products['date_added'] = pd.to_datetime(df_products['date_added'], errors ='coerce')

        # Rename column 1
        df_products = df_products.rename(columns={'Unnamed: 0': 'index'})
        
        return df_products
    
    def convert_product_weights(self, df_products):
        '''
        The convert_product_weights method is designed to modify and convert the weight column in the 
        data frame 'df_products'. The method applies three different functions to transform the 
        weight values in the DataFrame, the 'multiply_weight', 'convert_units_to_kg' and 
        'convert_weight_to_3_decimal_points'.
        '''   
        def multiply_weight(weight):
            '''
            This function takes a weight value as input, checks if it is a string containing 'x' 
            (e.g., "200g x 3"), and if so, extracts the numeric part after 'x', multiplies it with the 
            numeric part before 'x', and returns the multiplied value with 'g' appended. 
            '''
            if isinstance(weight, str) and 'x' in weight:
                parts = weight.split('x')
                numeric_part = int(parts[1].strip()[:-1])  # Extract the numeric part without 'g'
                multiplied_value = int(parts[0].strip()) * numeric_part  # Multiply the numeric parts
                return str(multiplied_value) + 'g'
            else:
                return weight
               
        def convert_units_to_kg(weight):
            '''
            This function converts the weight values to kilograms. It checks the unit (kg, g, ml, oz) at 
            the end of the weight value and converts it accordingly. The weight is then returned as a 
            string with 'kg' appended at the end of the string.
            '''
            if isinstance(weight, str):
                if weight.endswith('kg'):
                    numeric_part = float(weight[:-2])
                    return str(numeric_part) + 'kg'
                elif weight.endswith('g'):
                    numeric_part = float(weight[:-1]) / 1000
                    return str(numeric_part) + 'kg'
                elif weight.endswith('ml'):
                    numeric_part = float(weight[:-2]) / 1000
                    return str(numeric_part) + 'kg'
                elif weight.endswith('oz'):
                    numeric_part = float(weight[:-2]) / 35.274
                    return str(numeric_part) + 'kg'
            return weight
        
        def convert_weight_to_3_decimal_points(weight):
            '''
            This function rounds the weight value to three decimal points and returns it as a string with 
            'kg' appended at the end of the string.
            '''
            if isinstance(weight, str):
                numeric_part = round(float(weight[:-2]), 3)  # Extract numeric part, convert to float, and round to 3 decimal places
                return str(numeric_part) + 'kg'
            return weight
        
        # The apply() function is used to map each row of the 'weight' column through each of the three 
        # conversion functions.
        df_products['weight'] = df_products['weight'].apply(multiply_weight)
        df_products['weight'] = df_products['weight'].apply(convert_units_to_kg)
        df_products['weight'] = df_products['weight'].apply(convert_weight_to_3_decimal_points)

        return df_products
    
    def clean_orders_data(self, df_orders):
        
        # Drop the columns first-name, last_name and 1
        df_orders.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)

        return df_orders
    
    def clean_event_data(self, df_event_data):

        # Replace NULL values with 'Nan'
        df_event_data = df_event_data.replace(['NULL', 'N/A', 'None'], np.nan)

        # Convert the column month to numeric, non-numeric values will be NaN
        numeric_values = pd.to_numeric(df_event_data['month'], errors='coerce')

        # Get the indices of non-numeric values
        non_numeric_indices = np.where(np.isnan(numeric_values))

        # Replace unnecessary values with 'NaN
        df_event_data.iloc[non_numeric_indices] = np.nan

        # Clean time columns
        df_event_data['timestamp'] = pd.to_datetime(df_event_data['timestamp']).dt.time

        return df_event_data
>>>>>>> 74c2085 (project added up to milestone_2)