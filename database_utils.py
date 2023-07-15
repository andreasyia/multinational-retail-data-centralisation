import yaml
import sqlalchemy.exc
from sqlalchemy import create_engine, inspect
import pandas as pd
import getpass

creds_file = '/Users/andreasyianni/Desktop/multinational-retail-data-centralisation/db_creds.yaml'

class DatabaseConnector():

    def __init__(self, creds_file):
<<<<<<< HEAD
=======
        ''' 
        The __init__ method initializes variables related to a database connection, such as credentials,
        engine, and connection objects. 
        '''
>>>>>>> 74c2085 (project added up to milestone_2)
        self.creds = self.read_db_creds(creds_file)
        self.engine = None
        self.connection = None


    def read_db_creds(self, creds_file):
<<<<<<< HEAD
=======
        ''' 
        The function read_db_creds take as a parameter the creds_file, a yaml type and returns the 
        credentials for connecting to the database.
        '''
>>>>>>> 74c2085 (project added up to milestone_2)
        with open(creds_file, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials
    
    def init_db_engine(self):
<<<<<<< HEAD
=======
        ''' 
        The init_db_engine initialises the database engine by using `create_engine` from sqlalchemy 
        library and the credentials retrieved from the function `read_db_creds`. Also a try and except 
        block were created to handle errors such as if the database was not initialised.
        '''
>>>>>>> 74c2085 (project added up to milestone_2)
        try:
            db_url = f"postgresql://{self.creds['RDS_USER']}:{self.creds['RDS_PASSWORD']}@{self.creds['RDS_HOST']}:{self.creds['RDS_PORT']}/{self.creds['RDS_DATABASE']}"
            self.engine = sqlalchemy.create_engine(db_url)
            print("Database engine initialized!")
        except (sqlalchemy.exc.SQLAlchemyError, Exception) as error:
            print("Error initializing database engine:", error)

        return self.engine
    
    def list_db_tables(self):
<<<<<<< HEAD
=======
        ''' 
        In this function there are conditional if-else statements along with try, except blocks for 
        handling the errors. The engine was inspected by the inspector making sure that everything works 
        as expected.Then, for loop was used to print all the table names.
        '''
>>>>>>> 74c2085 (project added up to milestone_2)
        if self.engine:
            try:
                inspector = sqlalchemy.inspect(self.engine)
                table_names = inspector.get_table_names()
<<<<<<< HEAD
                # return table_names
                print("Tables in  the database:")
                for table_name in table_names:
                    print(table_name)
=======
                print("Tables in  the database:")
                for table_name in table_names:
                    print(table_name) 
>>>>>>> 74c2085 (project added up to milestone_2)
            except (sqlalchemy.exc.SQLAlchemyError, Exception) as error:
                print("Error listing database tables:", error)
        else:
            print("Database engine not initialized. Please initialize the engine first.")

    def upload_to_db(self, df, table_name):
<<<<<<< HEAD
=======
        ''' 
        The upload_to_db function is used to upload the data to a database in postgresql. For security 
        reasons the getpass was used to provide the password for connecting to the database.Then, the 
        self.engine is requesting a connection to the database using 'sqlalchemy.create_engine()' in 
        postgresql and then, uploads the data with the specified name given for the table.
        '''
>>>>>>> 74c2085 (project added up to milestone_2)
        password = getpass.getpass("Enter your password: ")
        sql_connection = (f'postgresql://postgres:{password}@localhost/sales_data')
        self.engine =sqlalchemy.create_engine(sql_connection)
        if self.engine:
            try:
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                print("Data uploaded to database table successfully!")
            except (sqlalchemy.exc.SQLAlchemyError, Exception) as error:
                print("Error uploading data to database:", error)
        else:
<<<<<<< HEAD
            print("Database engine not initialized. Please initialize the engine first.")

connector = DatabaseConnector(creds_file)

# Uploading(dim_users, dim_card_details, dim_store_details, dim_products) all the same way
# table_to_upload = 'dim_users'
# # connector.upload_to_db(df_user_1, table_to_upload)
# print("Data uploaded to the 'dim_users' table of the 'sales_data' database.")
=======
            print("Database engine not initialized. Please initialize the engine first.")
>>>>>>> 74c2085 (project added up to milestone_2)
