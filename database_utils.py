import yaml
import sqlalchemy.exc
from sqlalchemy import create_engine, inspect
import pandas as pd
import getpass

creds_file = '/Users/andreasyianni/Desktop/multinational-retail-data-centralisation/db_creds.yaml'

class DatabaseConnector():

    def __init__(self, creds_file):
        self.creds = self.read_db_creds(creds_file)
        self.engine = None
        self.connection = None


    def read_db_creds(self, creds_file):
        with open(creds_file, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials
    
    def init_db_engine(self):
        try:
            db_url = f"postgresql://{self.creds['RDS_USER']}:{self.creds['RDS_PASSWORD']}@{self.creds['RDS_HOST']}:{self.creds['RDS_PORT']}/{self.creds['RDS_DATABASE']}"
            self.engine = sqlalchemy.create_engine(db_url)
            print("Database engine initialized!")
        except (sqlalchemy.exc.SQLAlchemyError, Exception) as error:
            print("Error initializing database engine:", error)

        return self.engine
    
    def list_db_tables(self):
        if self.engine:
            try:
                inspector = sqlalchemy.inspect(self.engine)
                table_names = inspector.get_table_names()
                # return table_names
                print("Tables in  the database:")
                for table_name in table_names:
                    print(table_name)
            except (sqlalchemy.exc.SQLAlchemyError, Exception) as error:
                print("Error listing database tables:", error)
        else:
            print("Database engine not initialized. Please initialize the engine first.")

    def upload_to_db(self, df, table_name):
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
            print("Database engine not initialized. Please initialize the engine first.")

connector = DatabaseConnector(creds_file)

# Uploading(dim_users, dim_card_details, dim_store_details, dim_products) all the same way
# table_to_upload = 'dim_users'
# # connector.upload_to_db(df_user_1, table_to_upload)
# print("Data uploaded to the 'dim_users' table of the 'sales_data' database.")