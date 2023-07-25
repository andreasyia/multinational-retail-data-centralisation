import yaml
import sqlalchemy.exc
from sqlalchemy import create_engine, inspect
import pandas as pd
import getpass

class DatabaseConnector():

    def read_db_creds(self, creds_file):
        ''' 
        The function 'read_db_creds' take as a parameter the 'creds_file' and returns the 
        credentials for connecting to the database.
        '''
        with open(creds_file, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials
    
    def init_db_engine(self, creds):
        ''' 
        The 'init_db_engine' takes 'creds' as a parameter, a dictionary containing the 
        database credentials. The 'db_url' constructs the database connection URL using the 'creds' and the 
        'engine' which uses the 'create_engine' method to create the engine using the connection URL. If the
        engine is successfully created, it prints "Database engine initialized" and if there is an error, it 
        prints the error.
        '''
        try:
            db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            engine = create_engine(db_url)
            print("Database engine initialized!")
            return engine
        except (sqlalchemy.exc.SQLAlchemyError, Exception) as error:
            print("Error initializing database engine:", error)
    
    def list_db_tables(self, engine):
        ''' 
        The 'list_db_tables' takes only one parameter 'engine'. In this function, there are conditional 
        if-else statements along with try, except blocks for handling the errors. The inspector was used to 
        retrieve the list of the table names present in the connected database and the code then prints the 
        table names one by one using a loop.
        '''
        if engine:      
            try:
                inspector = sqlalchemy.inspect(engine)
                table_names = inspector.get_table_names()
                print("Tables in  the database:")
                for table_name in table_names:
                    print(table_name)
            except (sqlalchemy.exc.SQLAlchemyError, Exception) as error:
                print("Error listing database tables:", error)
        else:
            print("Database engine not initialized. Please initialize the engine first.")

    def upload_to_db(self, df, table_name):
        ''' 
        The upload_to_db function is used to upload the data to a database in Postgresql. For security 
        reasons the `getpass` was used to provide the password for connecting to the database.Then, the 
        self.engine is requesting a connection to the database using 'sqlalchemy.create_engine()' in 
        Postgresql and then, uploads the data with the specified name given for the table.
        '''
        password = getpass.getpass("Enter your password: ")
        sql_connection = (f'postgresql://postgres:{password}@localhost/sales_data')
        self.engine = sqlalchemy.create_engine(sql_connection) 
        if self.engine:
            try:
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                print("Data uploaded to database table successfully!")
            except (sqlalchemy.exc.SQLAlchemyError, Exception) as error:
                print("Error uploading data to database:", error)
        else:
            print("Database engine not initialized. Please initialize the engine first.")



if __name__ == "__main__":
    connector = DatabaseConnector()
    creds_file = 'db_creds.yaml'
    creds = connector.read_db_creds(creds_file)
    engine =  connector.init_db_engine(creds)