from sqlalchemy import create_engine, inspect
import getpass
import pandas as pd
import sqlalchemy.exc
import yaml

class DatabaseConnector():
    """
    The class 'DatabaseConnector' facilitates interactions with a PostgreSQL database. It includes several methods 
    to handle various aspects of connecting to the database, listing tables, and uploading data from a Pandas 
    DataFrame to a database table.

    Methods
    -------
    read_db_creds(self, creds_file)
    init_db_engine(self, creds)
    list_db_tables(self, engine)
    upload_to_db(self, df, table_name)
    """

    
    def read_db_creds(self, creds_file):
        ''' 
        The 'read_db_creds' method reads the content of the YAML file which contains the database 
        credentials.

            Parameters:
                    creds_file(String): Specifies the path to the YAML file

            Returns:
                    credentials(Dictionary): Contains the database connection information
        '''
        with open(creds_file, 'r') as file:
            credentials = yaml.safe_load(file)

        return credentials
    
    def init_db_engine(self, creds):
        ''' 
        The 'init_db_engine' method creates an engine which acts as a connector to a database.

        If the engine is successfully created, it prints "Database engine initialized" and if there is an 
        error, it prints the error.

            Parameters:
                    creds(Dictionary): Contains the required information for database connection
                
            Returns:
                    engine(SQLAlchemy Engine object): Can be used to interacct with thee PostgreSQL database
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
        The 'list_db_tables' method is used to retrieve and print the names of the tables present in the connected 
        database using the provided SQLAlchemy Engine.

            Parameters:
                    engine(SQLAlchemy Engine object): Can be used to interact with the PostgreSQL database
            Returns:
                    None
        '''
        if engine:      
            try:
                inspector = sqlalchemy.inspect(engine)
                table_names = inspector.get_table_names() #list the table names
                print("Tables in  the database:")
                for table_name in table_names:
                    print(table_name)
            except (sqlalchemy.exc.SQLAlchemyError, Exception) as error:
                print("Error listing database tables:", error)
        else:
            print("Database engine not initialized. Please initialize the engine first.")

    def upload_to_db(self, df, table_name):
        ''' 
        The 'upload_to_db' method is used to upload data from a Pandas DataFrame to a PostgreSQL database 
        table. 

            Parameters:
                    df(Dataframe): DataFrame containing the data to be uploaded
                    table_name(String): Specify the name of the database table
            Returns:
                    None
        '''
        password = getpass.getpass("Enter your password: ") # The function promts the user to tenter the databse password securely
        sql_connection = (f'postgresql://postgres:{password}@localhost/sales_data') # It constructs the database connection URL
        self.engine = sqlalchemy.create_engine(sql_connection) # It creates an SQLAlchemy Engine
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