import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def __init__(self, db_creds):
        self.db_creds = db_creds

    def read_db_creds(self):
        with open(self.db_creds, 'r') as file:
            creds = yaml.safe_load(file)
        return creds
    
    def init_db_engine(self, use_local=False):
        creds = self.read_db_creds()
        if use_local:
            engine = create_engine(
                f"postgresql://{creds['LOCAL_USER']}:{creds['LOCAL_PASSWORD']}@{creds['LOCAL_HOST']}:{creds['LOCAL_PORT']}/{creds['LOCAL_DATABASE']}"
            )
        else:
            engine = create_engine(
                f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            )
        return engine
        # engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        # return engine
    
    def list_db_tables(self, use_local = False):
        engine = self.init_db_engine(use_local)
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables
    
    def upload_to_db(self, df, table_name, use_local = False):
        engine = self.init_db_engine(use_local)
        try:
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Data successfully uploaded to {table_name}")
        except Exception as e:
            print(f"Error uploading data to {table_name}: {e}")


# Testing that the read_db_creds return the dictionary containing the database credentials
# if __name__ == "__main__":
#     connector = DatabaseConnector('db_creds.yaml')
#     tables = connector.list_db_tables()
#     print(tables)


# if __name__ == "__main__":
#     from data_extraction import DataExtractor

#     # Assuming we have succesfully extracted data into a dataframe
#     extractor = DataExtractor()
#     connector = DatabaseConnector('db_creds.yaml')
#     sales_data_df = extractor.read_rds_table(connector, 'legacy_users')

#     # Upload sales data to the dim_users table
#     connector.upload_to_db(sales_data_df, 'dim_users')