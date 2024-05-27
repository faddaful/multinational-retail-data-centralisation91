import pandas as pd
from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self) -> None:
        pass

    def read_rds_table(self, connector, table_name):
        engine = connector.init_db_engine()
        query = f'SELECT * FROM {table_name};'
        df = pd.read_sql_query(query, engine)
        return df
    
    def list_db_tables(self, connector):
        return connector.list_db_tables()
    

# To test if everything is working as it should work
# if __name__ == "__main__":

#     connector = DatabaseConnector('db_creds.yaml')
#     extractor = DataExtractor()
#     tables = extractor.list_db_tables(connector)
#     print("Tables in the database:", tables)
#     user_table_name = 'legacy_users'  # To print the actual user data table name in the database
#     user_data_df = extractor.read_rds_table(connector, user_table_name)
#     print("User data DataFrame:")
#     print(user_data_df)
#     column_names = user_data_df.columns
#     print(column_names)