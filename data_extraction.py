import pandas as pd

class DataExtractor:
    @staticmethod
    def read_data_from_rds(database_connector, table_name):
        engine = database_connector.init_db_engine()
        if engine:
            query = f"SELECT * FROM {table_name}"
            return pd.read_sql_query(query, engine)
        else:
            print("Failed to establish connection to the database.")
            return None

    @staticmethod
    def read_rds_table(database_connector, table_name):
        if table_name in database_connector.list_db_tables():
            return database_connector.read_data_from_rds(table_name)
        else:
            print(f"Table '{table_name}' not found in the database.")
            return None
        
data_extractor = DataExtractor()
user_data = data_extractor.read_rds_table(db_connector, table_name)
