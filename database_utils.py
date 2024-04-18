import yaml
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import URL

class DatabaseConnector:
    @staticmethod
    def read_db_creds():
        with open("db_creds.yaml", 'r') as file:
            return yaml.safe_load(file)


    @staticmethod
    def init_db_engine():
        creds = DatabaseConnector.read_db_creds()
        if creds:
            db_url = sqlalchemy.engine.url.URL(
                drivername='postgresql',
                username=creds['RDS_USER'],
                password=creds['RDS_PASSWORD'],
                host=creds['RDS_HOST'],
                port=creds['RDS_PORT'],
                database=creds['RDS_DATABASE']
            )
            return create_engine(db_url)
        else:
            return None

    @staticmethod
    def list_db_tables():
        engine = DatabaseConnector.init_db_engine()
        if engine:
            with engine.connect() as connection:
                tables = connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
                return [table[0] for table in tables]
        else:
            return None

    @staticmethod
    def upload_to_db(dataframe, table_name):
        engine = DatabaseConnector.init_db_engine()
        if engine:
            try:
                dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
                print(f"Data uploaded to table '{table_name}' successfully.")
            except Exception as e:
                print(f"Error uploading data to table '{table_name}': {str(e)}")
        else:
            print("Failed to upload data. Database engine is not initialized.")

# Now upload the cleaned data to the database
db_connector = DatabaseConnector()
table_name = db_connector.list_db_tables()[0]
db_connector.upload_to_db(cleaned_user_data, 'dim_users')
