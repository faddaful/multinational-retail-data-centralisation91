import pandas as pd
from database_utils import DatabaseConnector
import tabula

class DataExtractor:
    def __init__(self) -> None:
        pass

    def read_rds_table(self, connector, table_name):
        engine = connector.init_db_engine()
        query = f'SELECT * FROM {table_name};'
        df = pd.read_sql_query(query, engine)
        return df
    
    # Method to retrieve pdf link for a url
    def retrieve_pdf_data (self, link):
        # Extracting data from the pdf
        dfs = tabula.read_pdf(link, pages = 'all', multiple_tables = True)
        # Combine all tables into a single dataframe
        combined_df = pd.concat(dfs, ignore_index = True)
        return combined_df
    
    def list_db_tables(self, connector):
        return connector.list_db_tables()
    

# Usage and testing the pdf extraction
# if __name__ == "__main__":
#     extractor = DataExtractor()
#     pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    
#     try:
#         pdf_data_df = extractor.retrieve_pdf_data(pdf_link)
#         print("PDF data extracted successfully.")
#         print("Sample data extracted from PDF:")
#         print(pdf_data_df.head())  # Print the first few rows of the extracted data
#     except Exception as e:
#         print(f"Error extracting data from PDF: {e}")

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