import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests

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
    
    
    
    def list_number_of_stores(self, endpoint, headers):
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            return response.json()['number_stores']
        else:
            response.raise_for_status()
    

    def retrieve_stores_data(self, number_stores_endpoint, store_details_endpoint, headers):
        number_of_stores = self.list_number_of_stores(number_stores_endpoint, headers)
        all_stores_data = []

    
        for store_number in range(0, number_of_stores - 1):
            response = requests.get(store_details_endpoint.format(store_number=store_number), headers=headers)
            if response.status_code == 200:
                store_data = response.json()
                #print(store_data)
                all_stores_data.append(store_data)
            else:
                response.raise_for_status()

        stores_df = pd.DataFrame(all_stores_data)
        return stores_df
    
# Test to see if the number of stores will be printed
# if __name__ == "__main__":
    
#     extractor = DataExtractor()
#     headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
#     number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
  

#     try:
#         store_number = extractor.list_number_of_stores(number_stores_endpoint, headers)
#         print(store_number)
#     except Exception as e:
#         print('Error retrieving the number of stores: {e}')
    
#     store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
   
#     try:
#         stores_df = extractor.retrieve_stores_data(number_stores_endpoint, store_details_endpoint, headers)
#         print("Stores data extracted successfully.")
#         print(stores_df.head())  # Display the first few rows of the DataFrame
#     except Exception as e:
#         print(f"Error retrieving stores data: {e}")



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