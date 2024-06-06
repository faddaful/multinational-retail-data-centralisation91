import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import boto3
from io import StringIO, BytesIO
from urllib.parse import urlparse

class DataExtractor:
    def __init__(self) -> None:
        pass
    # Method to Extract data from rds_table
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
    
    # Extract product details from S3 bucket in AWS
    def extract_from_s3(self, s3_address):
        s3 = boto3.client('s3')

        # Extract the bucket name and object key from S3
        s3_parts = s3_address.replace("s3://", "").split('/')
        bucket_name = s3_parts.pop(0)
        object_key = '/'.join(s3_parts)

        # Downloading the file from s3
        response = s3.get_object(Bucket = bucket_name, Key = object_key)
        # Reading the file and loading it into a pandas dataframe
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        return df
    
    def extract_json_from_s3(self, s3_json_url):
        # Extract bucket and key from s3_url
        try:
            # Parse the S3 URL
            #parsed_url = urlparse(s3_url)
            bucket = 'data-handling-public'
            key = 'date_details.json'
            # print(parsed_url)
            # print(bucket)
            # print(key)
            
            # Initialize S3 client
            s3 = boto3.client('s3')
            
            # Get the file from S3
            response = s3.get_object(Bucket=bucket, Key=key)
            
            # Read the content of the file
            content = response['Body'].read()
            
            # Load the JSON content into a DataFrame
            df = pd.read_json(BytesIO(content))
            return df
        except Exception as e:
            print(f"Error extracting JSON from S3: {e}")
            return None

if __name__ == "__main__":
    extractor = DataExtractor()
    s3_json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    json_df = extractor.extract_json_from_s3(s3_json_url)
    if json_df is not None:
        print(json_df.head())
    else:
        print("Failed to extract JSON data from S3.")

# Testing the s3 extraction to see if it works
# if __name__ == "__main__":
#     extractor = DataExtractor()
#     s3_address = "s3://data-handling-public/products.csv"
#     try:
#         products_df = extractor.extract_from_s3(s3_address)
#         print("Products data extracted successfully.")
#         print(products_df.head())  # Display the first few rows of the DataFrame
#     except Exception as e:
#         print(f"Error extracting data from S3: {e}")

    
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