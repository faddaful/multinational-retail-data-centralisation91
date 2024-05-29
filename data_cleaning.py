import pandas as pd
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, user_data_df):
        # Drop rows with NULL values
        user_data_df.dropna(inplace=True)

        # Convert date columns to datetime format
        date_columns = ['date_of_birth', 'join_date']  # Update with date columns and date of birth
        for column in date_columns:
            user_data_df[column] = pd.to_datetime(user_data_df[column], errors='coerce')

        # Drop rows with invalid dates
        user_data_df = user_data_df[user_data_df[date_columns].notnull().all(axis=1)]

        # Additional cleaning steps as will be added here later

        return user_data_df
    
    def clean_card_data(self, card_data_df):
        card_data_df.dropna(inplace = True)
        #fill or drop rows with null values
        card_data_df = card_data_df.dropna(subset=['card_number', 'expiry_date', 'card_provider'])

        # Fix formatting issues, e.g., strip whitespace from string columns
        card_data_df = card_data_df.map(lambda x: x.strip() if isinstance(x, str) else x)

        return card_data_df
    
    def clean_store_data(self, stores_df):
        # Dropping rows with null values
        stores_df.dropna(inplace = True)
        # Drop rows where 'opening_date' conversion failed
        stores_df = stores_df.dropna(subset=['opening_date'])
        # Convert date columns to datetime format
        #stores_df['opening_date'] = pd.to_datetime(stores_df['opening_date'], errors = 'coerce')
       
        stores_df['store_type'] = stores_df['store_type'].astype(str)
        stores_df['store_type'] = stores_df['store_type'].str.strip()


        return stores_df


# Testing phase the cleaned pdf data
# if __name__ == "__main__":
#     from data_extraction import DataExtractor

#     extractor = DataExtractor()
#     pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    
#     try:
#         pdf_data_df = extractor.retrieve_pdf_data(pdf_link)
#         print("PDF data extracted successfully.")
#         print("Sample data extracted from PDF:")
#         print(pdf_data_df.head())  # Print the first few rows of the extracted data
        
#         # Initialize DataCleaning and clean the extracted PDF data
#         cleaner = DataCleaning()
#         cleaned_pdf_data_df = cleaner.clean_card_data(pdf_data_df)
#         print("Card data cleaned successfully.")
#         print("Sample cleaned data:")
#         print(cleaned_pdf_data_df.head())  # Print the first few rows of the cleaned data
#     except Exception as e:
#         print(f"Error: {e}")

# Testing if it will work as expected.
# if __name__ == "__main__":
#     # Create an instance of DataExtractor to extract user data
#     extractor = DataExtractor()
#     connector = DatabaseConnector('db_creds.yaml')
#     user_table_name = 'legacy_users'
#     user_data_df = extractor.read_rds_table(connector, user_table_name)

#     # Create an instance of DataCleaning
#     cleaner = DataCleaning()

#     # Call the clean_user_data method and print the result
#     cleaned_user_data = cleaner.clean_user_data(user_data_df)
#     print("Cleaned User Data:")
#     print(cleaned_user_data)