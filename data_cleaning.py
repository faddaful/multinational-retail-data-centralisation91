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

# Testing if it will work as expected.
if __name__ == "__main__":
    # Create an instance of DataExtractor to extract user data
    extractor = DataExtractor()
    connector = DatabaseConnector('db_creds.yaml')
    user_table_name = 'legacy_users'
    user_data_df = extractor.read_rds_table(connector, user_table_name)

    # Create an instance of DataCleaning
    cleaner = DataCleaning()

    # Call the clean_user_data method and print the result
    cleaned_user_data = cleaner.clean_user_data(user_data_df)
    print("Cleaned User Data:")
    print(cleaned_user_data)