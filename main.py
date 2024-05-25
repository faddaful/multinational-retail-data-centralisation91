# Importing required scripts/modules
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    # Step 1: Initialise DatabaseConnector
    connector = DatabaseConnector('db_creds.yaml')
    print("Step 1: DatabaseConnector initialized successfully.")

    # Step 2: Extract data using DataExtractor
    extractor = DataExtractor()
    user_table_name = 'legacy_users'
    user_data_df = extractor.read_rds_table(connector, user_table_name)
    print("Step 2: Data extracted successfully.")
    print("Sample size of data extracted:")
    print(user_data_df.head())  # Print the first few rows of extracted data

    # Step 3: Clean data using DataCleaning
    cleaner = DataCleaning()
    cleaned_user_data = cleaner.clean_user_data(user_data_df)
    print("Step 3: Data cleaned successfully.")
    print("Sample size of the cleaned data:")
    print(cleaned_user_data.head())  # Print the first few rows of cleaned data

    # Step 4: Upload cleaned data to database
    connector.upload_to_db(cleaned_user_data, 'dim_users')
    print("Step 4: Data uploaded to database successfully.")

if __name__ == "__main__":
    main()
