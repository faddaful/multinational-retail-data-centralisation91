# Importing required scripts/modules
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    # Step 1: Initialise DatabaseConnector
    connector = DatabaseConnector('db_creds.yaml')
    print("Step 1: Remote DatabaseConnector initialized successfully.")

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
    # Initialize DatabaseConnector with local credentials
    local_connector = DatabaseConnector('local_db_creds.yaml')
    
    # Step 1: List tables to verify read access
    try:
        tables = local_connector.list_db_tables(use_local=True)
        print("Tables in the local database:", tables)
    except Exception as e:
        print(f"Error listing tables: {e}")

    try:
        local_connector.upload_to_db(cleaned_user_data, 'dim_users', use_local=True)
        print("Data uploaded to local database successfully.")
    except Exception as e:
        print(f"Error uploading data: {e}")
    # connector.upload_to_db(cleaned_user_data, 'dim_users')
    # print("Step 4: Data uploaded to database successfully.")


    # Extracting the pdf data
    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    try:
        pdf_data_df = extractor.retrieve_pdf_data(pdf_link)
        print("PDF data extracted successfully.")
    except Exception as e:
        print(f"Error extracting data from PDF: {e}")

    # Clean card data using the data cleaning method DataCleaning
    try:
        cleaned_pdf_data_df = cleaner.clean_card_data(pdf_data_df)
        print("Card data cleaned successfully.")
    except Exception as e:
        print(f"Error cleaning card data: {e}")

    # Upload cleaned card data to local database in postgres
    try:
        local_connector.upload_to_db(cleaned_pdf_data_df, 'dim_card_details', use_local=True)
        print("Card data uploaded to local database successfully.")
    except Exception as e:
        print(f"Error uploading card data: {e}")

if __name__ == "__main__":
    main()

