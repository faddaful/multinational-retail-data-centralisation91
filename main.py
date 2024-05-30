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

    # Extracting the stores data from the API
    headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    try:
        store_number = extractor.list_number_of_stores(number_stores_endpoint, headers)
        print(store_number)
    except Exception as e:
        print('Error retrieving the number of stores: {e}')
    
    store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    try:
        stores_df = extractor.retrieve_stores_data(number_stores_endpoint, store_details_endpoint, headers)
        print("Stores data extracted successfully.")
        print(stores_df.head())  # Display the first few rows of the DataFrame
    except Exception as e:
        print(f"Error retrieving stores data: {e}")


    # Clean store data using the data cleaning method
    try:
        clean_store_data_df = cleaner.clean_store_data(stores_df)
        print("Stores data cleaned successfully.")
    except Exception as e:
        print(f"Error cleaning stores data: {e}")

    # Upload stores data to local database in postgres
    try:
        local_connector.upload_to_db(clean_store_data_df, 'dim_store_details', use_local = True)
        print("Stores data uploaded to database successfully")
    except Exception as e:
        print(f"Error uploading stores data: {e}")


    # Extracting products data from s3 bucket
    s3_address = "s3://data-handling-public/products.csv"
    try:
        products_df = extractor.extract_from_s3(s3_address)
        print("Products data extracted successfully.")
        print(products_df.head())  # Display the first few rows of the DataFrame
    except Exception as e:
        print(f"Error extracting data from S3: {e}")

    # cleaning the products data
    try:
        cleaned_products_df = cleaner.convert_product_weights(products_df)
        print("Product weights converted successfully.")
        print(cleaned_products_df)

        cleaned_products_df = cleaner.clean_products_data(cleaned_products_df)
        print("Product data cleaned successfully.")
        print(cleaned_products_df)
    except Exception as e:
        print(f"Error cleaning product data: {e}")

    # Uploading the products data to the database
    try:
        local_connector.upload_to_db(cleaned_products_df, 'dim_products', use_local=True)
        print("Product data uploaded to local database successfully.")
    except Exception as e:
        print(f"Error uploading product data: {e}")

    # Extract Product orders table from AWD RDS 
    orders_table_name = 'orders_table'
    orders_data_df = extractor.read_rds_table(connector, orders_table_name)
    print("Orders table data extracted successfully.")
    print("First few rows of Orders data extracted:")
    print(orders_data_df.head())

    # Cleaning the orders table data
    try:
        cleaned_orders_data_df = cleaner.clean_orders_table(orders_data_df)
        print(cleaned_orders_data_df)
        print('Orders data cleaned successfully')
    except Exception as e:
        print(f"Error cleaning Orders data: {e}")
    
    # Uploading the orders table data to the database
    try:
        local_connector.upload_to_db(cleaned_orders_data_df, 'orders_table', use_local=True)
        print("Orders data uploaded to local database successfully!")
    except Exception as e:
        print(f"Error Uploading to orders data to local database: {e}")

    # Extracting date event data from s3 bucket
    s3_json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    json_df = extractor.extract_json_from_s3(s3_json_url)
    if json_df is not None:
        print(json_df.head())
    else:
        print("Failed to extract JSON data from S3.")

    # Cleaning the Extracted JSON file from s3
    try:
        cleaned_date_data = cleaner.clean_date_data(json_df)
        print(cleaned_date_data)
    except Exception as e:
        print(f"Error cleaning date event data: {e}")

    # Uploading the date event data to the local database
    try:
        local_connector.upload_to_db(cleaned_date_data, 'dim_date_times', use_local=True)
        print("Date event data uploaded to local database successfully")
    except Exception as e:
        print(f"Error uploading date event data to database: {e}")


if __name__ == "__main__":
    main()

