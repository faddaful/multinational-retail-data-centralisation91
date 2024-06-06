import re
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
        #stores_df = stores_df.dropna(subset=['opening_date'])
        # Convert date columns to datetime format
        #stores_df['opening_date'] = pd.to_datetime(stores_df['opening_date'], errors = 'coerce')
       
        stores_df['store_type'] = stores_df['store_type'].astype(str)
        stores_df['store_type'] = stores_df['store_type'].str.strip()


        return stores_df
    
    # Cleaning products data
    # To clean the products data more thoroughly
    def clean_products_data(self, df):
        # Remove rows with any NULL values
        df = df.dropna()
        
        # Remove duplicates
        df = df.drop_duplicates()

        # Additional cleaning steps to be added in the future
        # Ensure that price is a float and remove any non-numeric characters
        print(df.columns.tolist())
        df['product_price'] = df['product_price'].replace('[^\d.]', '', regex=True).astype(float)

        return df
    
    # Converting the products weight
    def convert_product_weights(self, df):

        # Function to convert weight to kg
        def to_kg(weight):
            try:
                if isinstance(weight, str):
                    weight = weight.lower().strip()
                    if 'kg' in weight:
                        return float(weight.replace('kg', '').strip())
                    elif 'g' in weight:
                        return float(weight.replace('g', '').strip()) / (1000.0)
                    elif 'g' in weight:
                        return float(weight.replace('g', '').strip())
                    elif 'g' in weight:
                        return float((parts[0].strip()) * float(parts[1].strip()))
                    elif 'ml' in weight:
                        return float(weight.replace('ml', '').strip()) / 1000.0  # Assuming 1:1 ratio of ml to g
                    elif 'oz' in weight:
                        return float(weight.replace('oz', '').strip()) * 0.0283495  # Convert ounces to kg
                    else:
                        # Handle complex cases like '12 x 100g'
                        match = re.match(r'(\d+)\s*x\s*(\d+)', weight)
                        if match:
                            parts = match.groups()
                            return (float(parts[0].strip()) * float(parts[1].strip())) / 1000.0
                        else:
                            return float(weight)  # Assuming it's already in kg
                elif isinstance(weight, (int, float)):
                    return weight
                else:
                    raise ValueError(f"Unexpected weight format: {weight}")
            except ValueError as e:
                print(f"Error converting weight: {weight} -> {e}")
                return None

        # Apply the function to the weight column
        df['weight'] = df['weight'].apply(to_kg)

        # Remove rows where weight conversion failed
        df = df.dropna(subset=['weight'])

        return df
    # Cleaning the orders
    def clean_orders_table(self, orders_df):
        # Drop columns that contain None values
        orders_df = orders_df.drop(columns = ['first_name', 'last_name', '1'])
        # Add more cleaning steps in the future

        return orders_df
    
    def clean_date_data(self, df):
        # Handle null values
        df.dropna(inplace=True)
        
        # Drop any rows with invalid dates
        # Add specific cleaning steps here
        
        return df

# Test if cleaning the orders table will work
# if __name__ == "__main__":

#     extractor = DataExtractor()
#     connector = DatabaseConnector('db_creds.yaml')
#     orders_table_name = 'orders_table'
#     orders_data_df = extractor.read_rds_table(connector, orders_table_name)

#     # Create an instance of DataCleaning
#     cleaner = DataCleaning()

#     # Call the clean_orders_data method and print the result
#     cleaned_orders_data = cleaner.clean_orders_table(orders_data_df)
#     print("Cleaned Orders Data:")
#     print(cleaned_orders_data)

# Testing the weight conversion and the cleaning function
# if __name__ == "__main__":
#     cleaner = DataCleaning()
#     extractor = DataExtractor()
#     s3_address = "s3://data-handling-public/products.csv"
#     try:
#         products_df = extractor.extract_from_s3(s3_address)
#         print("Products data extracted successfully.")
#         print(products_df.head())  # Display the first few rows of the DataFrame
#     except Exception as e:
#         print(f"Error extracting data from S3: {e}")
#     try:
#         cleaned_products_df = cleaner.convert_product_weights(products_df)
#         print("Product weights converted successfully.")
#         print(cleaned_products_df)

#         cleaned_products_df = cleaner.clean_products_data(cleaned_products_df)
#         print("Product data cleaned successfully.")
#         print(cleaned_products_df)
#     except Exception as e:
#         print(f"Error cleaning product data: {e}")

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