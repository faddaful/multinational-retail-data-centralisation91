from data_extraction import data_extractor
user_data = data_extractor.read_rds_table(db_connector, table_name)

class DataCleaning:
    @staticmethod
    def clean_user_data(user_data):
        # Dropping rows with NULL values
        cleaned_data = user_data.dropna()
        
        # Additional cleaning steps can be added here
        
        return cleaned_data
    
cleaned_user_data = DataCleaning.clean_user_data(user_data)
