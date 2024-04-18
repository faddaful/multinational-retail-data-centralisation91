class DataCleaning:
    @staticmethod
    def clean_user_data(user_data):
        # Dropping rows with NULL values
        cleaned_data = user_data.dropna()
        
        # Additional cleaning steps can be added here
        
        return cleaned_data
    
cleaned_user_data = DataCleaning.clean_user_data(user_data)
