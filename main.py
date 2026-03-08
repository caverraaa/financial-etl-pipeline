import csv
from src.extract import extract_transactions, profile_dataframe

FILE_PATH = 'data/bank_transactions_data_2.csv'

raw_df = extract_transactions(FILE_PATH)

profile_dataframe(raw_df)

print(raw_df.head(5))


    


    

    


   
        
        

        