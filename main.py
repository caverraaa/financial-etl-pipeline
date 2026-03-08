from src.config import FILE_PATH
from src.extract import extract_transactions, profile_dataframe
from src.transform import clean_data, enrich_data




df_raw = extract_transactions(FILE_PATH)
df_clean = clean_data(df_raw)
df_enriched = enrich_data(df_clean)

print("\n")
print("Transactions with risk_score >= 3")
print(df_enriched[df_enriched["risk_score"] >= 3])




    


    

    


   
        
        

        