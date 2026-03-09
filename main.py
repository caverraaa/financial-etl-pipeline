from src.config import DATASET_PATH, SQL_PATH 
from src.extract import extract_transactions, profile_dataframe
from src.transform import clean_data, enrich_data
from src.load import get_engine, get_connection, load_transactions, verify_load, run_query



df_raw = extract_transactions(DATASET_PATH)
df_clean = clean_data(df_raw)
df_enriched = enrich_data(df_clean)

print("\n")
print("Transactions with risk_score >= 3")
print(df_enriched[df_enriched["risk_score"] >= 3])

engine = get_engine()
conn = get_connection()
load_transactions(df_enriched, engine)
verify_load(conn)
conn.close()

with open(SQL_PATH, "r") as f:
    sql_content = f.read()

queries = sql_content.split("-- split")

for i, sql in enumerate(queries, 1):
    if sql.strip(): 
        run_query(engine, sql, f"Query #{i}")






    


    

    


   
        
        

        