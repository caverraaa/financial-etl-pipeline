import csv

path = 'data/bank_transactions_data_2.csv' 
with open(path, mode='r') as f:
    reader = csv.reader(f)

    header = next(reader)
    print(f"Columns: {header}")

    total_rows = sum(1 for _ in reader)
    print(f"Total rows {total_rows}")

    


   
        
        

        