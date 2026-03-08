import pandas as pd
from src.config import HIGH_AMOUNT_THRESHOLD, HIGH_DURATION_THRESHOLD, HIGH_LOGIN_ATTEMPTS_THRESHOLD, SUSPICIOUS_CHANNELS

def clean_data(df):
    """
    Remove invalid records and standardize data types for downstream processing.

    Drops rows with missing critical values, enforces correct types for dates 
    and amounts, and filters out non-adult customers and duplicates.
    """
    rows_before = df.shape[0] 
    
    
    df = df.dropna(subset=["TransactionAmount", "AccountID"])
    df = df[df["TransactionAmount"] > 0]
    
  
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])
    df["PreviousTransactionDate"] = pd.to_datetime(df["PreviousTransactionDate"])
    
    df["TransactionAmount"] = df["TransactionAmount"].astype(float)
    df["AccountBalance"] = df["AccountBalance"].astype(float)
    df["CustomerAge"] = df["CustomerAge"].astype(int)
    
    
    df = df[(df["CustomerAge"] >= 18) & (df["CustomerAge"] <= 100)]

    df = df.drop_duplicates(subset=["TransactionID"])
    
    rows_after = df.shape[0] 
 
    
    print(f"Cleaning complete. Rows before: {rows_before}, rows after: {rows_after}, dropped: {rows_before - rows_after}")
    
    return df


def enrich_data(df):
    """
    Calculate risk metrics and assign a risk category to each transaction.

    Generates boolean risk flags, computes a cumulative risk score (0-4),
    and maps scores to descriptive labels.
    """
     
    df["is_high_value"] = df["TransactionAmount"] > HIGH_AMOUNT_THRESHOLD
    df["is_suspicious_login"] = df["LoginAttempts"] > HIGH_LOGIN_ATTEMPTS_THRESHOLD
    df["is_atm_transaction"] = df["Channel"].isin(SUSPICIOUS_CHANNELS)
    df["is_long_duration"] = df["TransactionDuration"] > HIGH_DURATION_THRESHOLD
    

    flags = ['is_high_value', 'is_suspicious_login', 'is_atm_transaction', 'is_long_duration']
    risk_map = {0: "low", 1: "medium", 2: "high", 3: "critical", 4: "critical"}
    
    #Convert sum of flags to the corresponding categories
    df['risk_score'] = df[flags].astype(int).sum(axis=1)
    df['risk_label'] = df['risk_score'].map(risk_map)

    print("Enrichment complete. Risk label distribution:")
    print(df['risk_label'].value_counts())

    return df
    