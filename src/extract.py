import pandas as pd


def extract_transactions(filepath):
    df = pd.read_csv(filepath)
    return df


def profile_dataframe(df):
    """
    Checking correctness of the data set and the data types of the columns
    Checking on possible missing values and adequacy of data and column data types

    """

    print("--- Shape ---")
    print(df.shape)
    
    print("\n--- Data Types ---")
    print(df.dtypes)
    
    print("\n--- Missing Values ---")
    print(df.isnull().sum())
    
    print("\n--- TransactionType Counts ---")
    print(df['TransactionType'].value_counts())
    
    print("\n--- Channel Counts ---")
    print(df['Channel'].value_counts())
    
    print("\n--- CustomerOccupation Counts ---")
    print(df['CustomerOccupation'].value_counts())

    print("\n--- CustomerAge Range ---")
    print(df['CustomerAge'].describe())



