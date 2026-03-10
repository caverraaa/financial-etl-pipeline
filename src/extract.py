import pandas as pd


def extract_transactions(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    return df


def profile_dataframe(df: pd.DataFrame) -> None:
    """
    Check correctness and adequacy of the dataset
    
    """

    print("--- Shape ---")
    print(df.shape)
    
    print("\n--- Data Types ---")
    print(df.dtypes)
    
    print("\n--- Missing Values ---")
    print(df.isnull().sum())
    
    print("\n--- TransactionType Counts ---")
    print(df["TransactionType"].value_counts())
    
    print("\n--- Channel Counts ---")
    print(df["Channel"].value_counts())
    
    print("\n--- CustomerOccupation Counts ---")
    print(df["CustomerOccupation"].value_counts())

    print("\n--- CustomerAge Range ---")
    print(df["CustomerAge"].describe())



