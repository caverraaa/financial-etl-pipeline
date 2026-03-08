from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import psycopg2

def get_engine():
    
    """Get engine for automatic table managing"""

    load_dotenv()
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    host = os.environ["DB_HOST"]
    port = os.environ["DB_PORT"]
    db = os.environ["DB_NAME"]
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")



def get_connection():

    """Establish connection to the database."""

    load_dotenv()
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=os.environ["DB_PORT"]
    )



def load_transactions(df, engine):
    n_rows = df.shape[0]
    df.to_sql('transactions', engine, if_exists='replace', index=False)
    print(f"Loaded {n_rows} rows into 'transactions' table")



def verify_load(conn):
    """
    Verify data integrity by running SQL queries against the database.
    """

    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM transactions;")
    total_rows = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM transactions WHERE risk_label IN ('high', 'critical');")
    high_risk_rows = cur.fetchone()[0]
    
    print(f"Verification: {total_rows} rows in database.")
    print(f"High-risk transactions: {high_risk_rows}")
    
    cur.close()