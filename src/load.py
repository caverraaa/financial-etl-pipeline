from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import os
import psycopg2
import psycopg2.extensions
import pandas as pd

def get_engine() -> Engine:
    
    """Get engine for automatic table managing"""

    load_dotenv()
    try:
        user = os.environ["DB_USER"]
        password = os.environ["DB_PASSWORD"]
        host = os.environ["DB_HOST"]
        port = os.environ["DB_PORT"]
        db = os.environ["DB_NAME"]
        return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
    except KeyError as e:
        raise KeyError(f"Missing environment variable: {e}.")



def get_connection() -> psycopg2.extensions.connection:

    """Establish connection to the database."""

    load_dotenv()
    try:
        return psycopg2.connect(
            host=os.environ["DB_HOST"],
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            port=os.environ["DB_PORT"]
        )
    except psycopg2.OperationalError as e:
        raise ConnectionError(f"Could not connect to PostgreSQL: {e}")



def load_transactions(df: pd.DataFrame, engine: Engine) -> None:
    n_rows = df.shape[0]
    df.to_sql('transactions', engine, if_exists='replace', index=False)
    print(f"Loaded {n_rows} rows into 'transactions' table")



def verify_load(conn: psycopg2.extensions.connection) -> None:
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

def run_query(engine: Engine, sql: str, label: str) -> None:
    os.makedirs("reports", exist_ok=True)
    df = pd.read_sql_query(sql, engine)

    report_name = f"reports/{label.replace(' ', '_')}.csv"
    df.to_csv(report_name, index=False)

    print(f"\n--- Report saved: {report_name} ---")
    print(df.head(5).to_string())
