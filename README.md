# ETL Pipeline — Bank Transaction Risk Analysis

This project implements a full ETL pipeline that ingests a bank transactions dataset from Kaggle, cleans and enriches it with risk scoring logic, loads it into a PostgreSQL database, and runs SQL-based analytical queries. The output is a set of CSV reports covering transaction volume by channel, high-risk transaction detection, repeat suspicious activity, and above-average transaction filtering.

---

## Pipeline Architecture

```
CSV (Kaggle) → Extract (pandas) → Transform (clean + enrich) → Load (PostgreSQL) → SQL Reports (CSV)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data processing | Python 3, pandas |
| Database | PostgreSQL |
| DB connectivity | psycopg2, SQLAlchemy |
| Credential security | python-dotenv |

---

## Feature Engineering

| Feature | Logic |
|---|---|
| `is_high_value` | `TransactionAmount > 500.0` |
| `is_suspicious_login` | `LoginAttempts > 3` |
| `is_atm_transaction` | `Channel == "ATM"` |
| `is_long_duration` | `TransactionDuration > 200` |
| `risk_score` | Sum of the four boolean flags above (0–4) |
| `risk_label` | `0 → low`, `1 → medium`, `2 → high`, `3–4 → critical` |

---

## Key Findings

**Q1 — Total transaction volume by channel:**

| Channel | Transaction Count | Total Amount | Avg Amount |
|---|---|---|---|
| ATM | 833 | 256,331.43 | 307.72 |
| Branch | 868 | 250,183.00 | 288.23 |
| Online | 811 | 241,041.14 | 297.21 |

ATM has the highest total transaction volume at $256,331.43.

**Q3 — Accounts with repeat suspicious activity (risk_score ≥ 2, more than one occurrence):**

73 accounts were identified with repeat suspicious transactions.

**Q4 — Transactions above average amount:**

952 transactions exceed the dataset's average transaction amount.

---

## Project Structure

```
etl-pipeline/
├── data/                          ← not committed, download from Kaggle
├── src/
│   ├── config.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── queries.sql
├── main.py
├── requirements.txt
├── .env                           ← not committed, see .env.example
└── .env.example
```

---

## Setup

1. Clone the repository:
   ```
   git clone https://github.com/caverraaa/etl-pipeline.git
   cd etl-pipeline
   ```

2. Create and activate a virtual environment:
   ```
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Copy `.env.example` to `.env` and fill in your PostgreSQL credentials:
   ```
   cp .env.example .env
   ```

5. Download the dataset from Kaggle and place the CSV file in the `data/` directory.

6. Run the pipeline:
   ```
   python3 main.py
   ```
