import os
import pandas as pd
from sqlalchemy.engine import Engine


QUERIES = {
    "By Channel": """
        SELECT
            "Channel",
            COUNT(*) AS transaction_count,
            ROUND(SUM("TransactionAmount")::numeric, 2) AS total_amount,
            ROUND(AVG("TransactionAmount")::numeric, 2) AS avg_amount
        FROM transactions
        GROUP BY "Channel"
        ORDER BY total_amount DESC
    """,
    "High Risk": """
        SELECT
            "TransactionID", "AccountID", "TransactionAmount", "Channel",
            "LoginAttempts", "TransactionDuration", "risk_score", "risk_label"
        FROM transactions
        WHERE risk_label IN ('high', 'critical')
        ORDER BY "risk_score" DESC, "TransactionAmount" DESC
    """,
    "Repeat Suspicious": """
        SELECT
            "AccountID",
            COUNT(*) AS suspicious_count,
            ROUND(SUM("TransactionAmount")::numeric, 2) AS total_amount
        FROM transactions
        WHERE risk_score >= 2
        GROUP BY "AccountID"
        HAVING COUNT(*) > 1
        ORDER BY suspicious_count DESC
    """,
    "Above Average": """
        SELECT *
        FROM transactions
        WHERE "TransactionAmount" > (SELECT AVG("TransactionAmount") FROM transactions)
        ORDER BY "TransactionAmount" ASC
    """,
    "Monthly Trend": """
        SELECT
            DATE_TRUNC('month', "TransactionDate") AS month,
            COUNT(*) AS transaction_count,
            ROUND(SUM("TransactionAmount")::numeric, 2) AS total_amount,
            ROUND(AVG("TransactionAmount")::numeric, 2) AS avg_amount
        FROM transactions
        GROUP BY month
        ORDER BY month
    """,
    "Channel Risk Breakdown": """
        SELECT
            "Channel",
            risk_label,
            COUNT(*) AS transaction_count,
            ROUND(SUM("TransactionAmount")::numeric, 2) AS total_amount,
            ROUND(AVG("TransactionAmount")::numeric, 2) AS avg_amount
        FROM transactions
        GROUP BY "Channel", risk_label
        ORDER BY "Channel", risk_label
    """,
}


def _compute_kpis(df: pd.DataFrame) -> pd.DataFrame:
    total_volume = round(df["TransactionAmount"].sum(), 2)
    avg_amount = round(df["TransactionAmount"].mean(), 2)
    total_transactions = len(df)
    high_risk_count = int((df["risk_label"].isin(["high", "critical"])).sum())
    high_risk_pct = round(high_risk_count / total_transactions * 100, 2)

    channel_breakdown = (
        df.groupby("Channel")["TransactionAmount"]
        .agg(count="count", total=sum)
        .reset_index()
    )
    channel_lines = " | ".join(
        f"{row['Channel']}: {int(row['count'])} txns (${row['total']:,.2f})"
        for _, row in channel_breakdown.iterrows()
    )

    kpis = pd.DataFrame([
        {"Metric": "Total Transaction Volume ($)",  "Value": total_volume},
        {"Metric": "Average Transaction Amount ($)", "Value": avg_amount},
        {"Metric": "Total Transactions",             "Value": total_transactions},
        {"Metric": "High-Risk Transactions",         "Value": high_risk_count},
        {"Metric": "High-Risk Rate (%)",             "Value": high_risk_pct},
        {"Metric": "Channel Breakdown",              "Value": channel_lines},
    ])
    return kpis


def export_powerbi(
    df_enriched: pd.DataFrame,
    engine: Engine,
    dq_report: pd.DataFrame,
    output_path: str = "reports/powerbi_report.xlsx",
) -> None:
    """
    Write a multi-sheet Excel workbook ready for import into Power BI Desktop.

    Sheets:
        Transactions        — full enriched dataset
        KPIs                — high-level summary metrics
        Data Quality        — null rates, records dropped, outlier counts
        By Channel          — volume by payment channel
        High Risk           — high and critical risk transactions
        Repeat Suspicious   — accounts with repeat suspicious activity
        Above Average       — transactions above average amount
        Monthly Trend       — volume trend by month
        Channel Risk Breakdown — risk distribution per channel
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    kpis = _compute_kpis(df_enriched)

    print("Running Power BI export queries...")
    query_results = {}
    for sheet_name, sql in QUERIES.items():
        query_results[sheet_name] = pd.read_sql_query(sql, engine)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_enriched.to_excel(writer, sheet_name="Transactions", index=False)
        kpis.to_excel(writer, sheet_name="KPIs", index=False)
        dq_report.to_excel(writer, sheet_name="Data Quality", index=False)
        for sheet_name, df in query_results.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Power BI workbook saved: {output_path}")
