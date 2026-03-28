import pandas as pd


def generate_dq_report(df_raw: pd.DataFrame, df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Compare raw and cleaned DataFrames to produce a data quality summary.

    Returns a DataFrame with one row per column covering null rates,
    plus a summary section with record-level stats (duplicates removed,
    rows dropped, etc.).
    """
    rows_raw = len(df_raw)
    rows_clean = len(df_clean)
    rows_dropped = rows_raw - rows_clean
    duplicates_removed = df_raw.duplicated(subset=["TransactionID"]).sum()

    # Per-column null analysis on the raw data
    null_counts = df_raw.isnull().sum()
    null_rates = (null_counts / rows_raw * 100).round(2)

    column_stats = pd.DataFrame({
        "column": null_counts.index,
        "null_count": null_counts.values,
        "null_rate_%": null_rates.values,
    })

    # Outlier counts based on enrichment thresholds (informational)
    from src.config import HIGH_AMOUNT_THRESHOLD, HIGH_DURATION_THRESHOLD, HIGH_LOGIN_ATTEMPTS_THRESHOLD
    outlier_high_amount = int((df_raw["TransactionAmount"] > HIGH_AMOUNT_THRESHOLD).sum())
    outlier_long_duration = int((df_raw["TransactionDuration"] > HIGH_DURATION_THRESHOLD).sum())
    outlier_login = int((df_raw["LoginAttempts"] > HIGH_LOGIN_ATTEMPTS_THRESHOLD).sum())

    summary_rows = pd.DataFrame([
        {"column": "--- RECORD STATS ---",         "null_count": "",  "null_rate_%": ""},
        {"column": "Total raw records",             "null_count": rows_raw,           "null_rate_%": ""},
        {"column": "Records after cleaning",        "null_count": rows_clean,         "null_rate_%": ""},
        {"column": "Records dropped",               "null_count": rows_dropped,       "null_rate_%": ""},
        {"column": "Duplicates removed",            "null_count": duplicates_removed, "null_rate_%": ""},
        {"column": "--- OUTLIER COUNTS (raw) ---",  "null_count": "",  "null_rate_%": ""},
        {"column": "High-value transactions",       "null_count": outlier_high_amount,    "null_rate_%": ""},
        {"column": "Long-duration transactions",    "null_count": outlier_long_duration,  "null_rate_%": ""},
        {"column": "High login-attempt transactions","null_count": outlier_login,          "null_rate_%": ""},
    ])

    report = pd.concat([column_stats, summary_rows], ignore_index=True)
    return report
