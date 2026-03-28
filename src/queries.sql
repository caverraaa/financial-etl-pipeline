-- Q1: Total and average transaction amount per channel


SELECT
    "Channel",
    COUNT(*) AS transaction_count,
    ROUND(SUM("TransactionAmount")::numeric, 2) AS total_amount,
    ROUND(AVG("TransactionAmount")::numeric, 2) AS avg_amount
FROM transactions
GROUP BY "Channel"
ORDER BY total_amount DESC;

-- split

-- Q2: All transactions in high-risk area in descended order by risk_score and TrnsactionAmount

SELECT 
    "TransactionID", "AccountID", "TransactionAmount", "Channel", 
    "LoginAttempts", "TransactionDuration", "risk_score", "risk_label"
FROM transactions
WHERE risk_label IN ('high', 'critical')
ORDER BY "risk_score" DESC, "TransactionAmount" DESC;

-- split

-- Q3: Accounts with repeat and suspicious activity

SELECT 
    "AccountID", 
    COUNT(*) AS suspicious_count, 
    ROUND(SUM("TransactionAmount")::numeric, 2) AS total_amount
FROM transactions
WHERE risk_score >= 2
GROUP BY "AccountID"
HAVING COUNT(*) > 1
ORDER BY suspicious_count DESC;

-- split

-- Q4: Transactions above average amount:

SELECT *
FROM transactions
WHERE "TransactionAmount" > (SELECT AVG("TransactionAmount") FROM transactions)
ORDER BY "TransactionAmount" ASC;

-- split

-- Q5: Monthly transaction volume trend

SELECT
    DATE_TRUNC('month', "TransactionDate") AS month,
    COUNT(*) AS transaction_count,
    ROUND(SUM("TransactionAmount")::numeric, 2) AS total_amount,
    ROUND(AVG("TransactionAmount")::numeric, 2) AS avg_amount
FROM transactions
GROUP BY month
ORDER BY month;

-- split

-- Q6: Volume and risk breakdown by channel

SELECT
    "Channel",
    risk_label,
    COUNT(*) AS transaction_count,
    ROUND(SUM("TransactionAmount")::numeric, 2) AS total_amount,
    ROUND(AVG("TransactionAmount")::numeric, 2) AS avg_amount
FROM transactions
GROUP BY "Channel", risk_label
ORDER BY "Channel", risk_label;