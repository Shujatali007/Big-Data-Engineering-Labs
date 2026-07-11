#!/usr/bin/env bash
# Exercise 3: Explore the DataShop transactions dataset with Unix pipeline tools.
set -euo pipefail

DATA_DIR="$HOME/Big_Data_Engineering_Labs/datashop_data"
TXN_FILE="$DATA_DIR/datashop_transactions.csv"

echo "=== Row count (includes header; data rows = result - 1) ==="
wc -l "$TXN_FILE"

echo
echo "=== Header + first 4 data rows ==="
head -5 "$TXN_FILE"

echo
echo "=== Transaction count per category (field 4) ==="
cut -d',' -f4 "$TXN_FILE" | sort | uniq -c

echo
echo "=== Average transaction amount per category (DuckDB) ==="
duckdb -c "
SELECT
    category,
    ROUND(AVG(amount), 2) AS avg_amount
FROM read_csv_auto('$TXN_FILE')
GROUP BY category
ORDER BY avg_amount DESC
"
