# load_to_duckdb.py
# Run this ONCE before running dbt. It loads the DataShop Parquet files into
# a DuckDB database that dbt will use as its data warehouse.

import duckdb
import os

lab_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(lab_dir)
data_dir = os.path.join(project_root, "datashop_data")
db_path = os.path.join(lab_dir, "datashop.duckdb")

con = duckdb.connect(db_path)

# Load transactions
con.execute(f"""
DROP TABLE IF EXISTS raw_transactions;
CREATE TABLE raw_transactions AS
SELECT * FROM read_parquet('{data_dir}/datashop_transactions.parquet')
""")

# Load customers
con.execute(f"""
DROP TABLE IF EXISTS raw_customers;
CREATE TABLE raw_customers AS
SELECT * FROM read_parquet('{data_dir}/datashop_customers.parquet')
""")

row1 = con.execute("SELECT COUNT(*) FROM raw_transactions").fetchone()[0]
row2 = con.execute("SELECT COUNT(*) FROM raw_customers").fetchone()[0]

print(f"Loaded {row1:,} transactions and {row2:,} customers into DuckDB.")
print(f"Database saved at: {db_path}")

# Preview the schema of each table
print("\nraw_transactions schema:")
print(con.execute("DESCRIBE raw_transactions").df().to_string(index=False))
print("\nraw_customers schema:")
print(con.execute("DESCRIBE raw_customers").df().to_string(index=False))

con.close()
