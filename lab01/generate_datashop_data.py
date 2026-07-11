import pandas as pd
import numpy as np
import os

np.random.seed(42)
os.makedirs(os.path.expanduser("~/Desktop/Big_Data_Engineering_Labs/datashop_data"), exist_ok=True)
data_dir = os.path.expanduser("~/Desktop/Big_Data_Engineering_Labs/datashop_data")

# ── Transactions (100,000 rows) ───────────────────────────────────────────────
n = 100_000
transactions = pd.DataFrame({
    "transaction_id": [f"TXN-{i:08d}" for i in range(n)],
    "customer_id": np.random.randint(1, 5001, n),
    "product_id": np.random.randint(1, 101, n),
    "category": np.random.choice(
        ["Electronics", "Clothing", "Food", "Books", "Sports"], n,
        p=[0.30, 0.25, 0.20, 0.15, 0.10]),
    "amount": np.random.uniform(5.0, 500.0, n).round(2),
    "currency": np.random.choice(["USD", "EUR", "GBP", "JPY", "SGD"], n,
                                  p=[0.60, 0.15, 0.12, 0.08, 0.05]),
    "country_code": np.random.choice(["US", "GB", "DE", "FR", "SG"], n,
                                      p=[0.50, 0.20, 0.15, 0.10, 0.05]),
    "status": np.random.choice(["completed", "cancelled", "refunded"], n,
                                p=[0.75, 0.15, 0.10]),
    "timestamp": pd.date_range("2024-01-01", periods=n, freq="10min"),
})
transactions.to_csv(f"{data_dir}/datashop_transactions.csv", index=False)
print(f"Generated {n:,} transactions -> {data_dir}/datashop_transactions.csv")

# ── Customers (5,000 rows) ────────────────────────────────────────────────────
first_names = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack",
               "Karen", "Liam", "Maria", "Noah", "Olivia", "Paul", "Quinn", "Rose", "Sam", "Tina"]
last_names = ["Smith", "Jones", "White", "Brown", "Davis", "Wilson", "Moore", "Taylor", "Anderson", "Thomas"]
n_cust = 5000
customers = pd.DataFrame({
    "customer_id": range(1, n_cust + 1),
    "email": [f"customer{i}@datashop.com" for i in range(1, n_cust + 1)],
    "first_name": np.random.choice(first_names, n_cust),
    "last_name": np.random.choice(last_names, n_cust),
    "signup_date": pd.date_range("2022-01-01", periods=n_cust, freq="6h").date,
    "country_code": np.random.choice(["US", "GB", "DE", "FR", "SG"], n_cust, p=[0.50, 0.20, 0.15, 0.10, 0.05]),
    "is_deleted": np.random.choice([False, True], n_cust, p=[0.95, 0.05]),
})
customers.to_csv(f"{data_dir}/datashop_customers.csv", index=False)
print(f"Generated {n_cust:,} customers -> {data_dir}/datashop_customers.csv")

# ── Products (100 rows) ───────────────────────────────────────────────────────
categories = ["Electronics", "Clothing", "Food", "Books", "Sports"]
n_prod = 100
products = pd.DataFrame({
    "product_id": range(1, n_prod + 1),
    "product_name": [f"Product-{i:03d}" for i in range(1, n_prod + 1)],
    "category": [categories[i % 5] for i in range(n_prod)],
    "brand": [f"Brand-{(i % 10) + 1}" for i in range(n_prod)],
    "unit_price": np.random.uniform(5.0, 500.0, n_prod).round(2),
})
products.to_csv(f"{data_dir}/datashop_products.csv", index=False)
print(f"Generated {n_prod} products -> {data_dir}/datashop_products.csv")

print("\nAll DataShop seed data generated successfully.")
