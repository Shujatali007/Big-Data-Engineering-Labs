import pandas as pd
import numpy as np
import os

# Generate a realistic e-commerce transaction dataset with 5 million rows
np.random.seed(42)
n_rows = 5_000_000

print(f"Generating {n_rows:,} rows of synthetic transaction data..")

df = pd.DataFrame({
    'transaction_id': [f"TXN-{i:08d}" for i in range(n_rows)],
    'customer_id':    np.random.randint(1, 100_000, n_rows),
    'product_id':     np.random.randint(1, 5_000, n_rows),
    'amount':         np.random.uniform(1.0, 500.0, n_rows).round(2),
    'category':       np.random.choice(
        ['Electronics', 'Clothing', 'Food', 'Books', 'Sports'], n_rows
    ),
    'country_code':   np.random.choice(['US', 'GB', 'DE', 'FR', 'SG'], n_rows),
    'timestamp':      pd.date_range('2024-01-01', periods=n_rows, freq='s'),
    'is_refunded':    np.random.choice([True, False], n_rows, p=[0.05, 0.95]),
})

os.makedirs("data", exist_ok=True)
df.to_csv("data/transactions.csv", index=False)
print(f"Dataset saved. Shape: {df.shape}")
