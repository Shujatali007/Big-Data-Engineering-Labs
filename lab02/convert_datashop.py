import pandas as pd
import os

data_dir = os.path.expanduser("~/Desktop/Big_Data_Engineering_Labs/datashop_data")

print("Converting DataShop CSV files to Parquet..")

df = pd.read_csv(f"{data_dir}/datashop_transactions.csv")
df_cust = pd.read_csv(f"{data_dir}/datashop_customers.csv")
df_prod = pd.read_csv(f"{data_dir}/datashop_products.csv")

df.to_parquet(f"{data_dir}/datashop_transactions.parquet", index=False)
print(f"  datashop_transactions.parquet  ({len(df):,} rows)")

df_cust.to_parquet(f"{data_dir}/datashop_customers.parquet", index=False)
print(f"  datashop_customers.parquet     ({len(df_cust):,} rows)")

df_prod.to_parquet(f"{data_dir}/datashop_products.parquet", index=False)
print(f"  datashop_products.parquet      ({len(df_prod):,} rows)")

print("\nDataShop datasets converted to Parquet and saved to ~/Desktop/Big_Data_Engineering_Labs/datashop_data/")
