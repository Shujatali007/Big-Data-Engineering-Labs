import os

data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datashop_data")
required = ["datashop_transactions.parquet", "datashop_products.parquet"]

print("Verifying DataShop input files for Lab 04..")
all_present = True
for f in required:
    path = os.path.join(data_dir, f)
    exists = os.path.exists(path)
    size_mb = round(os.path.getsize(path) / (1024**2), 2) if exists else 0
    status = "OK" if exists else "MISSING"
    print(f"  [{status}] {path} ({size_mb} MB)")
    if not exists:
        all_present = False

print()
if all_present:
    print("All required files found. Proceed with the lab exercises.")
else:
    print("One or more files are missing. Return to Lab 02 and run convert_datashop.py.")
