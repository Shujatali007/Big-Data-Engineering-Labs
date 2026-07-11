import pandas as pd
import time
import os

# ── Load the base CSV ─────────────────────────────────────────────────────────
print("Loading base CSV dataset..")
df = pd.read_csv("data/transactions.csv")
print(f"Loaded {len(df):,} rows.\n")

results = {}

# ── CSV (baseline) ────────────────────────────────────────────────────────────
start = time.time()
df.to_csv("data/transactions_out.csv", index=False)
results["CSV"] = {
    "write_time": round(time.time() - start, 2),
    "size_mb": round(os.path.getsize("data/transactions_out.csv") / (1024**2), 1)
}

# ── Parquet (ZSTD compression) — primary recommended format ──────────────────
# ZSTD is the safest cross-platform choice: it is bundled with pyarrow and
# does not require the native libsnappy C library that can fail on some
# macOS setups (especially Apple Silicon with older pyarrow builds).
start = time.time()
df.to_parquet("data/transactions_zstd.parquet", compression="zstd", index=False)
results["Parquet (ZSTD)"] = {
    "write_time": round(time.time() - start, 2),
    "size_mb": round(os.path.getsize("data/transactions_zstd.parquet") / (1024**2), 1)
}

# ── Parquet (Snappy compression) — optional, may fail on some Mac setups ─────
# If this block raises an error, install the native binding: pip install python-snappy
# and retry. On some macOS environments you can safely skip this block.
try:
    start = time.time()
    df.to_parquet("data/transactions.parquet", compression="snappy", index=False)
    results["Parquet (Snappy)"] = {
        "write_time": round(time.time() - start, 2),
        "size_mb": round(os.path.getsize("data/transactions.parquet") / (1024**2), 1)
    }
except Exception as e:
    print(f"Snappy compression unavailable: {e}")
    print("Tip: run  pip install python-snappy  and retry, or skip this format.\n")
    results["Parquet (Snappy)"] = {"write_time": "N/A", "size_mb": "N/A"}

# ── Print Write & Size Results ────────────────────────────────────────────────
csv_size = results["CSV"]["size_mb"]
print(f"{'Format':<22} {'Write Time (s)':<18} {'File Size (MB)':<18} {'Size Reduction'}")
print("-" * 75)
for fmt, data in results.items():
    if isinstance(data['size_mb'], float):
        reduction = f"{(1 - data['size_mb']/csv_size)*100:.1f}%" if fmt != "CSV" else "—"
    else:
        reduction = "N/A"
    print(f"{fmt:<22} {data['write_time']:<18} {data['size_mb']:<18} {reduction}")

# ── Query Benchmark ───────────────────────────────────────────────────────────
# Query: Total revenue for Electronics transactions in the US
print("\n--- Query Benchmark: SUM(amount) WHERE category='Electronics' AND country_code='US' ---\n")

query_results = {}

# CSV: Must read ALL columns and ALL rows, then filter in memory
start = time.time()
df_csv = pd.read_csv("data/transactions_out.csv")
result_csv = df_csv[
    (df_csv["category"] == "Electronics") &
    (df_csv["country_code"] == "US")
]["amount"].sum()
query_results["CSV (full scan)"] = round(time.time() - start, 2)

# Parquet (ZSTD): Column pruning — only reads the 3 columns needed
start = time.time()
df_pq = pd.read_parquet(
    "data/transactions_zstd.parquet",
    columns=["category", "country_code", "amount"]  # Only load 3 of 8 columns
)
result_pq = df_pq[
    (df_pq["category"] == "Electronics") &
    (df_pq["country_code"] == "US")
]["amount"].sum()
query_results["Parquet ZSTD (column pruning)"] = round(time.time() - start, 2)

# Verify both results match
assert abs(result_csv - result_pq) < 0.01, "Results don't match!"

print(f"{'Format':<35} {'Query Time (s)':<18} {'Speedup vs CSV'}")
print("-" * 65)
csv_time = query_results["CSV (full scan)"]
for fmt, t in query_results.items():
    speedup = f"{csv_time/t:.1f}x" if fmt != "CSV (full scan)" else "1.0x (baseline)"
    print(f"{fmt:<35} {t:<18} {speedup}")

print(f"\nBoth formats returned the same result: ${result_csv:,.2f}")
