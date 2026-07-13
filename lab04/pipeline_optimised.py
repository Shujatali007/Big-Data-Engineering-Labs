from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, broadcast, sum as spark_sum, count
import os
import time

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(project_root, "datashop_data")
event_log_dir = os.path.join(project_root, "spark-events")
os.makedirs(event_log_dir, exist_ok=True)

spark = SparkSession.builder \
    .appName("DataShop_SalesPipeline_Optimised") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", f"file://{event_log_dir}") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

start = time.time()

# Read the fact table and dimension table from Lab 02's Parquet output
raw_sales = spark.read.parquet(f"{data_dir}/datashop_transactions.parquet")

# Optimisation: Column pruning — only read the two columns we need from products
product_lookup = spark.read.parquet(f"{data_dir}/datashop_products.parquet") \
    .select("product_id", "product_name")

# Derive sale_date and clean the data
clean_sales = raw_sales \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .withColumn("sale_date", to_date(col("timestamp")))

# Optimisation 1: Broadcast join
# The product table has 100 rows — it is tiny and perfect for broadcasting.
# This avoids the expensive sort-merge join that Spark would choose by default.
enriched_sales = clean_sales.join(
    broadcast(product_lookup),  # <-- broadcast hint
    on="product_id",
    how="left"
)

# Aggregate by date and category
daily_summary = enriched_sales \
    .groupBy("sale_date", "category") \
    .agg(
        spark_sum("amount").alias("total_revenue"),
        count("transaction_id").alias("tx_count")
    ) \
    .orderBy("sale_date", "category")

# Inspect the execution plan — look for "BroadcastHashJoin" in the output
print("\nExecution Plan (look for BroadcastHashJoin):")
daily_summary.explain(mode="simple")

# Trigger execution with .count() — same action as baseline for a fair comparison
row_count = daily_summary.count()
elapsed = time.time() - start

print(f"\nOptimised pipeline completed in {elapsed:.2f}s. Output rows: {row_count}")

# Optimisation 3: Write output partitioned by sale_date for efficient downstream queries
# This is done AFTER timing so it does not inflate the optimised pipeline's measured time.
output_path = f"{data_dir}/daily_summary"
print(f"\nWriting partitioned output to {output_path} (not included in timing)..")
daily_summary.write \
    .mode("overwrite") \
    .partitionBy("sale_date") \
    .parquet(output_path)

print("Partitioned write complete.")

input("\nPress Enter to stop Spark and exit (UI available at http://localhost:4040)...")
spark.stop()
