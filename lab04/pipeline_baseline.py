from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as spark_sum, count
import os
import time

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(project_root, "datashop_data")
event_log_dir = os.path.join(project_root, "spark-events")
os.makedirs(event_log_dir, exist_ok=True)

spark = SparkSession.builder \
    .appName("DataShop_SalesPipeline_Baseline") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", f"file://{event_log_dir}") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

start = time.time()

# Read the fact table and dimension table from Lab 02's Parquet output
raw_sales = spark.read.parquet(f"{data_dir}/datashop_transactions.parquet")
product_lookup = spark.read.parquet(f"{data_dir}/datashop_products.parquet")

# Derive sale_date from the timestamp column and clean the data
clean_sales = raw_sales \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .withColumn("sale_date", to_date(col("timestamp")))

# Regular join (no broadcast hint) — Spark may choose a slow sort-merge join
# Drop 'category' from clean_sales first — both tables have this column and the
# duplicate causes an AMBIGUOUS_REFERENCE error during groupBy.
enriched_sales = clean_sales.drop("category").join(
    product_lookup.select("product_id", "category"),
    on="product_id",
    how="left"
)

# Aggregate by date and category
daily_summary = enriched_sales \
    .groupBy("sale_date", "category") \
    .agg(
        spark_sum("amount").alias("total_revenue"),
        count("transaction_id").alias("tx_count")
    )

print("\nBaseline Execution Plan:")
daily_summary.explain(mode="simple")

# Trigger execution with .count() — measures full pipeline without disk I/O
row_count = daily_summary.count()
elapsed = time.time() - start

print(f"\nBaseline pipeline completed in {elapsed:.2f}s. Output rows: {row_count}")

input("\nPress Enter to stop Spark and exit (UI available at http://localhost:4040)...")
spark.stop()
