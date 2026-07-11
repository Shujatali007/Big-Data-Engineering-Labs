from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, round as spark_round
import os

data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "datashop_data")

# 1. Initialise the SparkSession
# This is the entry point for programming Spark with the DataFrame API.
spark = SparkSession.builder \
    .appName("DataShop_TransactionAnalysis") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Spark Session initialised successfully.")

# 2. Read the Parquet data produced by Lab 02
# Spark reads the columnar data in parallel across available cores.
print("Reading DataShop transactions from Parquet..")
raw_df = spark.read.parquet(f"{data_dir}/datashop_transactions.parquet")
print(f"Total records: {raw_df.count():,}")

# 3. Clean and transform (Transformations — Lazy Evaluation)
# Filter to completed transactions only and drop any rows where amount is null.
clean_df = raw_df \
    .filter(col("status") == "completed") \
    .filter(col("amount").isNotNull())
print(f"Completed transactions: {clean_df.count():,}")

# 4. Aggregate by category and country
# This produces a 25-row result (5 categories × 5 countries).
summary_df = clean_df \
    .groupBy("category", "country_code") \
    .agg(
        spark_round(spark_sum("amount"), 2).alias("total_revenue"),
        count("transaction_id").alias("order_count")
    ) \
    .orderBy("category", "country_code")

# 5. Execute an Action to trigger computation and show results
print("\n--- Revenue Summary by Category and Country ---")
summary_df.show(30)

# 6. Write results to Parquet for downstream consumption (Lab 09 — Airflow)
output_path = f"{data_dir}/category_country_summary.parquet"
summary_df.write.mode("overwrite").parquet(output_path)
print(f"\nResults written to {output_path}")

# 7. Stop the SparkSession
spark.stop()
