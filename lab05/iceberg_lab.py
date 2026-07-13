from pyspark.sql import SparkSession
from pyspark.sql.functions import month, col, lit, when, rand, expr
import os
import shutil

# ── Resolve paths relative to this file (matches Lab 02-04 convention) ────────
lab_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(lab_dir)
data_dir = os.path.join(project_root, "datashop_data")
warehouse_dir = os.path.join(lab_dir, "warehouse")
jar_path = os.path.join(lab_dir, "jars", "iceberg-spark-runtime.jar")

# This script re-runs Exercises 1-3 end to end every time (rather than being
# appended-to incrementally), so wipe any warehouse left by a previous run —
# otherwise the table already has discount_pct from a prior Exercise 3 and the
# Exercise 1 January append fails with a schema mismatch.
shutil.rmtree(warehouse_dir, ignore_errors=True)

# ── Initialize Spark with Iceberg support ─────────────────────────────────────
spark = SparkSession.builder \
    .appName("IcebergLakehouseLab") \
    .master("local[*]") \
    .config("spark.jars", jar_path) \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", warehouse_dir) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=== EXERCISE 1: Create Iceberg Table and Load January Data (Snapshot 1) ===\n")

# ── Read the DataShop transactions Parquet file ───────────────────────────────
# Read and rename 'timestamp' to 'txn_timestamp' — 'timestamp' is a reserved
# SQL keyword in some contexts and causes parsing errors inside Iceberg SQL.
all_txns = spark.read.parquet(f"{data_dir}/datashop_transactions.parquet") \
    .withColumnRenamed("timestamp", "txn_timestamp") \
    .withColumn("txn_timestamp", col("txn_timestamp").cast("timestamp"))

print(f"Total DataShop transactions loaded: {all_txns.count():,}")
all_txns.printSchema()

# ── Split into monthly batches by filtering on the month of txn_timestamp ─────
# With freq="10min" starting 2024-01-01, 100K rows spans ~694 days,
# so January, February, and March 2024 are all present in the dataset.
jan_data = all_txns.filter(month(col("txn_timestamp")) == 1)
feb_data = all_txns.filter(month(col("txn_timestamp")) == 2)
mar_data = all_txns.filter(month(col("txn_timestamp")) == 3)

jan_count = jan_data.count()
feb_count = feb_data.count()
mar_count = mar_data.count()
print(f"\nMonthly split — Jan: {jan_count:,} | Feb: {feb_count:,} | Mar: {mar_count:,} rows\n")

# ── Create the Iceberg table — partitioned by month of txn_timestamp ──────────
# The schema matches the DataShop transactions schema exactly.
# PARTITIONED BY (months(txn_timestamp)) creates one partition per calendar month.
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.db.datashop_transactions (
        transaction_id  STRING,
        customer_id     INT,
        product_id      INT,
        category        STRING,
        amount          DOUBLE,
        currency        STRING,
        country_code    STRING,
        status          STRING,
        txn_timestamp   TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (months(txn_timestamp))
""")
print("Iceberg table 'local.db.datashop_transactions' created (or already exists).\n")

# ── Write January data → Snapshot 1 ──────────────────────────────────────────
jan_data.write \
    .format("iceberg") \
    .mode("append") \
    .saveAsTable("local.db.datashop_transactions")

print(f"Snapshot 1 created — {jan_count:,} January 2024 transactions loaded.\n")

# Show the current snapshot and a basic aggregation
print("Current table row count and snapshot:")
spark.sql("SELECT COUNT(*) AS row_count FROM local.db.datashop_transactions").show()

print("January revenue by category:")
spark.sql("""
    SELECT
        category,
        COUNT(*)               AS transaction_count,
        ROUND(SUM(amount), 2)  AS total_revenue,
        status
    FROM local.db.datashop_transactions
    WHERE status = 'completed'
    GROUP BY category, status
    ORDER BY total_revenue DESC
""").show()

print("Snapshot 1 history:")
spark.sql("""
    SELECT snapshot_id, committed_at, operation, summary['total-records'] AS total_records
    FROM local.db.datashop_transactions.snapshots
""").show(truncate=False)


print("=== EXERCISE 2: Load February and March Data, Then Time Travel ===\n")

# ── Write February data → Snapshot 2 ─────────────────────────────────────────
feb_data.write \
    .format("iceberg") \
    .mode("append") \
    .saveAsTable("local.db.datashop_transactions")

print(f"Snapshot 2 created — {feb_count:,} February 2024 transactions added.\n")

# ── Write March data → Snapshot 3 ────────────────────────────────────────────
mar_data.write \
    .format("iceberg") \
    .mode("append") \
    .saveAsTable("local.db.datashop_transactions")

print(f"Snapshot 3 created — {mar_count:,} March 2024 transactions added.\n")

# ── List all three snapshots ──────────────────────────────────────────────────
print("Full snapshot history (3 snapshots expected):")
spark.sql("""
    SELECT snapshot_id, committed_at, operation, summary['total-records'] AS total_records
    FROM local.db.datashop_transactions.snapshots
    ORDER BY committed_at ASC
""").show(truncate=False)

# ── Time travel: query the table as it existed at Snapshot 1 (January only) ───
# Retrieve the first snapshot ID — that is the January-only state.
snapshots = spark.sql("""
    SELECT snapshot_id
    FROM local.db.datashop_transactions.snapshots
    ORDER BY committed_at ASC
""").collect()

first_snapshot_id = snapshots[0]["snapshot_id"]
print(f"Time travelling to Snapshot 1 (ID: {first_snapshot_id}) — January only...\n")

time_travel_df = spark.sql(f"""
    SELECT COUNT(*) AS row_count
    FROM local.db.datashop_transactions VERSION AS OF {first_snapshot_id}
""")
time_travel_count = time_travel_df.collect()[0]["row_count"]
print(f"Row count at Snapshot 1 (time travel): {time_travel_count:,}")
print(f"Expected January row count:            {jan_count:,}")
print(f"Match: {time_travel_count == jan_count}\n")

# Show a sample of the January-only snapshot
print("Sample of January-only data (time travel query):")
spark.sql(f"""
    SELECT transaction_id, customer_id, category, amount, status, txn_timestamp
    FROM local.db.datashop_transactions VERSION AS OF {first_snapshot_id}
    LIMIT 10
""").show(truncate=False)


print("=== EXERCISE 3: Schema Evolution — Add discount_pct Column ===\n")

# ── Add a new column — this is a metadata-only operation ──────────────────────
# Iceberg adds the column definition to the table metadata without rewriting
# any existing Parquet data files. Existing rows will return NULL for the
# new column, while newly written rows can populate it.
spark.sql("ALTER TABLE local.db.datashop_transactions ADD COLUMN discount_pct DOUBLE")
print("Column 'discount_pct' added (metadata-only — no data rewrite).\n")

# Show existing rows — they return NULL for the new column
print("Existing rows after schema evolution (discount_pct is NULL for all):")
spark.sql("""
    SELECT transaction_id, amount, currency, status, discount_pct
    FROM local.db.datashop_transactions
    LIMIT 8
""").show()

# ── Write a new batch with discount values populated ──────────────────────────
# Simulate a promotional April batch where some transactions have a discount.
apr_sample = all_txns.filter(month(col("txn_timestamp")) == 4).limit(500)

# Note on the April sample: if the dataset does not span into April, fall back
# to any 500 rows so the schema evolution demonstration still works.
if apr_sample.count() == 0:
    apr_sample = all_txns.limit(500)

# Add a discount_pct column: 20% chance of a 5.0-15.0 discount; otherwise NULL
apr_with_discount = apr_sample.withColumn(
    "discount_pct",
    when(rand() < 0.2, expr("round(5.0 + rand() * 10.0, 1)")).otherwise(lit(None).cast("double"))
)

apr_with_discount.write \
    .format("iceberg") \
    .mode("append") \
    .saveAsTable("local.db.datashop_transactions")

print("New batch with discount_pct written (Snapshot 4 created).\n")

# Show the mixed NULL / non-NULL state
print("Mixed NULL/non-NULL discount_pct across old and new rows:")
spark.sql("""
    SELECT
        transaction_id,
        amount,
        status,
        discount_pct,
        CASE WHEN discount_pct IS NULL THEN 'legacy row (no discount)'
             ELSE 'new row (discount applied)'
        END AS row_origin
    FROM local.db.datashop_transactions
    ORDER BY discount_pct DESC NULLS LAST
    LIMIT 15
""").show(truncate=False)

print("Final snapshot history (4 snapshots):")
spark.sql("""
    SELECT snapshot_id, committed_at, operation, summary['total-records'] AS total_records
    FROM local.db.datashop_transactions.snapshots
    ORDER BY committed_at ASC
""").show(truncate=False)

print("Schema evolution complete — no data rewrite was required for existing rows.")
spark.stop()
