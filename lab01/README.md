# Lab 01: Environment Setup and the Data Engineering Toolkit

## Contents

- `verify_environment.py` — checks Python 3.10+, PySpark, Pandas, PyArrow, dbt-core, and Great Expectations.
- `generate_datashop_data.py` — generates the shared DataShop seed dataset (`datashop_transactions.csv`,
  `datashop_customers.csv`, `datashop_products.csv`) used by all subsequent labs.
- `explore_data.sh` — Unix pipeline exploration of the transactions file (`wc`, `head`, `cut | sort | uniq -c`)
  plus a DuckDB query for average transaction amount per category.
- `architecture_sketch.png` — sketch of the 7-layer DataShop platform architecture (Ingestion, Storage,
  Batch Processing, Streaming, Orchestration, Quality, Serving).

## Results

`verify_environment.py` output:

```
[OK ] Python: 3.12.13
[OK ] PySpark: 4.1.2
[OK ] Pandas: 3.0.3
[OK ] PyArrow: 25.0.0
[OK ] dbt-core: 1.11.12
[OK ] Great Expectations: 1.18.2
All tools verified. Your environment is ready for the course.
```

`explore_data.sh` confirms 100,000 transaction rows (100,001 lines including the header) across five
categories (Electronics, Clothing, Food, Books, Sports), with average transaction amount clustered
around $252 per category, as expected from the uniform amount distribution.

## Reflection

The trickiest part of this lab wasn't installing the tools — it was the `dbt-core` version check in
`verify_environment.py`. My first instinct was to read `dbt.__version__` directly, but that attribute
isn't reliably set across recent `dbt-core` releases and raised an `AttributeError` instead of the
`ImportError` the script's `try/except` was built to catch. Switching to the documented,
version-stable API — `from dbt.version import get_installed_version` — fixed it cleanly and is now
the approach used in the script. The second small hiccup was keeping track of *where* the shared
`~/Big_Data_Engineering_Labs/` project root actually lived versus where my git repository was
initialized; consolidating both into the same directory before generating the DataShop data avoided
duplicated (and possibly divergent) copies of the seed dataset for later labs.
