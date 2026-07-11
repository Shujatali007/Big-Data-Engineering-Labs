# Lab 03: Distributed Processing with PySpark

## Contents

- `process_transactions.py` — reads `datashop_transactions.parquet` (produced in Lab 02) with
  PySpark, filters to completed transactions, aggregates revenue and order count by
  `category` and `country_code`, and writes the 25-row summary back out as
  `category_country_summary.parquet` for downstream consumption (Lab 09 — Airflow).

## Results

```
Total records: 100,000
Completed transactions: 75,067
```

The ~75% completion rate matches how the DataShop dataset was generated in Lab 01.

| category    | country_code | total_revenue | order_count |
|-------------|--------------|----------------|--------------|
| Books       | DE           | 417,026.64     | 1,686        |
| Books       | FR           | 290,925.91     | 1,126        |
| Books       | GB           | 554,338.26     | 2,225        |
| Books       | SG           | 142,252.59     | 565          |
| Books       | US           | 1,413,653.47   | 5,540        |
| Clothing    | DE           | 727,347.56     | 2,853        |
| Clothing    | FR           | 467,623.75     | 1,859        |
| Clothing    | GB           | 962,440.29     | 3,801        |
| Clothing    | SG           | 244,628.49     | 994          |
| Clothing    | US           | 2,324,751.29   | 9,230        |
| Electronics | DE           | 821,925.86     | 3,271        |
| Electronics | FR           | 573,977.31     | 2,279        |
| Electronics | GB           | 1,166,648.42   | 4,600        |
| Electronics | SG           | 291,139.43     | 1,129        |
| Electronics | US           | 2,819,122.90   | 11,164       |
| Food        | DE           | 576,165.00     | 2,272        |
| Food        | FR           | 393,577.28     | 1,550        |
| Food        | GB           | 762,643.34     | 3,006        |
| Food        | SG           | 203,025.60     | 785          |
| Food        | US           | 1,866,900.06   | 7,490        |
| Sports      | DE           | 290,294.94     | 1,126        |
| Sports      | FR           | 181,013.00     | 738          |
| Sports      | GB           | 406,161.81     | 1,600        |
| Sports      | SG           | 94,688.79      | 370          |
| Sports      | US           | 953,631.54     | 3,808        |

25 rows (5 categories x 5 countries), as expected. US revenue is consistently far above SG's
across every category (e.g. Electronics: $2.82M vs. $291K) — a direct reflection of the 50%
vs. 5% country distribution baked into the synthetic dataset in Lab 01.

Output written to `datashop_data/category_country_summary.parquet` (`_SUCCESS` marker plus one
Snappy-compressed part file, confirming the write succeeded).

## Reflection: transformations vs. actions

Spark splits DataFrame operations into two categories, and `process_transactions.py` uses one
of each back to back:

```python
clean_df = raw_df.filter(col("status") == "completed").filter(col("amount").isNotNull())
print(f"Completed transactions: {clean_df.count():,}")
```

`filter()` is a **transformation**: it returns a new DataFrame describing "keep only rows where
`status == 'completed'` and `amount` is not null," but Spark does not actually scan any data when
this line executes. It just extends the logical execution plan — the *lineage* — with one more
step. This is why calling `.filter()` on a 100,000-row (or 100-million-row) DataFrame is
instantaneous regardless of dataset size: nothing has been computed yet.

`count()` is an **action**: it is the point where Spark takes the full accumulated plan (read
Parquet -> filter status -> filter amount) and actually executes it across all available cores,
producing a concrete result — `75,067` in this run. Actions are what force evaluation; every
transformation before an action is deferred until an action needs the result.

This lazy-evaluation model is why the script's `.count()` and `.show()` calls are noticeably
slower than the `.filter()` calls that precede them: the filters are free bookkeeping, while each
action triggers a real distributed computation over the underlying Parquet file. It also means
that if the same DataFrame were used in two separate actions without caching, Spark would
recompute the whole filter chain from scratch each time — the reason `.cache()` / `.persist()`
exist for DataFrames that are reused across multiple actions.
