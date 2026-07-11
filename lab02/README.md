# Lab 02: Storage Formats Benchmark — CSV, Parquet, and ORC

## Contents

- `generate_data.py` — generates a 5-million-row synthetic e-commerce transactions dataset
  (`data/transactions.csv`) used only for Part A's benchmark.
- `benchmark.py` — writes the dataset out as CSV, Parquet (ZSTD), and Parquet (Snappy), compares
  write time and file size, then benchmarks a filtered aggregation query (`SUM(amount) WHERE
  category='Electronics' AND country_code='US'`) reading CSV (full scan) vs. Parquet with column
  pruning.
- `convert_datashop.py` — converts the three real DataShop seed files from Lab 01
  (`datashop_transactions.csv`, `datashop_customers.csv`, `datashop_products.csv`) into Parquet,
  producing the files Labs 3–11 will read.

`data/` (the 5M-row benchmark files, ~760 MB) is gitignored and regenerated locally via
`python generate_data.py && python benchmark.py`.

## Results

### Write time & file size (5M rows)

| Format          | Write Time (s) | File Size (MB) | Size Reduction |
|------------------|----------------|-----------------|-----------------|
| CSV              | 5.61           | 320.0           | —               |
| Parquet (ZSTD)   | 4.78           | 39.7            | 87.6%           |
| Parquet (Snappy) | 0.66           | 79.4            | 75.2%           |

### Query benchmark

| Format                        | Query Time (s) | Speedup vs CSV |
|--------------------------------|-----------------|------------------|
| CSV (full scan)                | 3.19            | 1.0x (baseline)  |
| Parquet ZSTD (column pruning)  | 3.88            | 0.8x             |

Both formats returned the same result: **$50,346,076.93**.

**Note on the query result:** the lab guide's expected-outcome table shows Parquet ~14x faster than
CSV; on this machine (Apple Silicon, pandas 3.0 with its Arrow-backed CSV parser, SSD, 320 MB file
fully warm in the OS page cache) the measured result was the opposite — Parquet column-pruned read
was slightly *slower* than a full CSV scan. This is a real, reproducible measurement, not a fluke of
running the script once — I did not adjust it to match the guide's expected numbers. Three factors
explain the gap: (1) pandas 3.0's CSV reader is itself Arrow-backed and highly vectorized, so it no
longer pays the "slow Python CSV parser" tax the original 14x number assumes; (2) at 320 MB the whole
file sits comfortably in RAM/page cache after the write step that immediately preceded the read, so
disk I/O — the dimension Parquet's compression and column pruning most directly help — isn't the
bottleneck here; and (3) `pandas.to_parquet` doesn't dictionary-encode the low-cardinality string
columns by default, so decompression on read has real CPU work to do even for the 3 pruned columns.
The advantage Parquet has *on paper* (less data to read from disk, less to decompress for a narrow
column subset) becomes visible mainly at larger-than-RAM scale or when queried through an engine with
real predicate/column pushdown (DuckDB, Spark, Iceberg) rather than `pandas.read_parquet` loading a
full column into memory before filtering.

## Reflection

**1. By what factor is the Parquet file smaller than the CSV? Why does columnar compression achieve
such high ratios on this dataset?**

Parquet (ZSTD) is about 8x smaller than CSV (39.7 MB vs. 320.0 MB, an 87.6% reduction); even Snappy,
a weaker but faster codec, gets a 75.2% reduction. Columnar storage groups values from the same
column contiguously, so a compressor sees long runs of similar, low-entropy data — repeated
`category` strings, a narrow range of `country_code`s, monotonically increasing `timestamp`s — instead
of CSV's interleaved rows where every line mixes ID strings, floats, and text. Columnar formats also
use type-aware encodings (dictionary encoding for repeated strings, delta encoding for
timestamps/integers) before general compression is even applied, which CSV's plain-text
representation can't take advantage of at all.

**2. The Parquet query with column pruning only reads 3 of the 8 columns. How does this explain the
query speedup?**

In principle, reading only `category`, `country_code`, and `amount` instead of all 8 columns (including
the long `transaction_id` strings and `timestamp`s) should mean roughly 60-70% less data decoded and
loaded into memory, which is exactly why Parquet is the standard choice for wide analytical tables —
the fewer columns a query touches relative to the table's total width, the more column pruning saves.
On this run that theoretical saving didn't show up as a wall-clock win (see the note above): the file
was small enough and warm enough in cache that CSV's full scan wasn't disk-bound, and decompression
overhead ate the savings from reading fewer bytes. At real data-lake scale — hundreds of GB to TBs,
cold storage, wide tables with dozens of columns — the column-pruning advantage dominates because I/O
and decompression cost scale with columns read, not with total table width.

**3. If you were designing a data lake for a company that runs thousands of analytical queries per
day, which format would you choose and why?**

Parquet, specifically with ZSTD compression, matching the recommendation this lab operationalizes in
Part B. Even where this run's single-machine, warm-cache query timing didn't show a speedup, the file
size result is unambiguous and compounds directly into cost and latency at scale: 87.6% smaller files
mean proportionally less storage cost, less network/disk I/O per query, and — critically for a
data lake with many analytical (not transactional) queries — column pruning and predicate pushdown
that only a columnar format like Parquet supports. Those benefits are largest exactly in the regime a
production data lake operates in (TB-scale tables, cold storage, engines like Spark/DuckDB/Iceberg
that push filters and column selection down to the file reader) rather than the small-file,
single-machine, cache-warm regime this local benchmark ran in.

## Part B: DataShop conversion

`convert_datashop.py` converted the three Lab 01 seed files into Parquet:

```
datashop_customers.csv       305K   ->  datashop_customers.parquet     82K
datashop_products.csv        3.7K   ->  datashop_products.parquet      5.2K
datashop_transactions.csv    6.9M   ->  datashop_transactions.parquet  1.9M
```

`datashop_transactions.parquet` shrank by ~72%. `datashop_products.parquet` is actually slightly
*larger* than its CSV — at 100 rows the table is small enough that Parquet's per-file metadata
(schema, row-group statistics) outweighs any compression gain; the format's advantages only kick in
once a table has enough rows for compression to amortize that fixed overhead, which is exactly what
the 5M-row Part A benchmark was designed to demonstrate.

These three Parquet files now live in `~/Desktop/Big_Data_Engineering_Labs/datashop_data/` alongside
the original CSVs, ready for Lab 03 onward to read.
