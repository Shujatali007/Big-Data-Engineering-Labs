# Lab 06: Modern Data Transformation with dbt and DuckDB

## Reflection: `{{ source() }}` vs `{{ ref() }}`

`{{ source() }}` and `{{ ref() }}` both compile to a table or view name at run time, but they
mark fundamentally different kinds of dependency. `{{ source('raw', 'raw_transactions') }}`,
used in `stg_transactions.sql` and `stg_customers.sql`, tells dbt "this table is not built by
dbt — it is external input, loaded outside the project by `load_to_duckdb.py`." `{{ ref('stg_transactions')
}}`, used in both `fct_daily_revenue.sql` and `dim_customers.sql`, tells dbt "this table is
another model in this same project, build it first." dbt uses that distinction to compile the
full dependency graph: sources are always leaf nodes, and every `ref()` becomes an edge in the
DAG that determines execution order.

This distinction matters for lineage reliability for two concrete reasons. First, correctness
of build order: because `dim_customers.sql` calls `ref('stg_customers')` and `ref('stg_transactions')`
instead of hardcoding `main.stg_transactions`, dbt guarantees both staging views exist before
`dim_customers` runs, even if models are added, renamed, or reordered later — a hardcoded table
name gives no such guarantee and can silently read stale or missing data. Second, traceability
of raw data: because the raw tables are declared as sources in `sources.yml` rather than
referenced by raw string, dbt can point at exactly where in the pipeline the data enters
(`load_to_duckdb.py` populating `raw_transactions`/`raw_customers`), and the `dbt docs` DAG can
visually distinguish "this is data DataShop's ingestion job produced" from "this is a
transformation dbt owns." That is exactly the single-source-of-truth problem this lab is
solving: instead of analysts writing ad-hoc SQL against the raw Parquet-backed tables with
inconsistent filters, every downstream model is forced through the same tested staging layer,
and the lineage graph makes that dependency chain auditable end to end.
