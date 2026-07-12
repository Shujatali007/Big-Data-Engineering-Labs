# Lab 04: Spark Performance Tuning — Broadcast Joins, AQE, and Partitioning

## Contents

- `verify_data.py` — confirms `datashop_transactions.parquet` and `datashop_products.parquet`
  from Lab 02 are present before running the exercises.
- `pipeline_baseline.py` — unoptimised pipeline: AQE and broadcast joins disabled, forcing a
  sort-merge join between the ~100K-row transaction table and the 100-row product table.
- `pipeline_optimised.py` — same pipeline with three optimisations applied: a `broadcast()`
  hint on the product table, AQE enabled (skew join + partition coalescing), and column
  pruning on the product read. Writes the result partitioned by `sale_date` to
  `datashop_data/daily_summary/`.

## Results

| Pipeline  | Join strategy    | Elapsed time | Output rows |
|-----------|-------------------|--------------|-------------|
| Baseline  | SortMergeJoin     | 5.34s        | 3,475       |
| Optimised | BroadcastHashJoin | 2.64s        | 3,475       |

Both pipelines produce identical output (3,475 rows), confirming the optimisations changed
*how* the result was computed, not *what* was computed. The optimised pipeline ran about 2x
faster on this dataset (100K rows on a single laptop) — a modest but real speedup, consistent
with the lab's note that dramatic gains (broadcast avoiding a 45-minute shuffle) only show up at
production scale.

**Execution plan confirmation:** the baseline plan shows `SortMergeJoin` with two `Exchange
hashpartitioning` shuffles (one per side of the join); the optimised plan shows
`BroadcastHashJoin` with a single `BroadcastExchange` and no shuffle on the transaction side —
the join partner is sent whole to every executor instead of both sides being shuffled and
sorted.

**Partitioned output:** `datashop_data/daily_summary/` contains 695 `sale_date=YYYY-MM-DD`
subdirectories, matching the ~694 distinct dates expected from 100,000 transactions spaced 10
minutes apart starting 2024-01-01.

## Reflection: broadcast join vs. sort-merge join

**Why broadcast is faster here.** A sort-merge join has to guarantee that rows with the same
join key end up on the same executor, and the only way Spark can guarantee that for two
arbitrary, unsorted datasets is to **shuffle both sides** across the network by key and then
**sort** each partition before merging matches — that's the two `Exchange hashpartitioning`
steps visible in the baseline plan. The transactions table has ~100K rows, so most of the join
cost is spent shuffling and sorting *that* table, even though the only reason a shuffle is needed
at all is to align it with a 100-row lookup table. A broadcast join sidesteps the problem
entirely by not requiring alignment through shuffling: Spark copies the *entire* small table
(`datashop_products.parquet`, 100 rows) to every executor's memory once (`BroadcastExchange`),
and each executor then joins its already-local slice of the transactions table against that
in-memory copy. The large side never moves, and nothing gets sorted. Fewer bytes over the
network and one less algorithmic step (`Sort`) is exactly why the optimised pipeline finished in
2.64s versus the baseline's 5.34s despite computing an identical result.

**Scenario 1 — the ideal case (what this lab demonstrates).** A large fact table joined against
a small, mostly-static dimension table (products, customers, a currency-rate lookup, a country
code table) that comfortably fits in a few MB to tens of MB. This is the textbook use case:
broadcast cost is nearly free relative to the shuffle it replaces, and it's why Spark
auto-broadcasts any table under `spark.sql.autoBroadcastJoinThreshold` (10MB by default) without
even needing an explicit hint.

**Scenario 2 — the "small" table quietly grows.** Suppose the product catalog grows from 100
SKUs to 50 million rows (a plausible catalog size for a large multi-vendor marketplace) or the
lookup table is replaced with something several GB in size, but the `broadcast()` hint is left in
the code out of habit. Spark will try to materialize the *whole* table in the memory of *every*
executor simultaneously. On a cluster with, say, 50 executors, that's 50 redundant copies of a
multi-GB table sitting in memory at once. Best case, this just wastes memory that could have gone
to caching or larger shuffle buffers. Worst case, it triggers `OutOfMemoryError` on the driver
(which must first collect the table to broadcast it) or on executors, and the job fails outright
rather than merely running slowly — an explicit broadcast hint is Spark following your
instruction even when it's a bad idea, unlike auto-broadcast, which will not kick in past the
size threshold.

**Scenario 3 — skewed or unpredictable data volume.** If the "small" side is the output of an
upstream filter or join whose size can't be known in advance (e.g., "products discounted this
week" computed dynamically), hardcoding a `broadcast()` hint is risky because the assumption it
encodes ("this is always small") can silently become false as upstream data changes, with no
warning until a production job suddenly OOMs. This is precisely the scenario **AQE** is designed
for: with AQE enabled and no hint at all, Spark can look at *actual* post-shuffle partition
statistics at runtime and decide to broadcast (or not) based on real size rather than a
hardcoded assumption — a safer default than a hand-written hint for tables whose size may drift.

**Scenario 4 — both tables are large.** If neither side fits comfortably in executor memory
(e.g., joining two multi-hundred-million-row fact tables), broadcast is never appropriate
regardless of hints — a sort-merge join (or shuffle hash join) is the only correct strategy,
because the whole point of shuffling is to distribute a large dataset across the cluster rather
than concentrating it. In this case the baseline pipeline's approach — plain sort-merge join — is
not a "slow mistake" to be optimised away; it's the right tool for that shape of problem.

**Rule of thumb:** broadcast joins trade network/shuffle cost for memory cost. That trade is a
clear win when one side is small and stable (Scenario 1), a silent liability when one side is
small today but not guaranteed to stay that way (Scenarios 2–3), and simply inapplicable when
both sides are genuinely large (Scenario 4). Prefer relying on Spark's automatic broadcast
threshold plus AQE over hardcoding `broadcast()`, and reserve the explicit hint for tables you
are certain will remain small — typically low tens of MB — for the lifetime of the pipeline.
