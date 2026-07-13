# Lab 05: Building an Apache Iceberg Lakehouse

## Reflection: Why Time Travel Matters in a Production Lakehouse

Time travel is valuable because it turns a data lake from a mutable, single-state system
into an auditable one: every write produces a new immutable snapshot instead of overwriting
history, so analysts and engineers can always query the table exactly as it existed at any
prior point in time. In a plain Parquet-based lake, once a bad write lands there is no built-in
way to see what the data looked like before it — you either have a manual backup or the prior
state is gone for good. With Iceberg, that prior state is just another snapshot ID away, which
makes debugging, auditing, and recovering from bad writes fast and low-risk rather than a
best-effort recovery exercise.

A concrete DataShop scenario: suppose the nightly ETL job that loads transactions accidentally
runs twice, double-counting a batch of March transactions before anyone notices. Finance
already pulled a revenue report that morning based on the corrupted table. Instead of trying to
reconstruct what March revenue "should" have looked like, a data engineer can run a `VERSION AS
OF` query against the snapshot committed right before the duplicate load ran, compare it to the
current (corrupted) state to confirm exactly which rows were duplicated, and then use that
verified snapshot to correct the table or reissue the report — the same technique demonstrated
in Exercise 2, where querying Snapshot 1 reproduced the exact January-only row count even after
two more months of data had been appended.
