# Big Data Engineering Labs

Coursework for the Big Data Engineering course: building an e-commerce data platform
(ingestion, storage, batch/stream processing, orchestration, quality, and serving) over
a series of weekly labs, all sharing one synthetic DataShop dataset.

## Structure

```
.
├── bde_env/          # Core Python virtual environment (gitignored)
├── datashop_data/     # Shared generated seed dataset (gitignored, regenerate via lab01)
├── lab01/             # Environment setup, seed data generation, CLI exploration
├── lab02/             # CSV/Parquet/ORC storage benchmark; converts DataShop CSVs to Parquet
├── lab03/ ...          # Added as each week's lab is completed
```

Each `labNN/` folder is self-contained and includes its own `README.md` with results and
a short reflection.

## Setup

See `lab01/README.md` and `lab01/generate_datashop_data.py` to create the local virtual
environment and regenerate the shared dataset.
