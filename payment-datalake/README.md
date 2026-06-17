# Payment Hub Datalake — Local Setup & Run Guide

A local prototype of a cloud-native medallion-architecture data pipeline for payment processing.

**Engine**: DuckDB + Python | **Layers**: Bronze (raw ingestion) → Gold (analytics)

---

## Project Structure

```
payment-datalake/
├── DESIGN.md                   # Architecture decisions & trade-offs (Part 1)
├── README.md                   # This file
├── config.yaml                 # All pipeline configuration (Bonus B4)
├── requirements.txt
├── run_pipeline.py             # CLI entry point
├── data/
│   ├── payments/               # Input CSV files (copy from package/data/)
│   └── merchants/
├── src/
│   ├── common/                 # Shared utilities (schema, metadata, manifest, observability)
│   ├── bronze/                 # Bronze ingestion (reader, deduplicator, orchestrator)
│   └── gold/                   # Gold aggregations (reader, aggregations, orchestrator)
├── tests/                      # pytest test suite (5 test files)
└── output/                     # Generated data (gitignored; structure committed)
    ├── bronze/
    │   └── _manifests/
    ├── gold/
    └── run_reports/
```

---

## Prerequisites

- Python 3.10+
- No Java, no Spark, no cloud account required

```bash
# Create a virtual environment (recommended)
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## Data Setup

Copy the provided sample data into the project:

```bash
# From the project root (payment-datalake/)
# Windows
xcopy /E /I ..\package\data data

# macOS / Linux
cp -r ../package/data ./data
```

Or adjust `config.yaml` paths to point directly to `package/data/`.

---

## Running the Pipeline

### Run all stages (Bronze + Gold)

```bash
python run_pipeline.py --config config.yaml --stage all
```

### Run only Bronze ingestion

```bash
python run_pipeline.py --config config.yaml --stage bronze
```

### Run only Gold transformation

```bash
python run_pipeline.py --config config.yaml --stage gold
```

### Verify idempotency (run twice — row count must not change)

```bash
python run_pipeline.py --config config.yaml --stage bronze
python run_pipeline.py --config config.yaml --stage bronze
# Check output/bronze/_manifests/*.json — row counts identical
```

### Backfill mode (Bonus B2)

Re-process a specific date range only:

```bash
python run_pipeline.py --config config.yaml --stage all \
    --mode backfill --from_date 2024-01-15 --to_date 2024-01-16
```

---

## Running Tests

```bash
# All tests with verbose output
pytest tests/ -v

# With coverage report
pytest tests/ -v --cov=src --cov-report=term-missing
```

### Test Suite (5 required tests)

| Test File | What It Tests |
|---|---|
| `test_bronze_idempotency.py` | Double-run produces same row count; resubmit adds only new rows |
| `test_bronze_schema.py` | Missing required column → file skipped, pipeline continues |
| `test_bronze_metadata.py` | `ingest_ts`, `ingest_run_id`, `record_hash` columns present and correct |
| `test_gold_approval_rate.py` | All-DECLINED → 0.0; no-CARD → NULL; empty → no crash |
| `test_gold_grain.py` | Known input → correct number of grain rows; no duplicates |

---

## Pipeline Output

After running, the following are generated:

```
output/
├── bronze/
│   ├── event_date=2024-01-15/part-<timestamp>.parquet
│   ├── event_date=2024-01-16/part-<timestamp>.parquet
│   └── _manifests/<run_id>.json          # Run audit record
├── gold/
│   ├── daily_payment_summary/
│   │   ├── event_date=2024-01-15/part-0.parquet
│   │   └── event_date=2024-01-16/part-0.parquet
│   └── merchant_performance_7d/
│       ├── snapshot_date=2024-01-15/part-0.parquet
│       └── snapshot_date=2024-01-16/part-0.parquet
└── run_reports/
    ├── <run_id>_bronze.json              # Observability report (Bonus B1)
    └── <run_id>_gold.json
```

### Querying Output with DuckDB

```python
import duckdb

# Query Gold daily summary
conn = duckdb.connect()
conn.execute("SELECT * FROM read_parquet('output/gold/daily_payment_summary/**/*.parquet')").df()

# Query Bronze (all partitions)
conn.execute("SELECT * FROM read_parquet('output/bronze/**/*.parquet')").df()
```

---

## Configuration

All pipeline settings are in `config.yaml` — zero hardcoded values in source code.

Key settings:

| Setting | Default | Effect |
|---|---|---|
| `gold.window_days` | `7` | Change to `30` for a 30-day rolling window — no code change |
| `gold.overwrite_partitions` | `true` | Gold idempotency strategy |
| `bronze.required_columns` | see config | Schema validation whitelist |

---

## Design Decisions

See [`DESIGN.md`](DESIGN.md) for full answers to:
1. Why DuckDB over PySpark (and when to switch)
2. Layer contracts: grain, primary key, write mode for Bronze and Gold
3. Idempotency mechanism: SHA-256 hash dedup in Bronze; partition overwrite in Gold
4. Trade-off: merged Bronze+Silver for prototype scope

---

## Assumptions Made

1. **No Silver layer** — Bronze enriched with metadata serves Silver's purpose at this scale. Documented in DESIGN.md.
2. **UTC timestamps** — all `transaction_ts` values treated as UTC; `event_date` derived accordingly.
3. **approval_rate scope** — computed over CARD-initiated transactions only; WALLET/BANK_TRANSFER excluded. Returns NULL (not 0) when no eligible transactions exist.
4. **Merchant join** — left join; transactions without a matching merchant retain null `merchant_name`/`merchant_category`.
5. **CSV read-as-string** — all columns read as strings at I/O layer, then cast explicitly — avoids pandas type inference surprises.
