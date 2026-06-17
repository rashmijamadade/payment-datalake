# RUNBOOK — Payment Hub Datalake (Local)

Step-by-step operator guide for running, verifying, and troubleshooting the pipeline on a local Windows machine.

---

## Prerequisites

| Requirement | Version | Check |
|---|---|---|
| Python | 3.10 or later | `python --version` |
| pip | any | `pip --version` |
| Git (optional) | any | `git --version` |
| Disk space | ~50 MB | For Parquet output |

> No Java, no Docker, no AWS account, no Spark required.

---

## 1. One-Time Setup

### 1.1 Navigate to the project root

```powershell
cd C:\Rashmi\python-medallion-coding-assignment-1\payment-datalake
```

All commands in this runbook are run from this directory.

### 1.2 Create and activate a virtual environment

```powershell
python -m venv .venv
.venv\Scripts\activate
```

You should see `(.venv)` in your prompt. If you skip this step, dependencies install globally.

### 1.3 Install dependencies

```powershell
pip install -r requirements.txt
```

Expected output (first run):
```
Successfully installed duckdb pandas pyarrow pyyaml pytest pytest-cov coverage
```

On re-runs: `Requirement already satisfied` for all packages — safe to ignore.

### 1.4 Confirm data files are in place

```powershell
dir data\payments\
dir data\merchants\
```

Expected files:
```
data\payments\payments_2024_01_15.csv
data\payments\payments_2024_01_15_resubmit.csv
data\payments\payments_2024_01_16.csv
data\merchants\merchants.csv
```

If missing, copy from the package folder:
```powershell
xcopy /E /I /Y ..\package\data data
```

---

## 2. Running the Pipeline

### 2.1 Full pipeline — Bronze + Gold (normal daily run)

```powershell
python run_pipeline.py --config config.yaml --stage all
```

**What happens:**
1. Reads all 3 CSV files from `data\payments\`
2. Validates schema; skips any file with missing required columns
3. Computes SHA-256 `record_hash` per row
4. Writes only net-new rows to `output\bronze\` (partitioned by `event_date`)
5. Writes a JSON manifest to `output\bronze\_manifests\`
6. Reads Bronze Parquet + `merchants.csv`
7. Writes `daily_payment_summary` and `merchant_performance_7d` to `output\gold\`
8. Writes observability reports to `output\run_reports\`

**Expected log output (first run):**
```
2024-xx-xx | INFO | Bronze | payments_2024_01_15.csv      read=15  written=15  skipped=0
2024-xx-xx | INFO | Bronze | payments_2024_01_15_resubmit read=17  written=2   skipped=15
2024-xx-xx | INFO | Bronze | payments_2024_01_16.csv      read=15  written=15  skipped=0
2024-xx-xx | INFO | Gold   | daily_payment_summary        rows_written=24
2024-xx-xx | INFO | Gold   | merchant_performance_7d      rows_written=16
```

---

### 2.2 Bronze only

```powershell
python run_pipeline.py --config config.yaml --stage bronze
```

Use this when:
- New CSV files arrive and you want to ingest before running Gold
- You want to check idempotency without touching Gold tables

---

### 2.3 Gold only

```powershell
python run_pipeline.py --config config.yaml --stage gold
```

Use this when:
- Bronze is already populated (run 2.1 or 2.2 first)
- You changed `config.yaml` (e.g. `window_days`) and want to recompute Gold without re-ingesting

> **Prerequisite:** Bronze must have data. If `output\bronze\` is empty, run Bronze first.

---

### 2.4 Verify idempotency — run the pipeline twice

```powershell
python run_pipeline.py --config config.yaml --stage all
python run_pipeline.py --config config.yaml --stage all
```

**Expected second-run output:**
```
Bronze | payments_2024_01_15.csv      read=15  written=0  skipped=15  ✅
Bronze | payments_2024_01_15_resubmit read=17  written=0  skipped=17  ✅
Bronze | payments_2024_01_16.csv      read=15  written=0  skipped=15  ✅
Gold   | daily_payment_summary        rows_written=24   (same count)  ✅
Gold   | merchant_performance_7d      rows_written=16   (same count)  ✅
```

Second run writes 0 new Bronze rows — Bronze count stays at 32.

---

### 2.5 Backfill — reprocess a specific date range

```powershell
python run_pipeline.py --config config.yaml --stage all `
    --mode backfill `
    --from_date 2024-01-15 `
    --to_date 2024-01-15
```

Use this when:
- Upstream corrected data for a specific date
- You want to reprocess only one partition

For a multi-day backfill:
```powershell
python run_pipeline.py --config config.yaml --stage all `
    --mode backfill `
    --from_date 2024-01-15 `
    --to_date 2024-01-16
```

---

### 2.6 Change the rolling window and recompute Gold

Edit `config.yaml`:
```yaml
gold:
  window_days: 30    # was 7
```

Then rerun Gold only:
```powershell
python run_pipeline.py --config config.yaml --stage gold
```

Output directory changes automatically to `merchant_performance_30d\`.
No code change needed.

---

## 3. Verifying Outputs

### 3.1 Check Bronze partitions were created

```powershell
dir output\bronze\
```

Expected:
```
output\bronze\event_date=2024-01-15\
output\bronze\event_date=2024-01-16\
output\bronze\_manifests\
```

### 3.2 Check Gold tables were created

```powershell
dir output\gold\
```

Expected:
```
output\gold\daily_payment_summary\event_date=2024-01-15\
output\gold\daily_payment_summary\event_date=2024-01-16\
output\gold\merchant_performance_7d\snapshot_date=2024-01-15\
output\gold\merchant_performance_7d\snapshot_date=2024-01-16\
```

### 3.3 Check observability reports

```powershell
dir output\run_reports\
```

Open the most recent report:
```powershell
Get-ChildItem output\run_reports\ | Sort-Object LastWriteTime -Descending | Select-Object -First 2
```

Each JSON file contains:
```json
{
  "run_id": "...",
  "stage": "bronze",
  "status": "SUCCESS",
  "duration_seconds": 0.23,
  "rows_read": 47,
  "rows_written": 32,
  "rows_quarantined": 0
}
```

### 3.4 Check the run manifest (Bronze audit trail)

```powershell
dir output\bronze\_manifests\
```

Open the latest manifest to see per-file row counts:
```powershell
Get-Content (Get-ChildItem output\bronze\_manifests\ | Sort-Object LastWriteTime -Descending | Select-Object -First 1 -ExpandProperty FullName)
```

### 3.5 Query output with DuckDB (ad-hoc)

Open a Python shell:
```powershell
python
```

```python
import duckdb

# Query Gold daily summary
conn = duckdb.connect()
df = conn.execute("SELECT * FROM read_parquet('output/gold/daily_payment_summary/**/*.parquet') ORDER BY event_date, merchant_id").df()
print(df.to_string())

# Query Bronze (all partitions)
bronze = conn.execute("SELECT event_date, COUNT(*) as rows FROM read_parquet('output/bronze/**/*.parquet') GROUP BY event_date").df()
print(bronze)

# Check merchant performance rolling window
perf = conn.execute("SELECT * FROM read_parquet('output/gold/merchant_performance_7d/**/*.parquet')").df()
print(perf.to_string())
```

---

## 4. Running Tests

### 4.1 Run all tests

```powershell
python -m pytest tests/ -v
```

Expected:
```
37 passed in ~2s
```

### 4.2 Run with coverage report

```powershell
python -m pytest tests/ -v --cov=src --cov-report=term-missing
```

### 4.3 Run a specific test file

```powershell
# Just idempotency tests
python -m pytest tests/test_bronze_idempotency.py -v

# Just Gold approval rate edge cases
python -m pytest tests/test_gold_approval_rate.py -v
```

### 4.4 Run a single test by name

```powershell
python -m pytest tests/test_bronze_idempotency.py::TestBronzeIngestionIdempotency::test_double_run_same_file_does_not_duplicate -v
```

---

## 5. Clean State — Start Fresh

If you want to wipe all generated output and re-run from scratch:

```powershell
# Remove all generated Parquet and JSON output
Remove-Item -Recurse -Force output\bronze\event_date=*
Remove-Item -Recurse -Force output\gold\daily_payment_summary\
Remove-Item -Recurse -Force output\gold\merchant_performance_*\
Remove-Item -Recurse -Force output\bronze\_manifests\*.json
Remove-Item -Recurse -Force output\run_reports\*.json
```

Then run the full pipeline again:
```powershell
python run_pipeline.py --config config.yaml --stage all
```

---

## 6. Troubleshooting

### `ModuleNotFoundError: No module named 'src'`

You are not in the project root. Fix:
```powershell
cd C:\Rashmi\python-medallion-coding-assignment-1\payment-datalake
python run_pipeline.py --config config.yaml --stage all
```

### `FileNotFoundError: config.yaml not found`

Pass the full path:
```powershell
python run_pipeline.py --config C:\Rashmi\python-medallion-coding-assignment-1\payment-datalake\config.yaml --stage all
```

### `No CSV file(s) found in ...`

Data files are missing. Run the copy command:
```powershell
xcopy /E /I /Y ..\package\data data
```

### Bronze shows `written=0` on first run

The output directory already has data from a previous run. This is correct behaviour (idempotency). If you want a fresh run, follow **Section 5** above.

### Gold fails with `Bronze DataFrame is empty`

Bronze has not been run yet, or the output was wiped. Run Bronze first:
```powershell
python run_pipeline.py --config config.yaml --stage bronze
python run_pipeline.py --config config.yaml --stage gold
```

### A test fails with `KeyError` or `ColumnNotFound`

Check that you are running Python 3.10+ and all dependencies are installed:
```powershell
python --version
pip list | findstr "duckdb pandas pyarrow"
pip install -r requirements.txt
```

### `--mode backfill` runs but writes 0 rows

All rows for those dates already exist in Bronze. This is expected — the hash dedup prevents duplicates. To verify the dates have data:
```python
import duckdb
duckdb.connect().execute(
    "SELECT event_date, COUNT(*) FROM read_parquet('output/bronze/**/*.parquet') GROUP BY 1"
).df()
```

---

## 7. Quick Reference Card

| Goal | Command |
|---|---|
| Full daily run | `python run_pipeline.py --stage all` |
| Bronze only | `python run_pipeline.py --stage bronze` |
| Gold only | `python run_pipeline.py --stage gold` |
| Verify idempotency | Run `--stage all` twice; second `written=0` |
| Backfill one date | `python run_pipeline.py --stage all --mode backfill --from_date 2024-01-15 --to_date 2024-01-15` |
| Change window to 30d | Edit `config.yaml` → `window_days: 30`, then `--stage gold` |
| Run all tests | `python -m pytest tests/ -v` |
| Run with coverage | `python -m pytest tests/ -v --cov=src --cov-report=term-missing` |
| Wipe and restart | Delete `output/bronze/event_date=*` and `output/gold/*/`, then re-run |
| Ad-hoc query | `python` → `import duckdb; duckdb.connect().execute("SELECT * FROM read_parquet('output/gold/**/*.parquet')").df()` |

---

## 8. Known Limitations

### Single-Writer Operation (Concurrent Run Assumption)

This prototype is designed for **single-writer** use. Running two pipeline instances concurrently against the same file will result in duplicate rows being written to Bronze, because the hash-filter check happens before new rows are committed.

**This is not a concern for normal daily operation** (one scheduled run per day). If you need to run multiple processes concurrently in a test environment, wipe the output first (Section 5) before re-running.

**Production** (AWS): this is resolved by Iceberg `MERGE INTO`, which is transactionally safe.

---

### Full Bronze Scan on Standard Gold Runs

When running Gold without a backfill date range (`--stage gold` with no `--mode backfill`), the pipeline reads **all** available Bronze partitions. For the 3-file prototype this is fast (< 0.5s), but as the Bronze store grows over months, each Gold run will read a progressively larger dataset.

**Workaround** (available now): Use `--mode backfill --from_date YYYY-MM-DD --to_date YYYY-MM-DD` to limit the Gold scan to only the affected date range.

**Production** (AWS): Iceberg partition statistics and Athena predicate pushdown eliminate this by only scanning matching partitions.

