# Coding Assignment: Payment Hub Datalake Pipeline

**Role**: Senior Data Engineer  
**Domain**: Payments / Financial Data  
**Expected Effort**: 1–2 days  
**Submission**: A zip file with your code, tests, and a design document

---

## Context

You are joining a team building a cloud-native **Payment Hub Datalake** on AWS. Your first task is to design and implement a local prototype of the core ingestion pipeline — from raw payment files through to analytics-ready Gold tables — covering the **Bronze** (raw ingestion) and **Gold** (aggregated analytics) layers.

We are evaluating you on **how you think and design**, not just whether the code runs. A working but monolithic script scores lower than a well-structured, modular solution that covers the hard cases.

> **On AI tools**: You are free to use any tools, including AI assistants. However, we will ask you to walk through your design decisions in a follow-up discussion. Your ability to explain *why* you made specific architectural choices matters more than the code itself.

---

## Compute Choice

Use **local PySpark** or **DuckDB + Python** — no AWS account required.  
You must clearly state your choice in your design document and justify it.

---

## Datasets

**Sample data is provided** in the `data/` directory of this package. It includes:
- `payments/` — 3 CSV files: two daily drops and one resubmit file (overlapping transaction IDs to test dedup)
- `merchants/` — 1 CSV file used for merchant lookups in the Gold layer

### `payments_raw.csv` — Schema

| Column | Type | Notes |
|---|---|---|
| `transaction_id` | string | Globally unique per transaction |
| `merchant_id` | string | FK to merchants |
| `customer_id` | string | Internal customer reference — **PII** |
| `customer_email` | string | **PII** |
| `customer_phone` | string | **PII** |
| `amount` | decimal(18,2) | Transaction amount |
| `currency` | string | ISO 4217 (e.g. USD, GBP, EUR) |
| `status` | string | `APPROVED`, `DECLINED`, `REVERSED`, `PENDING` |
| `transaction_ts` | timestamp | UTC |
| `payment_method` | string | `CARD`, `WALLET`, `BANK_TRANSFER` |
| `card_last4` | string | **Sensitive** — last 4 digits only |
| `gateway_ref` | string | External reference |
| `source_file` | string | Filename — populated at ingestion |

### `merchants.csv` — Schema

| Column | Type | Notes |
|---|---|---|
| `merchant_id` | string | PK |
| `merchant_name` | string | |
| `merchant_category` | string | MCC category (e.g. `RETAIL`, `FOOD`, `TRAVEL`) |
| `country` | string | ISO 3166-1 alpha-2 |
| `onboarding_date` | date | |
| `is_active` | boolean | |
| `settlement_currency` | string | |

---

## Tasks

### Part 1 — Design Document (Required)

**Before writing any code**, produce a `DESIGN.md` (max 2 pages) covering:

1. **Architecture decision**: Why did you choose PySpark vs DuckDB for this problem? What would change your answer at production scale on AWS?
2. **Layer contracts**: For each layer (Bronze / Silver / Gold), define: the grain, the primary key strategy, and the write mode (append / overwrite / merge). Justify each.
3. **Idempotency strategy**: How do you ensure that re-running the same pipeline twice does not duplicate or corrupt data? Be specific — describe the mechanism, not just the goal.
4. **Trade-off you made**: Identify one design choice where you deliberately prioritised one quality (e.g. simplicity, performance, correctness) over another. Explain the reasoning.

> This section is evaluated on **depth of thinking**, not length. Bullet points are fine.

---

### Part 2 — Bronze Layer

**Goal**: Ingest raw payment CSV files into a Bronze table with full auditability and idempotent re-run support.

#### Requirements

- Read all CSV files from `data/payments/` and write to a Bronze Parquet store (partitioned by `event_date` derived from `transaction_ts`)
- Add the following **metadata columns** to every row:

| Column | Value |
|---|---|
| `ingest_ts` | Current timestamp when the record was written |
| `ingest_run_id` | Unique ID per pipeline run (caller-supplied or generated) |
| `source_file` | Filename of the originating CSV |
| `record_hash` | SHA-256 hash of the core payload (exclude metadata columns) |

- **Idempotency**: Re-running on the same file must not produce duplicate rows. Implement and demonstrate this.
- **Schema validation**: If an input file is missing a required column, log a clear error and skip that file (do not crash the entire run).
- **Run manifest**: After each run, write a small JSON manifest to `output/bronze/_manifests/` recording: `run_id`, `files_processed`, `row_counts`, `status`, `run_ts`.

#### What we are evaluating here

- Is the ingestion logic separated from file I/O?
- Is the schema validation reusable for other datasets?
- Is the metadata enrichment a composable step?

---

### Part 3 — Gold Layer

**Goal**: Produce analytics-ready aggregated tables for BI/reporting consumption.

#### Table 1: `daily_payment_summary`

Grain: one row per `(event_date, merchant_id, currency, status)`

| Column | Description |
|---|---|
| `event_date` | Date of transactions |
| `merchant_id` | |
| `merchant_name` | From current merchant record |
| `merchant_category` | From current merchant record |
| `currency` | |
| `status` | |
| `transaction_count` | |
| `total_amount` | Sum of amounts |
| `avg_amount` | |
| `max_amount` | |
| `approval_rate` | `APPROVED / (APPROVED + DECLINED)` — only meaningful for card-initiated transactions |

This table must be **idempotent**: re-running for a date range must produce the same result.

#### Table 2: `merchant_performance_7d`

Grain: one row per `(snapshot_date, merchant_id)`  
Rolling 7-day window ending on `snapshot_date`.

| Column | Description |
|---|---|
| `snapshot_date` | Date of the snapshot |
| `merchant_id` | |
| `merchant_name` | |
| `total_transactions_7d` | |
| `total_approved_amount_7d` | |
| `approval_rate_7d` | |
| `reversal_rate_7d` | `REVERSED / total` |
| `active_days_7d` | Number of distinct days with at least one transaction |

#### What we are evaluating here

- Is the Gold transformation readable and separated from Bronze reading?
- Are aggregation functions reusable (e.g. could you add a 30-day version without copy-pasting)?
- Is the approval_rate calculated correctly, including null-safety for division by zero?

---

### Part 4 — Tests

Provide unit tests (pytest or equivalent) covering **at minimum**:

1. Bronze idempotency — run ingestion twice on the same file, assert row count does not double
2. Bronze schema validation — a file missing a required column is skipped, pipeline does not crash
3. Bronze metadata — output contains `ingest_ts`, `ingest_run_id`, `record_hash` columns
4. Gold `approval_rate` calculation — test edge case: all transactions are `DECLINED` (division by zero guard)
5. Gold row count — for a known input, assert the number of output rows matches the expected grain

You do **not** need to test every function — demonstrate judgment in what is worth testing.

---

## Bonus Challenges (Senior Differentiators)

Complete these only if you have time — they are not required to pass.

### B1 — Observability Framework
After each pipeline run, emit a structured JSON run report to `output/run_reports/` containing:
- `run_id`, `pipeline_stage`, `start_ts`, `end_ts`, `duration_seconds`
- Per-table: `rows_read`, `rows_written`, `rows_quarantined`
- `status`: `SUCCESS`, `PARTIAL` (some quarantined), `FAILED`

Make this reusable: it should work for Bronze and Gold stages without duplicating logic.

### B2 — Backfill Mode
Add a CLI argument `--mode=backfill --from_date=YYYY-MM-DD --to_date=YYYY-MM-DD` that re-processes only the specified date range, overwriting the target partitions. Ensure the Gold layer is also re-derived for the affected dates.

### B3 — Schema Evolution
Simulate a breaking schema change: add a new nullable column `payment_network` to the payments CSV in a later file drop.  
Demonstrate that your Bronze ingestion handles this without crashing. Write a brief note in `DESIGN.md` on how you would handle a **non-backward-compatible** change (column rename or type change) in a production Iceberg-based system.

### B4 — Configuration-Driven Pipeline
Externalise all pipeline configuration (partition keys, file paths, layer settings) into a single `config.yaml` or `config.json`. The pipeline should be runnable with `python run_pipeline.py --config config.yaml --stage all` with no hardcoded values in pipeline code.

---

## Project Structure (Suggested — not enforced)

```
payment-datalake/
├── DESIGN.md                   # Your design document (Part 1)
├── README.md                   # How to run locally
├── config.yaml                 # (Bonus B4) Pipeline configuration
├── data/
│   ├── payments/               # Raw input CSV files
│   └── merchants/              # Merchant lookup data
├── src/
│   ├── bronze/                 # Bronze ingestion logic
│   ├── gold/                   # Gold aggregation logic
│   └── common/                 # Shared utilities (schema, manifests, logging)
├── tests/
│   └── ...
├── output/                     # Generated data (gitignore the data, keep structure)
└── run_pipeline.py             # (Optional) Entry point
```

> You are not required to follow this structure. A different structure that is equally or more logical is fine — but explain it in your `README.md`.

---

## Submission Checklist

Before submitting, verify:

- [ ] `DESIGN.md` answers all 4 questions in Part 1
- [ ] Bronze ingestion is idempotent (re-run produces same row count)
- [ ] Gold tables are populated and queryable
- [ ] All 4 required tests pass
- [ ] `README.md` contains clear local setup + run instructions
- [ ] No credentials, secrets, or AWS account details in the repo

---

## Evaluation Rubric

| Area | Weight | What we look for |
|---|---|---|
| **Code modularity & reusability** | 35% | Functions/classes with single responsibility; no copy-paste across layers; shared utilities extracted; logic reusable across tables |
| **Idempotency & correctness** | 25% | Bronze and Gold safe to re-run; dedup logic handles cross-run duplicates; re-running same file does not double rows |
| **Data modelling** | 20% | Correct grain for each Gold table; approval_rate edge cases handled; partition strategy justified |
| **Testing** | 15% | Tests cover behaviour not just happy paths; idempotency tested; edge cases covered |
| **Design document & trade-offs** | 5% | Answers are specific and defensible; trade-off is real (not generic); would hold up in a design review |

### What would immediately downgrade a submission

- Monolithic script with all logic in one file
- Pipeline fails or produces wrong results on second run (non-idempotent)
- No tests, or tests that only assert the code runs without asserting correctness
- Design document that describes *what* the code does rather than *why* decisions were made

### What would immediately upgrade a submission to Senior level

- Observability framework (Bonus B1) implemented cleanly as a cross-cutting concern
- `DESIGN.md` contains a specific, honest trade-off with reasoning — not generic best-practice language
- Aggregation logic is parameterised (e.g. the 7-day window can become 30-day with one config change)

---

## Questions

If anything is ambiguous, make a reasonable assumption, document it in `DESIGN.md`, and proceed. We are as interested in how you handle ambiguity as we are in the final output.
