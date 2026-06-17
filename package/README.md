# Payment Hub Datalake — Coding Assignment

Welcome. This package contains everything you need to complete the assignment.

---

## What's Inside

```
package/
├── ASSIGNMENT.md                       # Full assignment brief — read this first
├── README.md                           # This file
└── data/
    ├── payments/
    │   ├── payments_2024_01_15.csv         # Day 1 transactions (15 rows)
    │   ├── payments_2024_01_16.csv         # Day 2 transactions (15 rows)
    │   └── payments_2024_01_15_resubmit.csv  # Day 1 re-drop + 2 new rows (17 rows)
    └── merchants/
        └── merchants.csv                   # Merchant lookup table (8 merchants)
```

### About the data files

| File | Purpose |
|---|---|
| `payments_2024_01_15.csv` | Normal daily transaction drop |
| `payments_2024_01_16.csv` | Next day's transaction drop |
| `payments_2024_01_15_resubmit.csv` | A resubmit of the Jan 15 file **plus 2 new transactions**. All 15 original rows are identical — use this to verify your idempotency handling. |
| `merchants.csv` | Merchant reference data — join on `merchant_id` |

---

## Getting Started

### Prerequisites

Choose **one** of the following (justify your choice in `DESIGN.md`):

**Option A — PySpark**
```bash
pip install pyspark
```
Requires Java 8+ on your machine. Verify with: `java -version`

**Option B — DuckDB + Python**
```bash
pip install duckdb pandas pyarrow
```
No Java required. Lighter-weight and fully local.

Either choice is acceptable. We are more interested in how you structure the solution than the engine you pick.

---

## Suggested First Steps

1. **Read `ASSIGNMENT.md` in full** before writing any code
2. **Write `DESIGN.md` first** — answer the 4 design questions before touching code
3. **Start with Bronze** — get the ingestion working and verified before moving to Gold
4. **Test idempotency early** — run your Bronze ingestion twice on the same file and check row counts

---

## Submission

Submit a Git repository (public GitHub, GitLab, or a zip archive) containing:
- Your source code
- `DESIGN.md` (your design decisions)
- `README.md` (setup and run instructions for your solution)
- Your tests

Include the `data/` directory or reference these provided files in your README.  
Do **not** commit any credentials, API keys, or cloud account details.

---

## Questions

If anything is ambiguous, make a reasonable assumption, document it in `DESIGN.md`, and proceed.  
We are as interested in how you handle ambiguity as we are in the final output.
