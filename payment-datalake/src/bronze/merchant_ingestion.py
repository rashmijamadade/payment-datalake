"""
merchant_ingestion.py  (Bronze layer)
--------------------------------------
Orchestrates the Bronze ingestion pipeline for the merchant CSV lookup file.

This mirrors the payments ingestion pattern so both datasets land in the
Bronze zone with the same quality guarantees:
  1. Read the merchant CSV                → pandas read_csv (all strings)
  2. Validate schema                      → schema_validator.validate_schema()
  3. Cast types                           → _cast_merchant_types()
  4. Enrich with metadata                 → metadata_enricher.enrich()
  5. Dedup against existing Bronze        → deduplicator.load_existing_hashes() + filter_new_rows()
  6. Write to Parquet (single partition)  → _write_merchant_bronze()
  7. Write run manifest                   → manifest_writer.write_manifest()
  8. Emit observability report            → observability.PipelineObserver

Merchant data is small and static (no time-series partitioning), so it is
stored as a flat Parquet file under output/bronze_merchants/ rather than
date-partitioned.  A record_hash on (merchant_id + all business columns)
provides idempotent deduplication across runs.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.common.config_loader import Config
from src.common.schema_validator import validate_schema
from src.common.metadata_enricher import enrich
from src.bronze.deduplicator import filter_new_rows
from src.common.manifest_writer import write_manifest
from src.common.observability import PipelineObserver

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema definition (mirrors config.yaml merchants.required_columns)
# ---------------------------------------------------------------------------

# Canonical column data types after initial CSV read-as-string
_MERCHANT_DTYPE_MAP: Dict[str, str] = {
    "merchant_id":          "string",
    "merchant_name":        "string",
    "merchant_category":    "string",
    "country":              "string",
    "settlement_currency":  "string",
}


# ---------------------------------------------------------------------------
# Type-casting helper
# ---------------------------------------------------------------------------

def _cast_merchant_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast merchant columns to their correct types after CSV read-as-string.

    Conversions applied:
      - onboarding_date  → datetime64 (date only, UTC-aware)
      - is_active        → boolean   (handles 'true'/'false' strings)
      - All other columns remain as string (merchant_id, names, codes)
    """
    df = df.copy()

    if "onboarding_date" in df.columns:
        df["onboarding_date"] = pd.to_datetime(
            df["onboarding_date"], errors="coerce"
        ).dt.date  # store as plain date (no TZ needed for a lookup table)

    if "is_active" in df.columns:
        # CSV stores 'true'/'false' strings → proper bool
        df["is_active"] = df["is_active"].str.strip().str.lower().map(
            {"true": True, "false": False}
        )

    return df


# ---------------------------------------------------------------------------
# Parquet writer
# ---------------------------------------------------------------------------

def _write_merchant_bronze(df: pd.DataFrame, bronze_merchants_dir: Path) -> int:
    """
    Write *df* to the Merchant Bronze Parquet store.

    Uses a timestamped filename so concurrent runs don't overwrite each other.
    Dedup has already removed duplicate hashes, so only net-new rows land here.

    Returns the number of rows written.
    """
    if df.empty:
        logger.warning("No new merchant rows to write — skipping.")
        return 0

    bronze_merchants_dir.mkdir(parents=True, exist_ok=True)
    ts_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    out_path = bronze_merchants_dir / f"merchants-{ts_str}.parquet"

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path)
    logger.debug("Wrote %d merchant rows → %s", len(df), out_path)
    return len(df)


# ---------------------------------------------------------------------------
# Main ingestion entry point
# ---------------------------------------------------------------------------

def run_merchant_bronze_ingestion(
    config: Config,
    run_id: Optional[str] = None,
) -> Dict:
    """
    Run the full Bronze ingestion pipeline for the merchant CSV file.

    Parameters
    ----------
    config:
        Loaded pipeline configuration.
    run_id:
        Unique run identifier. Auto-generated UUID if None.

    Returns
    -------
    dict
        Summary with run_id, rows_read, rows_written, rows_skipped, status.
    """
    run_id = run_id or str(uuid.uuid4())
    merchants_csv_path = config.resolve(config.paths.input_merchants)
    bronze_merchants_dir = config.resolve(config.paths.output_bronze_merchants)
    manifests_dir = config.resolve(config.paths.bronze_merchants_manifests)
    run_reports_dir = config.resolve(config.paths.run_reports)

    source_name = merchants_csv_path.name  # e.g. "merchants.csv"

    observer = PipelineObserver(
        run_id=run_id, stage="bronze_merchants", output_dir=run_reports_dir
    )
    overall_status = "SUCCESS"

    with observer.track():
        # ── Step 1: Read merchant CSV ─────────────────────────────────────
        if not merchants_csv_path.exists():
            logger.error("Merchant file not found: %s", merchants_csv_path)
            observer.mark_failed()
            return {
                "run_id": run_id,
                "rows_read": 0,
                "rows_written": 0,
                "rows_skipped": 0,
                "status": "FAILED",
            }

        df_raw = pd.read_csv(merchants_csv_path, dtype=str, keep_default_na=False)
        logger.info("Merchant Bronze | read %d rows from '%s'.", len(df_raw), source_name)

        # ── Step 2: Schema validation ──────────────────────────────────────
        validation = validate_schema(
            df_raw, config.merchants.required_columns, source_name
        )
        if not validation:
            logger.error(
                "Merchant schema validation FAILED — skipping ingestion. %s",
                validation.error_message,
            )
            observer.record(
                source_name, rows_read=len(df_raw), rows_quarantined=len(df_raw)
            )
            return {
                "run_id": run_id,
                "rows_read": len(df_raw),
                "rows_written": 0,
                "rows_skipped": len(df_raw),
                "status": "FAILED",
            }

        # ── Step 3: Type casting ───────────────────────────────────────────
        df_typed = _cast_merchant_types(df_raw)

        # ── Step 4: Metadata enrichment ───────────────────────────────────
        df_enriched = enrich(
            df_typed,
            source_file=source_name,
            run_id=run_id,
            hash_exclude_columns=config.merchants.hash_exclude_columns,
        )

        # ── Step 5: Dedup against existing Bronze ─────────────────────────
        # Merchant data has no date partitions — scan all parquet files directly.
        existing_hashes: set = set()
        if bronze_merchants_dir.exists():
            for pq_file in bronze_merchants_dir.glob("*.parquet"):
                try:
                    h_df = pd.read_parquet(pq_file, columns=["record_hash"])
                    existing_hashes.update(h_df["record_hash"].tolist())
                except Exception as exc:
                    logger.warning("Could not read hashes from '%s': %s", pq_file, exc)
        logger.debug("Loaded %d existing merchant record_hash(es).", len(existing_hashes))
        df_new = filter_new_rows(df_enriched, existing_hashes)

        rows_read = len(df_raw)
        rows_skipped = rows_read - len(df_new)

        # ── Step 6: Write to Parquet ──────────────────────────────────────
        rows_written = _write_merchant_bronze(df_new, bronze_merchants_dir)

        observer.record(
            source_name,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_quarantined=0,
        )
        logger.info(
            "Merchant Bronze | '%s': read=%d written=%d skipped=%d",
            source_name,
            rows_read,
            rows_written,
            rows_skipped,
        )

    # ── Step 7: Run manifest ──────────────────────────────────────────────
    write_manifest(
        run_id=run_id,
        files_processed=[source_name],
        row_counts={
            source_name: {
                "read": rows_read,
                "written": rows_written,
                "skipped": rows_skipped,
            }
        },
        status=overall_status,
        output_dir=manifests_dir,
    )

    return {
        "run_id": run_id,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rows_skipped": rows_skipped,
        "status": overall_status,
    }
