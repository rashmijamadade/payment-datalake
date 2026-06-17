"""
ingestion.py  (Bronze layer)
-----------------------------
Orchestrates the full Bronze ingestion pipeline for payment CSV files.

Processing steps (each delegated to a single-responsibility module):
  1. Discover CSV files             → reader.discover_csv_files()
  2. Read each CSV                  → reader.read_csv()
  3. Validate schema                → schema_validator.validate_schema()  [skip on failure]
  4. Cast types                     → _cast_types()
  5. Derive event_date partition    → _derive_event_date()
  6. Enrich with metadata           → metadata_enricher.enrich()
  7. Dedup against existing Bronze  → deduplicator.load_existing_hashes() + filter_new_rows()
  8. Write to Parquet by partition  → _write_partition()
  9. Write run manifest             → manifest_writer.write_manifest()
 10. Emit observability report      → observability.PipelineObserver

Schema evolution (Bonus B3): extra columns in incoming files are preserved;
missing optional columns are silently filled with None.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.common.config_loader import Config
from src.bronze.reader import stream_csv_files
from src.bronze.deduplicator import load_existing_hashes, filter_new_rows
from src.common.schema_validator import validate_schema
from src.common.metadata_enricher import enrich
from src.common.manifest_writer import write_manifest
from src.common.observability import PipelineObserver

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type casting helpers
# ---------------------------------------------------------------------------

def _cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to their correct types after CSV read-as-string."""
    df = df.copy()
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    if "transaction_ts" in df.columns:
        df["transaction_ts"] = pd.to_datetime(df["transaction_ts"], utc=True, errors="coerce")
    return df


def _derive_event_date(df: pd.DataFrame) -> pd.DataFrame:
    """Add event_date (YYYY-MM-DD string) derived from transaction_ts."""
    df = df.copy()
    df["event_date"] = df["transaction_ts"].dt.strftime("%Y-%m-%d")
    return df


# ---------------------------------------------------------------------------
# Partition writer
# ---------------------------------------------------------------------------

def _write_partition(df: pd.DataFrame, bronze_dir: Path, event_date: str) -> int:
    """
    Write *df* to the Bronze Parquet store under the given event_date partition.
    Uses append semantics — dedup has already filtered to net-new rows.
    Returns number of rows written.
    """
    if df.empty:
        return 0

    partition_dir = bronze_dir / f"event_date={event_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    # Use a timestamp-based file name to avoid collisions across runs
    ts_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    out_path = partition_dir / f"part-{ts_str}.parquet"

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path)
    logger.debug("Wrote %d rows → %s", len(df), out_path)
    return len(df)


# ---------------------------------------------------------------------------
# Main ingestion entry point
# ---------------------------------------------------------------------------

def run_bronze_ingestion(
    config: Config,
    run_id: Optional[str] = None,
    backfill_dates: Optional[List[str]] = None,
) -> Dict:
    """
    Run the full Bronze ingestion pipeline.

    Parameters
    ----------
    config:
        Loaded pipeline configuration.
    run_id:
        Unique run identifier. Auto-generated UUID if None.
    backfill_dates:
        If supplied (Bonus B2), only process files whose event_date falls
        within this list. None means process all discovered files.

    Returns
    -------
    dict
        Summary with run_id, files_processed, row_counts, status.
    """
    run_id = run_id or str(uuid.uuid4())
    bronze_dir = config.resolve(config.paths.output_bronze_payments)
    manifests_dir = config.resolve(config.paths.bronze_payments_manifests)
    run_reports_dir = config.resolve(config.paths.run_reports)
    input_dir = config.resolve(config.paths.input_payments)

    observer = PipelineObserver(run_id=run_id, stage="bronze", output_dir=run_reports_dir)
    files_processed: List[str] = []
    row_counts: Dict[str, Dict[str, int]] = {}
    overall_status = "SUCCESS"

    with observer.track():
        for df_raw, filename in stream_csv_files(input_dir, config.payments.file_pattern):
            files_processed.append(filename)

            # ── Step 3: Schema validation ─────────────────────────────────
            validation = validate_schema(df_raw, config.payments.required_columns, filename)
            if not validation:
                logger.error("Skipping '%s': %s", filename, validation.error_message)
                row_counts[filename] = {"read": len(df_raw), "written": 0, "skipped": len(df_raw)}
                observer.record(filename, rows_read=len(df_raw), rows_quarantined=len(df_raw))
                overall_status = "PARTIAL"
                continue

            # ── Step 4 & 5: Type casting + event_date ────────────────────
            df_typed = _cast_types(df_raw)
            df_dated = _derive_event_date(df_typed)

            # ── Backfill filter (Bonus B2) ─────────────────────────────────
            if backfill_dates:
                df_dated = df_dated[df_dated["event_date"].isin(backfill_dates)]
                if df_dated.empty:
                    logger.info("Backfill: no rows in date range for '%s' — skipping.", filename)
                    row_counts[filename] = {"read": 0, "written": 0, "skipped": 0}
                    continue

            # ── Step 6: Metadata enrichment ───────────────────────────────
            df_enriched = enrich(
                df_dated,
                source_file=filename,
                run_id=run_id,
                hash_exclude_columns=config.payments.hash_exclude_columns,
            )

            # ── Step 7: Dedup against existing Bronze ─────────────────────
            target_dates = df_enriched["event_date"].dropna().unique().tolist()
            existing_hashes = load_existing_hashes(bronze_dir, target_dates)
            df_new = filter_new_rows(df_enriched, existing_hashes)

            rows_read = len(df_raw)
            rows_written = 0
            rows_skipped = rows_read - len(df_new)

            # ── Step 8: Write by partition ────────────────────────────────
            for event_date, group in df_new.groupby("event_date"):
                written = _write_partition(group, bronze_dir, str(event_date))
                rows_written += written

            row_counts[filename] = {
                "read": rows_read,
                "written": rows_written,
                "skipped": rows_skipped,
            }
            observer.record(
                filename,
                rows_read=rows_read,
                rows_written=rows_written,
                rows_quarantined=0,
            )
            logger.info(
                "Bronze | '%s': read=%d written=%d skipped=%d",
                filename,
                rows_read,
                rows_written,
                rows_skipped,
            )

    # ── Step 9: Run manifest ──────────────────────────────────────────────
    write_manifest(
        run_id=run_id,
        files_processed=files_processed,
        row_counts=row_counts,
        status=overall_status,
        output_dir=manifests_dir,
    )

    return {
        "run_id": run_id,
        "files_processed": files_processed,
        "row_counts": row_counts,
        "status": overall_status,
    }
