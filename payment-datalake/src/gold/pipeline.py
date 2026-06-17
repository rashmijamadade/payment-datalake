"""
pipeline.py  (Gold layer)
--------------------------
Orchestrates the full Gold pipeline:
  1. Read all Bronze data
  2. Read merchant lookup
  3. Build daily_payment_summary  → assert output schema → write (idempotent)
  4. Build merchant_performance_Nd → assert output schema → write (idempotent)
  5. Emit observability report

Idempotency strategy for Gold:
  Before writing, delete any existing Parquet files for the target partitions,
  then write fresh aggregates. Re-running produces identical output.

Output schema validation:
  After each table is built, validate_schema() asserts that every expected
  column is present in the result DataFrame.  This is an *output assertion*
  — not input validation.  If it fails, the table is NOT written and the
  pipeline status is set to PARTIAL so downstream consumers are not silently
  served broken data.
"""

from __future__ import annotations

import logging
import shutil
import uuid
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.common.config_loader import Config
from src.common.observability import PipelineObserver
from src.common.schema_validator import validate_schema
from src.gold.reader import read_bronze, read_merchants
from src.gold.aggregations import (
    build_daily_payment_summary,
    build_merchant_performance_rolling,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Parquet writer with partition-overwrite idempotency
# ---------------------------------------------------------------------------

def _write_gold_table(
    df: pd.DataFrame,
    output_dir: Path,
    partition_col: str,
    overwrite_partitions: bool = True,
) -> int:
    """
    Write a Gold DataFrame to Parquet, partitioned by *partition_col*.
    If *overwrite_partitions* is True, delete existing partition dirs before writing.

    Returns total rows written.
    """
    if df.empty:
        logger.warning("Empty DataFrame — nothing written to %s.", output_dir)
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    total_written = 0

    for partition_val, group in df.groupby(partition_col):
        partition_val_str = str(partition_val)
        partition_dir = output_dir / f"{partition_col}={partition_val_str}"

        if overwrite_partitions and partition_dir.exists():
            shutil.rmtree(partition_dir)
            logger.debug("Overwrote existing partition: %s", partition_dir)

        partition_dir.mkdir(parents=True, exist_ok=True)
        out_path = partition_dir / "part-0.parquet"

        table = pa.Table.from_pandas(group.drop(columns=[partition_col]), preserve_index=False)
        pq.write_table(table, out_path)
        total_written += len(group)
        logger.debug("Gold: wrote %d rows → %s", len(group), out_path)

    return total_written


# ---------------------------------------------------------------------------
# Main Gold pipeline entry point
# ---------------------------------------------------------------------------

def run_gold_pipeline(
    config: Config,
    run_id: Optional[str] = None,
    backfill_dates: Optional[List[str]] = None,
) -> Dict:
    """
    Run the full Gold transformation pipeline.

    Parameters
    ----------
    config:
        Loaded pipeline configuration.
    run_id:
        Unique run identifier. Auto-generated UUID if None.
    backfill_dates:
        If supplied (Bonus B2), only process these event_dates in Bronze.
        Gold partitions for these dates are overwritten.

    Returns
    -------
    dict
        Summary with run_id, table row counts, status.
    """
    run_id = run_id or str(uuid.uuid4())
    bronze_dir = config.resolve(config.paths.output_bronze_payments)
    merchants_path = config.resolve(config.paths.input_merchants)
    gold_daily_dir = config.resolve(config.paths.output_gold_daily)
    gold_7d_dir = config.resolve(config.paths.output_gold_merchant_7d)
    run_reports_dir = config.resolve(config.paths.run_reports)

    observer = PipelineObserver(run_id=run_id, stage="gold", output_dir=run_reports_dir)
    overall_status = "SUCCESS"
    result: Dict = {"run_id": run_id, "status": "SUCCESS", "tables": {}}

    with observer.track():
        # ── Read sources ──────────────────────────────────────────────────
        bronze_df = read_bronze(bronze_dir, event_dates=backfill_dates)
        merchants_df = read_merchants(merchants_path)

        if bronze_df.empty:
            logger.error("No Bronze data found — Gold pipeline cannot proceed.")
            observer.mark_failed()
            result["status"] = "FAILED"
            return result

        logger.info("Gold: loaded %d Bronze rows, %d merchant records.", len(bronze_df), len(merchants_df))

        # ── Table 1: daily_payment_summary ────────────────────────────────
        daily_df = build_daily_payment_summary(
            bronze_df=bronze_df,
            merchants_df=merchants_df,
            approved_status=config.gold.approved_status,
            approval_denominator_statuses=config.gold.approval_denominator_statuses,
            approval_rate_payment_method=config.gold.approval_rate_payment_method,
        )

        # ── Output schema assertion: daily_payment_summary ───────────────
        if config.gold.output_schema_daily_payment_summary:
            schema_check = validate_schema(
                daily_df,
                config.gold.output_schema_daily_payment_summary,
                source_file="daily_payment_summary (Gold output)",
                strict=True,
            )
            if not schema_check:
                logger.error(
                    "Gold output schema assertion FAILED for daily_payment_summary: %s",
                    schema_check.error_message,
                )
                overall_status = "PARTIAL"
                result["tables"]["daily_payment_summary"] = {
                    "rows_read": len(bronze_df), "rows_written": 0,
                    "schema_error": schema_check.error_message,
                }
                observer.record("daily_payment_summary", rows_read=len(bronze_df), rows_quarantined=len(daily_df))
            else:
                daily_written = _write_gold_table(
                    daily_df,
                    gold_daily_dir,
                    partition_col="event_date",
                    overwrite_partitions=config.gold.overwrite_partitions,
                )
                observer.record("daily_payment_summary", rows_read=len(bronze_df), rows_written=daily_written)
                result["tables"]["daily_payment_summary"] = {
                    "rows_read": len(bronze_df),
                    "rows_written": daily_written,
                }
                logger.info("Gold: daily_payment_summary → %d rows written.", daily_written)
        else:
            # No schema defined in config — write without assertion
            daily_written = _write_gold_table(
                daily_df,
                gold_daily_dir,
                partition_col="event_date",
                overwrite_partitions=config.gold.overwrite_partitions,
            )
            observer.record("daily_payment_summary", rows_read=len(bronze_df), rows_written=daily_written)
            result["tables"]["daily_payment_summary"] = {
                "rows_read": len(bronze_df),
                "rows_written": daily_written,
            }
            logger.info("Gold: daily_payment_summary → %d rows written.", daily_written)

        # ── Table 2: merchant_performance_Nd ─────────────────────────────
        rolling_df = build_merchant_performance_rolling(
            bronze_df=bronze_df,
            merchants_df=merchants_df,
            window_days=config.gold.window_days,
            approved_status=config.gold.approved_status,
            approval_denominator_statuses=config.gold.approval_denominator_statuses,
            approval_rate_payment_method=config.gold.approval_rate_payment_method,
            reversed_status=config.gold.reversed_status,
        )

        # ── Output schema assertion: merchant_performance ─────────────────
        if config.gold.output_schema_merchant_performance:
            expected_cols = list(config.gold.output_schema_merchant_performance) + [
                f"total_transactions_{config.gold.window_days}d",
                f"total_approved_amount_{config.gold.window_days}d",
                f"merchant_daily_approval_rate_{config.gold.window_days}d",
                f"reversal_rate_{config.gold.window_days}d",
                f"active_days_{config.gold.window_days}d",
            ]
            schema_check = validate_schema(
                rolling_df,
                expected_cols,
                source_file=f"merchant_performance_{config.gold.window_days}d (Gold output)",
                strict=True,
            )
            if not schema_check:
                logger.error(
                    "Gold output schema assertion FAILED for merchant_performance_%dd: %s",
                    config.gold.window_days,
                    schema_check.error_message,
                )
                overall_status = "PARTIAL"
                result["tables"][f"merchant_performance_{config.gold.window_days}d"] = {
                    "rows_read": len(bronze_df), "rows_written": 0,
                    "schema_error": schema_check.error_message,
                }
                observer.record(
                    f"merchant_performance_{config.gold.window_days}d",
                    rows_read=len(bronze_df), rows_quarantined=len(rolling_df),
                )
            else:
                rolling_written = _write_gold_table(
                    rolling_df,
                    gold_7d_dir,
                    partition_col="snapshot_date",
                    overwrite_partitions=config.gold.overwrite_partitions,
                )
                observer.record(
                    f"merchant_performance_{config.gold.window_days}d",
                    rows_read=len(bronze_df),
                    rows_written=rolling_written,
                )
                result["tables"][f"merchant_performance_{config.gold.window_days}d"] = {
                    "rows_read": len(bronze_df),
                    "rows_written": rolling_written,
                }
                logger.info(
                    "Gold: merchant_performance_%dd → %d rows written.",
                    config.gold.window_days,
                    rolling_written,
                )
        else:
            # No schema defined in config — write without assertion
            rolling_written = _write_gold_table(
                rolling_df,
                gold_7d_dir,
                partition_col="snapshot_date",
                overwrite_partitions=config.gold.overwrite_partitions,
            )
            observer.record(
                f"merchant_performance_{config.gold.window_days}d",
                rows_read=len(bronze_df),
                rows_written=rolling_written,
            )
            result["tables"][f"merchant_performance_{config.gold.window_days}d"] = {
                "rows_read": len(bronze_df),
                "rows_written": rolling_written,
            }
            logger.info(
                "Gold: merchant_performance_%dd → %d rows written.",
                config.gold.window_days,
                rolling_written,
            )

    result["status"] = overall_status
    return result
