"""
run_pipeline.py
----------------
CLI entry point for the Payment Hub Datalake pipeline.

Usage:
  # Run all stages
  python run_pipeline.py --config config.yaml --stage all

  # Run only Bronze
  python run_pipeline.py --config config.yaml --stage bronze

  # Run only Gold
  python run_pipeline.py --config config.yaml --stage gold

  # Backfill mode (Bonus B2): re-process specific date range
  python run_pipeline.py --config config.yaml --stage all --mode backfill --from_date 2024-01-15 --to_date 2024-01-16
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# Configure structured logging before importing pipeline modules
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("run_pipeline")


def _parse_date_range(from_date: str, to_date: str) -> list[str]:
    """Return all YYYY-MM-DD date strings in [from_date, to_date] inclusive."""
    start = date.fromisoformat(from_date)
    end = date.fromisoformat(to_date)
    if start > end:
        raise ValueError(f"--from_date ({from_date}) must be <= --to_date ({to_date})")
    dates = []
    current = start
    while current <= end:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Payment Hub Datalake Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml in current directory)",
    )
    parser.add_argument(
        "--stage",
        choices=["bronze", "bronze_merchants", "gold", "all"],
        default="all",
        help="Pipeline stage to run: bronze | bronze_merchants | gold | all (default: all)",
    )
    parser.add_argument(
        "--mode",
        choices=["normal", "backfill"],
        default="normal",
        help="Run mode: normal (all files) or backfill (date range only). Default: normal",
    )
    parser.add_argument(
        "--from_date",
        default=None,
        help="Backfill start date (YYYY-MM-DD). Required when --mode=backfill.",
    )
    parser.add_argument(
        "--to_date",
        default=None,
        help="Backfill end date inclusive (YYYY-MM-DD). Required when --mode=backfill.",
    )
    parser.add_argument(
        "--run_id",
        default=None,
        help="Optional run ID (UUID). Auto-generated if not supplied.",
    )

    args = parser.parse_args()

    # ── Load config ───────────────────────────────────────────────────────
    from src.common.config_loader import load_config

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = Path.cwd() / config_path

    try:
        config = load_config(config_path, project_root=config_path.parent)
    except FileNotFoundError as exc:
        logger.error("Config file not found: %s", exc)
        return 1

    # ── Backfill date range ───────────────────────────────────────────────
    backfill_dates: list[str] | None = None
    if args.mode == "backfill":
        if not args.from_date or not args.to_date:
            logger.error("--mode=backfill requires --from_date and --to_date.")
            return 1
        try:
            backfill_dates = _parse_date_range(args.from_date, args.to_date)
        except ValueError as exc:
            logger.error("Invalid date range: %s", exc)
            return 1
        logger.info("Backfill mode: processing dates %s → %s (%d days)",
                    args.from_date, args.to_date, len(backfill_dates))

    # ── Run Bronze ────────────────────────────────────────────────────────
    if args.stage in ("bronze", "all"):
        from src.bronze.ingestion import run_bronze_ingestion

        logger.info("Starting Bronze ingestion (run_id=%s)...", args.run_id or "auto")
        result = run_bronze_ingestion(
            config=config,
            run_id=args.run_id,
            backfill_dates=backfill_dates,
        )
        logger.info(
            "Bronze complete | status=%s | files=%d",
            result["status"],
            len(result["files_processed"]),
        )
        for fname, counts in result["row_counts"].items():
            logger.info(
                "  %-45s read=%-5d written=%-5d skipped=%d",
                fname,
                counts["read"],
                counts["written"],
                counts["skipped"],
            )

    # ── Run Merchant Bronze ───────────────────────────────────────────────
    if args.stage in ("bronze_merchants", "all"):
        from src.bronze.merchant_ingestion import run_merchant_bronze_ingestion

        logger.info("Starting Merchant Bronze ingestion (run_id=%s)...", args.run_id or "auto")
        merchant_result = run_merchant_bronze_ingestion(
            config=config,
            run_id=args.run_id,
        )
        logger.info(
            "Merchant Bronze complete | status=%s | read=%d written=%d skipped=%d",
            merchant_result["status"],
            merchant_result["rows_read"],
            merchant_result["rows_written"],
            merchant_result["rows_skipped"],
        )

    # ── Run Gold ──────────────────────────────────────────────────────────
    if args.stage in ("gold", "all"):
        from src.gold.pipeline import run_gold_pipeline

        logger.info("Starting Gold transformation...")
        gold_result = run_gold_pipeline(
            config=config,
            run_id=args.run_id,
            backfill_dates=backfill_dates,
        )
        logger.info("Gold complete | status=%s", gold_result["status"])
        for table, counts in gold_result.get("tables", {}).items():
            logger.info(
                "  %-40s rows_written=%d",
                table,
                counts.get("rows_written", 0),
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
