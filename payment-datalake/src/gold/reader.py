"""
reader.py  (Gold layer)
------------------------
Reads from the Bronze Parquet store for Gold-layer transformations.

Kept separate from Gold aggregation logic so the reading step can be
tested or swapped independently.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def read_bronze(
    bronze_dir: str | Path,
    event_dates: Optional[List[str]] = None,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Read Bronze Parquet data, optionally filtered to specific event_dates.

    Parameters
    ----------
    bronze_dir:
        Root directory of the Bronze Parquet store.
    event_dates:
        If supplied, only partitions matching these dates are read.
        If None, all available partitions are read.
    columns:
        If supplied, only these columns are loaded (projection pushdown).

    Returns
    -------
    pd.DataFrame
        Combined DataFrame of all matching Bronze rows.
        Returns an empty DataFrame if no data is found.
    """
    bronze_dir = Path(bronze_dir)
    if not bronze_dir.exists():
        logger.warning("Bronze directory does not exist: %s", bronze_dir)
        return pd.DataFrame()

    # Collect target partition paths
    if event_dates:
        partition_dirs = [bronze_dir / f"event_date={d}" for d in event_dates]
        partition_dirs = [p for p in partition_dirs if p.exists()]
    else:
        partition_dirs = sorted(
            [p for p in bronze_dir.iterdir() if p.is_dir() and p.name.startswith("event_date=")]
        )

    if not partition_dirs:
        logger.warning("No Bronze partitions found in '%s'.", bronze_dir)
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    for part_dir in partition_dirs:
        parquet_files = list(part_dir.glob("*.parquet"))
        for pq_file in parquet_files:
            try:
                df = pd.read_parquet(pq_file, columns=columns)
                # Re-attach event_date from partition name if not present as column
                if "event_date" not in df.columns:
                    date_str = part_dir.name.split("=", 1)[1]
                    df["event_date"] = date_str
                frames.append(df)
            except Exception as exc:
                logger.warning("Failed to read Bronze file '%s': %s", pq_file, exc)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info("Read %d Bronze rows from %d partition(s).", len(combined), len(partition_dirs))
    return combined


def read_merchants(merchants_path: str | Path) -> pd.DataFrame:
    """
    Read the merchant lookup CSV.

    Returns
    -------
    pd.DataFrame
        Merchants with columns: merchant_id, merchant_name, merchant_category,
        country, onboarding_date, is_active, settlement_currency.
    """
    merchants_path = Path(merchants_path)
    if not merchants_path.exists():
        logger.error("Merchants file not found: %s", merchants_path)
        return pd.DataFrame()

    df = pd.read_csv(merchants_path)
    logger.debug("Read %d merchant records.", len(df))
    return df
