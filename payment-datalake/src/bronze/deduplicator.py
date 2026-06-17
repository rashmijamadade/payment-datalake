"""
deduplicator.py  (Bronze layer)
--------------------------------
Implements the hash-based idempotency mechanism for Bronze ingestion.

Idempotency strategy (specific mechanism):
  1. Read all existing record_hash values from the Bronze Parquet store
     for the target event_dates present in the incoming batch.
  2. Filter incoming rows: keep only rows whose record_hash is NOT already
     in the existing hash set.
  3. Return only the net-new rows for writing.

This ensures:
  - Re-running the same file twice → 0 additional rows written.
  - Running the resubmit file (15 original + 2 new rows) → 2 rows written.
  - Bronze row count never doubles on re-run.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Set

import pandas as pd

logger = logging.getLogger(__name__)


def load_existing_hashes(bronze_dir: str | Path, event_dates: list[str]) -> Set[str]:
    """
    Read all record_hash values already stored in Bronze for the given event_dates.

    Parameters
    ----------
    bronze_dir:
        Root directory of the Bronze Parquet store.
        Expected partition layout: <bronze_dir>/event_date=YYYY-MM-DD/
    event_dates:
        List of date strings (YYYY-MM-DD) to check.

    Returns
    -------
    Set[str]
        Set of SHA-256 hashes already present in Bronze for those partitions.
        Returns an empty set if no data exists yet (first run).
    """
    bronze_dir = Path(bronze_dir)
    existing_hashes: Set[str] = set()

    for date_str in event_dates:
        partition_path = bronze_dir / f"event_date={date_str}"
        if not partition_path.exists():
            logger.debug("No existing Bronze partition for event_date=%s.", date_str)
            continue

        parquet_files = list(partition_path.glob("*.parquet"))
        if not parquet_files:
            continue

        for pq_file in parquet_files:
            try:
                part_df = pd.read_parquet(pq_file, columns=["record_hash"])
                existing_hashes.update(part_df["record_hash"].tolist())
            except Exception as exc:
                logger.warning("Could not read hashes from '%s': %s", pq_file, exc)

    logger.debug(
        "Loaded %d existing record_hash(es) for dates: %s",
        len(existing_hashes),
        event_dates,
    )
    return existing_hashes


def filter_new_rows(df: pd.DataFrame, existing_hashes: Set[str]) -> pd.DataFrame:
    """
    Return only the rows in *df* whose record_hash is not in *existing_hashes*.

    Parameters
    ----------
    df:
        Enriched DataFrame (must contain a 'record_hash' column).
    existing_hashes:
        Set of hashes already persisted in Bronze.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame with only net-new rows.
    """
    if "record_hash" not in df.columns:
        raise ValueError("DataFrame must contain a 'record_hash' column before deduplication.")

    total = len(df)
    new_df = df[~df["record_hash"].isin(existing_hashes)].copy()
    skipped = total - len(new_df)

    if skipped:
        logger.info(
            "Deduplication: %d/%d rows already exist in Bronze → %d net-new rows.",
            skipped,
            total,
            len(new_df),
        )
    else:
        logger.debug("Deduplication: all %d rows are net-new.", total)

    return new_df
