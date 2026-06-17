"""
metadata_enricher.py
---------------------
Adds auditing metadata columns to a DataFrame before it is written to Bronze.

Kept as a composable, pure-function step — no I/O, no side effects.
Accepts any DataFrame and a run context; returns the enriched DataFrame.

Metadata columns added:
  - ingest_ts      : UTC timestamp when this enrichment ran
  - ingest_run_id  : Caller-supplied run UUID (or auto-generated)
  - source_file    : Filename of the originating CSV
  - record_hash    : SHA-256 of all payload columns (excludes metadata columns)
                     Deterministic: same input always produces same hash.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timezone
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def _compute_record_hash(row: pd.Series, payload_columns: List[str]) -> str:
    """
    Compute a deterministic SHA-256 hash over the payload columns of *row*.

    The hash is computed over a canonical string formed by joining:
      column_name=value pairs, sorted by column_name, separated by '|'.

    Sorting column names ensures hash stability regardless of DataFrame column order.
    """
    parts = [f"{col}={row[col]}" for col in sorted(payload_columns) if col in row.index]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def enrich(
    df: pd.DataFrame,
    source_file: str,
    run_id: Optional[str] = None,
    hash_exclude_columns: Optional[List[str]] = None,
    ingest_ts: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Add metadata columns to *df* and return the enriched copy.

    Parameters
    ----------
    df:
        Input DataFrame (payload columns only — metadata columns must NOT be present).
    source_file:
        Filename of the originating CSV file. Stored as-is.
    run_id:
        Unique identifier for this pipeline run. Auto-generated UUID if None.
    hash_exclude_columns:
        Column names to exclude from record_hash computation (i.e. the metadata
        columns themselves). Defaults to the standard metadata column names.
    ingest_ts:
        Override ingestion timestamp (UTC). Defaults to utcnow(). Useful in tests.

    Returns
    -------
    pd.DataFrame
        A new DataFrame with four additional columns appended.

    Notes
    -----
    - A copy is returned; the original DataFrame is never mutated.
    - record_hash is computed BEFORE any metadata columns are added, so the hash
      is purely over business payload columns.
    """
    if hash_exclude_columns is None:
        hash_exclude_columns = ["ingest_ts", "ingest_run_id", "source_file", "record_hash"]

    if run_id is None:
        run_id = str(uuid.uuid4())

    if ingest_ts is None:
        ingest_ts = datetime.now(timezone.utc)

    enriched = df.copy()

    # Determine payload columns (everything not in the exclude list)
    payload_columns = [c for c in enriched.columns if c not in hash_exclude_columns]

    logger.debug(
        "Enriching %d rows from '%s' | run_id=%s | payload_cols=%d",
        len(enriched),
        source_file,
        run_id,
        len(payload_columns),
    )

    # Compute record_hash row-by-row over payload columns only
    enriched["record_hash"] = enriched.apply(
        lambda row: _compute_record_hash(row, payload_columns), axis=1
    )

    # Add remaining metadata columns
    enriched["ingest_ts"] = ingest_ts
    enriched["ingest_run_id"] = run_id
    enriched["source_file"] = source_file

    return enriched
