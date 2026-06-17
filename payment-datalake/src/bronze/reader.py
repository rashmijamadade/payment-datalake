"""
reader.py  (Bronze layer)
--------------------------
Responsible ONLY for file discovery and CSV reading.
No business logic, no validation, no enrichment — pure I/O.

Separation rationale: keeping file I/O isolated means ingestion logic
can be tested against in-memory DataFrames without touching the filesystem.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def discover_csv_files(directory: str | Path, pattern: str = "*.csv") -> list[Path]:
    """
    Return a sorted list of CSV file paths found in *directory*.

    Parameters
    ----------
    directory:
        Directory to search (non-recursive).
    pattern:
        Glob pattern for matching files. Default: "*.csv".

    Returns
    -------
    list[Path]
        Sorted list of matched file paths. Empty list if directory does not exist.
    """
    directory = Path(directory)
    if not directory.exists():
        logger.warning("Input directory does not exist: %s", directory)
        return []

    files = sorted(directory.glob(pattern))
    logger.info("Discovered %d CSV file(s) in '%s'.", len(files), directory)
    return files


def read_csv(file_path: str | Path) -> Tuple[pd.DataFrame, str]:
    """
    Read a single CSV file into a DataFrame.

    Parameters
    ----------
    file_path:
        Path to the CSV file.

    Returns
    -------
    Tuple[pd.DataFrame, str]
        (DataFrame with all columns as strings initially, filename)

    Notes
    -----
    - All columns are read as strings at this stage. Type casting happens later.
    - The filename (stem + suffix) is returned for use as source_file metadata.
    - Raises on genuine I/O errors so the caller can handle/skip.
    """
    file_path = Path(file_path)
    logger.debug("Reading CSV: %s", file_path)

    df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
    filename = file_path.name

    logger.debug("Read %d rows from '%s'.", len(df), filename)
    return df, filename


def stream_csv_files(
    directory: str | Path, pattern: str = "*.csv"
) -> Iterator[Tuple[pd.DataFrame, str]]:
    """
    Yield (DataFrame, filename) for each CSV file discovered in *directory*.

    Skips files that raise an I/O error, logging a warning for each.
    """
    for file_path in discover_csv_files(directory, pattern):
        try:
            yield read_csv(file_path)
        except Exception as exc:
            logger.warning("Failed to read '%s': %s — skipping.", file_path.name, exc)
