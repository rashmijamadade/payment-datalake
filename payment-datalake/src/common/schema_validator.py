"""
schema_validator.py
--------------------
Reusable schema validation for any CSV/DataFrame dataset.

Design principle: validation is a pure function that never raises — it returns
a ValidationResult so the caller decides whether to skip or abort.
This makes it equally usable for payments, merchants, or any future dataset.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    is_valid: bool
    missing_columns: List[str] = field(default_factory=list)
    error_message: str = ""

    def __bool__(self) -> bool:
        return self.is_valid


def validate_schema(
    df: pd.DataFrame,
    required_columns: List[str],
    source_file: str = "<unknown>",
) -> ValidationResult:
    """
    Validate that *df* contains all *required_columns*.

    Parameters
    ----------
    df:
        The DataFrame to validate.
    required_columns:
        List of column names that must be present.
    source_file:
        Name/path of the originating file — used only for logging.

    Returns
    -------
    ValidationResult
        is_valid=True if all required columns are present.
        is_valid=False with missing_columns and error_message populated otherwise.

    Notes
    -----
    - Never raises an exception.
    - Logs a clear, structured error message when validation fails.
    - Column check is case-sensitive (payment schema uses lowercase snake_case).
    """
    actual_columns = set(df.columns.tolist())
    required_set = set(required_columns)
    missing = sorted(required_set - actual_columns)

    if not missing:
        logger.debug("Schema validation PASSED for '%s' (%d columns checked).", source_file, len(required_columns))
        return ValidationResult(is_valid=True)

    error_message = (
        f"Schema validation FAILED for '{source_file}': "
        f"missing required columns {missing}. "
        f"File will be skipped."
    )
    logger.error(error_message)
    return ValidationResult(
        is_valid=False,
        missing_columns=missing,
        error_message=error_message,
    )
