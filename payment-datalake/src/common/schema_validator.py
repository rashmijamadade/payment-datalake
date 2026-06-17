"""
schema_validator.py
--------------------
Reusable schema validation for any CSV/DataFrame dataset.

Design principle: validation is a pure function that never raises — it returns
a ValidationResult so the caller decides whether to skip or abort.
This makes it equally usable for payments, merchants, or any future dataset.

Strict mode (strict=True):
  In addition to checking for missing required columns, also flags columns that
  are present in the DataFrame but NOT in the required list. Use this for Gold
  output assertions to catch accidental column additions from code refactors.
  Bronze input validation uses strict=False (schema evolution allows extra cols).
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
    extra_columns: List[str] = field(default_factory=list)
    error_message: str = ""

    def __bool__(self) -> bool:
        return self.is_valid


def validate_schema(
    df: pd.DataFrame,
    required_columns: List[str],
    source_file: str = "<unknown>",
    strict: bool = False,
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
    strict:
        If True, also fails when the DataFrame contains columns that are NOT
        in *required_columns*. Use this for Gold output assertions to make the
        schema contract bidirectional (catches accidental column additions as
        well as removals). Defaults to False for backward compatibility.

    Returns
    -------
    ValidationResult
        is_valid=True if all required columns are present (and, when strict=True,
        no unexpected extra columns exist).
        is_valid=False with missing_columns / extra_columns and error_message
        populated otherwise.

    Notes
    -----
    - Never raises an exception.
    - Logs a clear, structured error message when validation fails.
    - Column check is case-sensitive (payment schema uses lowercase snake_case).
    """
    actual_columns = set(df.columns.tolist())
    required_set = set(required_columns)
    missing = sorted(required_set - actual_columns)
    extra = sorted(actual_columns - required_set) if strict else []

    if not missing and not extra:
        logger.debug(
            "Schema validation PASSED for '%s' (%d columns checked%s).",
            source_file,
            len(required_columns),
            ", strict" if strict else "",
        )
        return ValidationResult(is_valid=True)

    errors: List[str] = []
    if missing:
        errors.append(f"missing required columns {missing}")
    if extra:
        errors.append(f"unexpected extra columns {extra}")

    error_message = (
        f"Schema validation FAILED for '{source_file}': "
        + "; ".join(errors)
        + ". File will be skipped."
    )
    logger.error(error_message)
    return ValidationResult(
        is_valid=False,
        missing_columns=missing,
        extra_columns=extra,
        error_message=error_message,
    )

