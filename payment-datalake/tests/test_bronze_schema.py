"""
test_bronze_schema.py
----------------------
Verifies Bronze schema validation behaviour:
  - Files with missing required columns are SKIPPED (not crashed)
  - The pipeline continues processing remaining valid files
  - A file missing one column does not halt the entire run

What is tested:
  1. ValidationResult is False for a file missing required columns
  2. ValidationResult identifies the specific missing columns
  3. Full ingestion run with a bad file → only good files are processed, no exception raised
"""

from __future__ import annotations

import pandas as pd
import pytest

from tests.conftest import make_payment_row, REQUIRED_COLUMNS
from src.common.schema_validator import validate_schema, ValidationResult


class TestSchemaValidator:
    """Unit tests for the reusable schema_validator module."""

    def test_valid_schema_returns_true(self, minimal_payments_df):
        result = validate_schema(minimal_payments_df, REQUIRED_COLUMNS, "test.csv")
        assert result.is_valid is True
        assert result.missing_columns == []
        assert bool(result) is True

    def test_missing_column_returns_false(self):
        df = pd.DataFrame([make_payment_row()])
        df = df.drop(columns=["amount"])  # Remove a required column

        result = validate_schema(df, REQUIRED_COLUMNS, "bad_file.csv")

        assert result.is_valid is False
        assert "amount" in result.missing_columns
        assert bool(result) is False

    def test_error_message_names_missing_columns(self):
        df = pd.DataFrame([make_payment_row()])
        df = df.drop(columns=["amount", "currency"])

        result = validate_schema(df, REQUIRED_COLUMNS, "bad_file.csv")

        assert "amount" in result.error_message or "currency" in result.error_message

    def test_multiple_missing_columns_all_reported(self):
        """All missing columns must be in the result — not just the first one."""
        df = pd.DataFrame([make_payment_row()])
        df = df.drop(columns=["amount", "currency", "status"])

        result = validate_schema(df, REQUIRED_COLUMNS, "bad_file.csv")

        assert set(result.missing_columns) == {"amount", "currency", "status"}

    def test_extra_columns_do_not_fail_validation(self):
        """Schema evolution: extra columns in CSV should not fail validation (Bonus B3)."""
        df = pd.DataFrame([make_payment_row()])
        df["payment_network"] = "VISA"  # New column not in required list

        result = validate_schema(df, REQUIRED_COLUMNS, "evolved.csv")

        assert result.is_valid is True

    def test_empty_dataframe_with_correct_columns_passes(self):
        """Empty file with correct headers should pass validation."""
        df = pd.DataFrame(columns=REQUIRED_COLUMNS)
        result = validate_schema(df, REQUIRED_COLUMNS, "empty.csv")
        assert result.is_valid is True


class TestBronzeIngestionSkipsBadFile:
    """Integration test: pipeline must not crash when a bad file is encountered."""

    def test_bad_file_is_skipped_pipeline_does_not_raise(self, tmp_path, fake_config):
        """
        One bad file (missing 'amount') + one good file.
        Pipeline should process the good file without raising.
        """
        from src.bronze.ingestion import run_bronze_ingestion

        payments_dir = tmp_path / "data" / "payments"
        payments_dir.mkdir(parents=True, exist_ok=True)

        # Good file
        good_df = pd.DataFrame([
            make_payment_row("TXN-001", transaction_ts="2024-01-15 10:00:00"),
            make_payment_row("TXN-002", transaction_ts="2024-01-15 11:00:00"),
        ])
        good_df.to_csv(payments_dir / "good_file.csv", index=False)

        # Bad file — missing 'amount' column
        bad_df = pd.DataFrame([make_payment_row("TXN-BAD", transaction_ts="2024-01-15 09:00:00")])
        bad_df = bad_df.drop(columns=["amount"])
        bad_df.to_csv(payments_dir / "bad_file.csv", index=False)

        # Must not raise
        result = run_bronze_ingestion(fake_config, run_id="test-skip-run")

        assert result["status"] in ("SUCCESS", "PARTIAL"), (
            "Pipeline should not FAIL — bad file should be skipped with PARTIAL status"
        )
        # Good file rows should be written
        bronze_dir = fake_config.resolve(fake_config.paths.output_bronze_payments)
        total_rows = sum(
            len(pd.read_parquet(f))
            for f in bronze_dir.rglob("*.parquet")
        )
        assert total_rows == 2, f"Expected 2 rows from good file, got {total_rows}"
