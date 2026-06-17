"""
test_bronze_idempotency.py
---------------------------
Verifies that running Bronze ingestion twice on the same file
does NOT produce duplicate rows (idempotency requirement).

What is tested:
  - Run ingestion once → N rows written
  - Run ingestion again on same file → 0 additional rows
  - Total Bronze row count remains N (not 2N)

This is the most critical correctness test for this assignment.
"""

from __future__ import annotations

import os
import pandas as pd
import pytest

from tests.conftest import make_payment_row, REQUIRED_COLUMNS
from src.common.metadata_enricher import enrich
from src.bronze.deduplicator import load_existing_hashes, filter_new_rows


# ---------------------------------------------------------------------------
# Unit-level idempotency test (no filesystem Bronze store needed)
# ---------------------------------------------------------------------------

class TestDeduplicatorIdempotency:
    """Tests the deduplication logic in isolation."""

    def test_same_rows_produce_zero_net_new(self):
        """
        Given a set of rows already in Bronze (hashes known),
        filtering them again should produce 0 net-new rows.
        """
        rows = [make_payment_row(f"TXN-{i:03d}") for i in range(5)]
        df = pd.DataFrame(rows)
        df_enriched = enrich(df, source_file="test.csv", run_id="run-1")

        existing_hashes = set(df_enriched["record_hash"].tolist())
        new_rows = filter_new_rows(df_enriched, existing_hashes)

        assert len(new_rows) == 0, (
            f"Expected 0 net-new rows on re-run, got {len(new_rows)}"
        )

    def test_new_rows_are_not_filtered(self):
        """
        New rows (hashes not in existing set) must all pass through.
        """
        rows = [make_payment_row(f"TXN-{i:03d}") for i in range(5)]
        df = pd.DataFrame(rows)
        df_enriched = enrich(df, source_file="test.csv", run_id="run-1")

        # Simulate empty Bronze (first run)
        new_rows = filter_new_rows(df_enriched, existing_hashes=set())

        assert len(new_rows) == 5, (
            f"All 5 rows should pass first-run filter, got {len(new_rows)}"
        )

    def test_partial_overlap_keeps_only_net_new(self):
        """
        Resubmit scenario: 5 existing rows + 2 genuinely new rows.
        filter_new_rows should return exactly 2.
        """
        existing_rows = [make_payment_row(f"TXN-{i:03d}") for i in range(5)]
        new_rows_data = [make_payment_row(f"TXN-{i:03d}") for i in range(5, 7)]  # 2 new

        df_existing = enrich(pd.DataFrame(existing_rows), source_file="day1.csv", run_id="run-1")
        df_resubmit = enrich(
            pd.DataFrame(existing_rows + new_rows_data),
            source_file="day1_resubmit.csv",
            run_id="run-2",
        )

        existing_hashes = set(df_existing["record_hash"].tolist())
        net_new = filter_new_rows(df_resubmit, existing_hashes)

        assert len(net_new) == 2, (
            f"Resubmit with 2 new rows should yield 2 net-new, got {len(net_new)}"
        )


# ---------------------------------------------------------------------------
# Integration-level idempotency test (writes to tmp_path Bronze store)
# ---------------------------------------------------------------------------

class TestBronzeIngestionIdempotency:
    """Tests idempotency through the full Bronze ingestion pipeline."""

    def _write_csv(self, tmp_path, rows, filename="payments.csv"):
        """Write rows to a CSV file in tmp_path/data/payments/."""
        payments_dir = tmp_path / "data" / "payments"
        payments_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(rows)
        csv_path = payments_dir / filename
        df.to_csv(csv_path, index=False)
        return csv_path

    def _count_bronze_rows(self, bronze_dir) -> int:
        """Count total rows across all Bronze Parquet files."""
        bronze_path = bronze_dir
        total = 0
        if not bronze_path.exists():
            return 0
        for pq_file in bronze_path.rglob("*.parquet"):
            df = pd.read_parquet(pq_file)
            total += len(df)
        return total

    def test_double_run_same_file_does_not_duplicate(self, tmp_path, fake_config):
        """
        Core idempotency test: run ingestion twice on the same file.
        Row count must be identical after both runs.
        """
        from src.bronze.ingestion import run_bronze_ingestion

        rows = [
            make_payment_row("TXN-001", transaction_ts="2024-01-15 10:00:00"),
            make_payment_row("TXN-002", transaction_ts="2024-01-15 11:00:00"),
            make_payment_row("TXN-003", transaction_ts="2024-01-15 12:00:00"),
        ]
        self._write_csv(tmp_path, rows)

        # Run 1
        result_1 = run_bronze_ingestion(fake_config, run_id="run-1")
        count_after_run1 = self._count_bronze_rows(
            fake_config.resolve(fake_config.paths.output_bronze_payments)
        )

        # Run 2 — same file
        result_2 = run_bronze_ingestion(fake_config, run_id="run-2")
        count_after_run2 = self._count_bronze_rows(
            fake_config.resolve(fake_config.paths.output_bronze_payments)
        )

        assert count_after_run1 == 3, f"Run 1 should write 3 rows, got {count_after_run1}"
        assert count_after_run2 == 3, (
            f"Run 2 on same file should keep row count at 3, got {count_after_run2}"
        )

    def test_resubmit_file_adds_only_new_rows(self, tmp_path, fake_config):
        """
        Resubmit scenario: process original file, then process resubmit
        (same rows + 2 new). Bronze should gain exactly 2 rows.
        """
        from src.bronze.ingestion import run_bronze_ingestion

        original_rows = [
            make_payment_row("TXN-001", transaction_ts="2024-01-15 10:00:00"),
            make_payment_row("TXN-002", transaction_ts="2024-01-15 11:00:00"),
        ]
        resubmit_rows = original_rows + [
            make_payment_row("TXN-003", transaction_ts="2024-01-15 12:00:00"),
            make_payment_row("TXN-004", transaction_ts="2024-01-15 13:00:00"),
        ]

        # Process original
        self._write_csv(tmp_path, original_rows, filename="payments_day1.csv")
        run_bronze_ingestion(fake_config, run_id="run-1")
        count_after_original = self._count_bronze_rows(
            fake_config.resolve(fake_config.paths.output_bronze_payments)
        )

        # Process resubmit (add the file alongside original)
        self._write_csv(tmp_path, resubmit_rows, filename="payments_day1_resubmit.csv")
        run_bronze_ingestion(fake_config, run_id="run-2")
        count_after_resubmit = self._count_bronze_rows(
            fake_config.resolve(fake_config.paths.output_bronze_payments)
        )

        assert count_after_original == 2
        # After resubmit: 2 original + 2 from resubmit (TXN-003, TXN-004) — TXN-001/002 deduped
        assert count_after_resubmit == 4, (
            f"Expected 4 rows after resubmit (2 original + 2 new), got {count_after_resubmit}"
        )
