"""
test_bronze_metadata.py
------------------------
Verifies that Bronze output contains all required metadata columns
and that the metadata values are correctly computed.

What is tested:
  1. Output DataFrame contains: ingest_ts, ingest_run_id, source_file, record_hash
  2. record_hash is deterministic (same input → same hash, always)
  3. record_hash differs between rows with different payload data
  4. ingest_run_id matches the supplied run_id
  5. source_file matches the supplied filename
"""

from __future__ import annotations

import pandas as pd
import pytest

from tests.conftest import make_payment_row
from src.common.metadata_enricher import enrich


class TestMetadataEnricher:
    """Unit tests for the metadata_enricher module."""

    def test_all_metadata_columns_present(self, minimal_payments_df):
        enriched = enrich(minimal_payments_df, source_file="test.csv", run_id="run-001")

        for col in ["ingest_ts", "ingest_run_id", "source_file", "record_hash"]:
            assert col in enriched.columns, f"Expected column '{col}' in enriched output"

    def test_ingest_run_id_matches_supplied_value(self, minimal_payments_df):
        run_id = "my-unique-run-id"
        enriched = enrich(minimal_payments_df, source_file="test.csv", run_id=run_id)

        assert (enriched["ingest_run_id"] == run_id).all(), (
            "All rows should have the supplied run_id"
        )

    def test_source_file_matches_supplied_filename(self, minimal_payments_df):
        filename = "payments_2024_01_15.csv"
        enriched = enrich(minimal_payments_df, source_file=filename, run_id="r1")

        assert (enriched["source_file"] == filename).all()

    def test_ingest_ts_is_populated(self, minimal_payments_df):
        enriched = enrich(minimal_payments_df, source_file="test.csv", run_id="r1")

        assert enriched["ingest_ts"].notna().all(), "ingest_ts must be populated for all rows"

    def test_record_hash_is_deterministic(self):
        """
        Determinism test: same payload + same columns → same hash, every time.
        This is critical for idempotency — the hash must be reproducible.
        """
        row = make_payment_row("TXN-STABLE")
        df = pd.DataFrame([row])

        hash_1 = enrich(df, source_file="f.csv", run_id="run-A")["record_hash"].iloc[0]
        hash_2 = enrich(df, source_file="f.csv", run_id="run-B")["record_hash"].iloc[0]

        assert hash_1 == hash_2, (
            "record_hash must be identical for the same payload regardless of run_id"
        )

    def test_record_hash_differs_for_different_payloads(self):
        """Different transaction data must produce different hashes."""
        df1 = pd.DataFrame([make_payment_row("TXN-AAA", amount="100.00")])
        df2 = pd.DataFrame([make_payment_row("TXN-BBB", amount="200.00")])

        hash_1 = enrich(df1, source_file="f.csv", run_id="r")["record_hash"].iloc[0]
        hash_2 = enrich(df2, source_file="f.csv", run_id="r")["record_hash"].iloc[0]

        assert hash_1 != hash_2, "Different payloads must produce different record_hashes"

    def test_metadata_columns_not_included_in_hash_computation(self):
        """
        Metadata columns (ingest_ts, ingest_run_id, source_file) must NOT affect
        the record_hash — only payload columns should be hashed.
        """
        df = pd.DataFrame([make_payment_row("TXN-HASH-TEST")])

        # Same payload, different run_ids and source files
        hash_run_a = enrich(df, source_file="file_a.csv", run_id="run-A")["record_hash"].iloc[0]
        hash_run_b = enrich(df, source_file="file_b.csv", run_id="run-B")["record_hash"].iloc[0]

        assert hash_run_a == hash_run_b, (
            "record_hash must be identical for the same payload "
            "even when source_file or run_id differ"
        )

    def test_auto_run_id_generated_when_not_supplied(self, minimal_payments_df):
        """When run_id is omitted, a UUID should be auto-generated."""
        enriched = enrich(minimal_payments_df, source_file="test.csv")

        assert enriched["ingest_run_id"].notna().all()
        assert enriched["ingest_run_id"].iloc[0] != "", "Auto-generated run_id must not be empty"

    def test_original_dataframe_not_mutated(self, minimal_payments_df):
        """enrich() must return a copy — never mutate the input DataFrame."""
        original_cols = set(minimal_payments_df.columns.tolist())
        _ = enrich(minimal_payments_df, source_file="test.csv", run_id="r1")

        assert set(minimal_payments_df.columns.tolist()) == original_cols, (
            "enrich() must not add columns to the original DataFrame"
        )
