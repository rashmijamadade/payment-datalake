"""
test_gold_grain.py
-------------------
Verifies that the Gold tables produce the correct number of rows
for a known input — i.e., the grain is correctly implemented.

Grain definitions:
  daily_payment_summary     : one row per (event_date, merchant_id, currency, status)
  merchant_performance_7d   : one row per (snapshot_date, merchant_id)

What is tested:
  1. daily_payment_summary row count for known input
  2. merchant_performance_7d row count for known input
  3. No duplicate grain combinations in daily_payment_summary
  4. No duplicate grain combinations in merchant_performance_7d
  5. All expected grain values are present in output
"""

from __future__ import annotations

import pandas as pd
import pytest

from tests.conftest import make_payment_row
from src.gold.aggregations import (
    build_daily_payment_summary,
    build_merchant_performance_rolling,
)


def _make_bronze_df(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["transaction_ts"] = pd.to_datetime(df["transaction_ts"], utc=True, errors="coerce")
    df["event_date"] = df["transaction_ts"].dt.strftime("%Y-%m-%d")
    return df


MERCHANTS_DF = pd.DataFrame([
    {"merchant_id": "M001", "merchant_name": "Merchant One", "merchant_category": "RETAIL"},
    {"merchant_id": "M002", "merchant_name": "Merchant Two", "merchant_category": "FOOD"},
])


class TestDailyPaymentSummaryGrain:
    """Tests grain correctness for daily_payment_summary."""

    def test_single_date_single_merchant_single_status(self):
        """
        Simplest case: 3 APPROVED rows for M001 on one date.
        Expected: 1 row (one unique grain combination).
        """
        rows = [
            make_payment_row("TXN-001", merchant_id="M001", currency="USD",
                             status="APPROVED", transaction_ts="2024-01-15 10:00:00"),
            make_payment_row("TXN-002", merchant_id="M001", currency="USD",
                             status="APPROVED", transaction_ts="2024-01-15 11:00:00"),
            make_payment_row("TXN-003", merchant_id="M001", currency="USD",
                             status="APPROVED", transaction_ts="2024-01-15 12:00:00"),
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_daily_payment_summary(bronze_df, MERCHANTS_DF)

        assert len(result) == 1, f"Expected 1 grain row, got {len(result)}"
        assert result.iloc[0]["transaction_count"] == 3

    def test_two_statuses_produce_two_rows(self):
        """
        APPROVED + DECLINED for same merchant/date/currency → 2 grain rows.
        """
        rows = [
            make_payment_row("TXN-001", merchant_id="M001", currency="USD",
                             status="APPROVED", transaction_ts="2024-01-15 10:00:00"),
            make_payment_row("TXN-002", merchant_id="M001", currency="USD",
                             status="DECLINED", transaction_ts="2024-01-15 11:00:00"),
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_daily_payment_summary(bronze_df, MERCHANTS_DF)

        assert len(result) == 2, f"Expected 2 rows (2 statuses), got {len(result)}"

    def test_two_merchants_produce_separate_rows(self):
        """Two merchants on same date/currency/status → 2 grain rows."""
        rows = [
            make_payment_row("TXN-001", merchant_id="M001", currency="USD",
                             status="APPROVED", transaction_ts="2024-01-15 10:00:00"),
            make_payment_row("TXN-002", merchant_id="M002", currency="USD",
                             status="APPROVED", transaction_ts="2024-01-15 11:00:00"),
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_daily_payment_summary(bronze_df, MERCHANTS_DF)

        assert len(result) == 2, f"Expected 2 rows (2 merchants), got {len(result)}"
        assert set(result["merchant_id"].tolist()) == {"M001", "M002"}

    def test_two_dates_produce_separate_rows(self):
        """Same merchant/currency/status on two dates → 2 grain rows."""
        rows = [
            make_payment_row("TXN-001", merchant_id="M001", currency="USD",
                             status="APPROVED", transaction_ts="2024-01-15 10:00:00"),
            make_payment_row("TXN-002", merchant_id="M001", currency="USD",
                             status="APPROVED", transaction_ts="2024-01-16 10:00:00"),
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_daily_payment_summary(bronze_df, MERCHANTS_DF)

        assert len(result) == 2, f"Expected 2 rows (2 dates), got {len(result)}"

    def test_no_duplicate_grain_combinations(self):
        """
        The grain (event_date, merchant_id, currency, status) must be unique.
        No duplicate rows allowed.
        """
        rows = [
            make_payment_row(f"TXN-{i:03d}", merchant_id="M001", currency="USD",
                             status="APPROVED", transaction_ts="2024-01-15 10:00:00")
            for i in range(10)
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_daily_payment_summary(bronze_df, MERCHANTS_DF)

        grain_cols = ["event_date", "merchant_id", "currency", "status"]
        duplicates = result.duplicated(subset=grain_cols, keep=False)
        assert not duplicates.any(), "Grain combination must be unique — no duplicates allowed"

    def test_aggregations_are_correct(self):
        """Verify sum, avg, max, count are computed correctly for known input."""
        rows = [
            make_payment_row("TXN-001", amount="100.00", status="APPROVED",
                             transaction_ts="2024-01-15 10:00:00"),
            make_payment_row("TXN-002", amount="200.00", status="APPROVED",
                             transaction_ts="2024-01-15 11:00:00"),
            make_payment_row("TXN-003", amount="300.00", status="APPROVED",
                             transaction_ts="2024-01-15 12:00:00"),
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_daily_payment_summary(bronze_df, MERCHANTS_DF)

        row = result.iloc[0]
        assert row["transaction_count"] == 3
        assert row["total_amount"] == pytest.approx(600.0)
        assert row["avg_amount"] == pytest.approx(200.0)
        assert row["max_amount"] == pytest.approx(300.0)


class TestMerchantPerformanceGrain:
    """Tests grain correctness for merchant_performance_7d (rolling window)."""

    def test_one_date_one_merchant_produces_one_row(self):
        """Single date, single merchant → exactly 1 row."""
        rows = [
            make_payment_row("TXN-001", merchant_id="M001",
                             transaction_ts="2024-01-15 10:00:00"),
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_merchant_performance_rolling(
            bronze_df, MERCHANTS_DF, window_days=7
        )

        assert len(result) == 1, f"Expected 1 row, got {len(result)}"

    def test_two_dates_two_merchants_correct_row_count(self):
        """
        2 dates × 2 merchants = 4 expected rows
        (each merchant appears once per snapshot_date).
        """
        rows = [
            make_payment_row("TXN-001", merchant_id="M001",
                             transaction_ts="2024-01-15 10:00:00"),
            make_payment_row("TXN-002", merchant_id="M002",
                             transaction_ts="2024-01-15 11:00:00"),
            make_payment_row("TXN-003", merchant_id="M001",
                             transaction_ts="2024-01-16 10:00:00"),
            make_payment_row("TXN-004", merchant_id="M002",
                             transaction_ts="2024-01-16 11:00:00"),
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_merchant_performance_rolling(
            bronze_df, MERCHANTS_DF, window_days=7
        )

        # 2 snapshot_dates × 2 merchants = 4 rows
        assert len(result) == 4, f"Expected 4 rows, got {len(result)}"

    def test_no_duplicate_grain_combinations(self):
        """(snapshot_date, merchant_id) must be unique."""
        rows = [
            make_payment_row(f"TXN-{i:03d}", merchant_id="M001",
                             transaction_ts="2024-01-15 10:00:00")
            for i in range(5)
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_merchant_performance_rolling(
            bronze_df, MERCHANTS_DF, window_days=7
        )

        duplicates = result.duplicated(subset=["snapshot_date", "merchant_id"], keep=False)
        assert not duplicates.any(), "Grain (snapshot_date, merchant_id) must be unique"

    def test_window_days_parameter_affects_rolling_count(self):
        """
        Parameterisation test (Bonus B4): window_days=1 vs window_days=3.
        With 3 days of data, window_days=1 and window_days=3 should produce
        different total_transactions counts for days beyond day 1.
        """
        rows = [
            make_payment_row("TXN-001", merchant_id="M001",
                             transaction_ts="2024-01-13 10:00:00"),
            make_payment_row("TXN-002", merchant_id="M001",
                             transaction_ts="2024-01-14 10:00:00"),
            make_payment_row("TXN-003", merchant_id="M001",
                             transaction_ts="2024-01-15 10:00:00"),
        ]
        bronze_df = _make_bronze_df(rows)

        result_1d = build_merchant_performance_rolling(
            bronze_df, MERCHANTS_DF, window_days=1
        )
        result_3d = build_merchant_performance_rolling(
            bronze_df, MERCHANTS_DF, window_days=3
        )

        # On Jan 15 with window=1: only 1 transaction
        jan15_1d = result_1d[result_1d["snapshot_date"].astype(str) == "2024-01-15"]
        # On Jan 15 with window=3: all 3 transactions
        jan15_3d = result_3d[result_3d["snapshot_date"].astype(str) == "2024-01-15"]

        count_1d = jan15_1d["total_transactions_1d"].iloc[0] if not jan15_1d.empty else 0
        count_3d = jan15_3d["total_transactions_3d"].iloc[0] if not jan15_3d.empty else 0

        assert count_1d == 1, f"window_days=1 on Jan 15 → expected 1, got {count_1d}"
        assert count_3d == 3, f"window_days=3 on Jan 15 → expected 3, got {count_3d}"
