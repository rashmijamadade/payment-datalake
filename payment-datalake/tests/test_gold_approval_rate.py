"""
test_gold_approval_rate.py
---------------------------
Verifies correctness of the merchant_daily_approval_rate calculation in the Gold layer,
with specific focus on edge cases:

  1. All DECLINED — merchant_daily_approval_rate must be 0.0 (not ZeroDivisionError or NaN)
  2. All APPROVED — merchant_daily_approval_rate must be 1.0
  3. Mixed — correct fraction
  4. No CARD transactions — merchant_daily_approval_rate must be NaN/None (null-safe)
  5. All REVERSED (no APPROVED or DECLINED) — merchant_daily_approval_rate must be NaN/None
  6. Empty DataFrame — no exception raised, empty result returned
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from tests.conftest import make_payment_row
from src.gold.aggregations import build_daily_payment_summary


def _make_bronze_df(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal Bronze-like DataFrame from payment rows."""
    df = pd.DataFrame(rows)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["transaction_ts"] = pd.to_datetime(df["transaction_ts"], utc=True, errors="coerce")
    df["event_date"] = df["transaction_ts"].dt.strftime("%Y-%m-%d")
    df["ingest_ts"] = pd.to_datetime(df["ingest_ts"], utc=True, errors="coerce")
    return df


def _get_approval_rate(result_df: pd.DataFrame, merchant_id: str = "M001") -> float | None:
    """Extract merchant_daily_approval_rate for a merchant from the daily summary."""
    rows = result_df[result_df["merchant_id"] == merchant_id]["merchant_daily_approval_rate"].tolist()
    if not rows:
        return None
    # Return first non-null value, or None if all null
    for v in rows:
        if v is not None and not (isinstance(v, float) and math.isnan(v)):
            return v
    return None


MERCHANTS_DF = pd.DataFrame([{
    "merchant_id": "M001",
    "merchant_name": "Test Merchant",
    "merchant_category": "RETAIL",
}])


class TestApprovalRateEdgeCases:

    def test_all_declined_approval_rate_is_zero(self):
        """
        Edge case: all CARD transactions are DECLINED.
        approval_rate = 0 / 2 = 0.0 — not ZeroDivisionError, not NaN.
        """
        rows = [
            make_payment_row("TXN-001", status="DECLINED", payment_method="CARD",
                             transaction_ts="2024-01-15 10:00:00"),
            make_payment_row("TXN-002", status="DECLINED", payment_method="CARD",
                             transaction_ts="2024-01-15 11:00:00"),
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_daily_payment_summary(bronze_df, MERCHANTS_DF)

        rate = _get_approval_rate(result)
        assert rate == pytest.approx(0.0), f"All DECLINED should give merchant_daily_approval_rate=0.0, got {rate}"

    def test_all_approved_approval_rate_is_one(self):
        """All APPROVED CARD transactions → merchant_daily_approval_rate = 1.0."""
        rows = [
            make_payment_row("TXN-001", status="APPROVED", payment_method="CARD",
                             transaction_ts="2024-01-15 10:00:00"),
            make_payment_row("TXN-002", status="APPROVED", payment_method="CARD",
                             transaction_ts="2024-01-15 11:00:00"),
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_daily_payment_summary(bronze_df, MERCHANTS_DF)

        rate = _get_approval_rate(result)
        assert rate == pytest.approx(1.0), f"All APPROVED should give merchant_daily_approval_rate=1.0, got {rate}"

    def test_mixed_approval_rate_correct_fraction(self):
        """2 APPROVED + 1 DECLINED → merchant_daily_approval_rate = 2/3 ≈ 0.6667."""
        rows = [
            make_payment_row("TXN-001", status="APPROVED", payment_method="CARD",
                             transaction_ts="2024-01-15 10:00:00"),
            make_payment_row("TXN-002", status="APPROVED", payment_method="CARD",
                             transaction_ts="2024-01-15 11:00:00"),
            make_payment_row("TXN-003", status="DECLINED", payment_method="CARD",
                             transaction_ts="2024-01-15 12:00:00"),
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_daily_payment_summary(bronze_df, MERCHANTS_DF)

        rate = _get_approval_rate(result)
        assert rate == pytest.approx(2 / 3, rel=1e-3), (
            f"2 APPROVED + 1 DECLINED → expected ~0.6667, got {rate}"
        )

    def test_no_card_transactions_approval_rate_is_null(self):
        """
        Null-safety: approval_rate is only meaningful for CARD transactions.
        If all payments are WALLET/BANK_TRANSFER, approval_rate must be NULL.
        """
        rows = [
            make_payment_row("TXN-001", status="APPROVED", payment_method="WALLET",
                             transaction_ts="2024-01-15 10:00:00"),
            make_payment_row("TXN-002", status="APPROVED", payment_method="BANK_TRANSFER",
                             transaction_ts="2024-01-15 11:00:00"),
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_daily_payment_summary(bronze_df, MERCHANTS_DF)

        # merchant_daily_approval_rate should be NaN/None (not 0, not 1)
        approval_rates = result["merchant_daily_approval_rate"].tolist()
        for rate in approval_rates:
            is_null = rate is None or (isinstance(rate, float) and math.isnan(rate))
            assert is_null, f"No CARD transactions → merchant_daily_approval_rate must be null, got {rate}"

    def test_all_reversed_approval_rate_is_null(self):
        """
        REVERSED transactions are not in the denominator (APPROVED+DECLINED only).
        All-REVERSED should result in null approval_rate.
        """
        rows = [
            make_payment_row("TXN-001", status="REVERSED", payment_method="CARD",
                             transaction_ts="2024-01-15 10:00:00"),
        ]
        bronze_df = _make_bronze_df(rows)
        result = build_daily_payment_summary(bronze_df, MERCHANTS_DF)

        approval_rates = result["merchant_daily_approval_rate"].tolist()
        for rate in approval_rates:
            is_null = rate is None or (isinstance(rate, float) and math.isnan(rate))
            assert is_null, f"All REVERSED → merchant_daily_approval_rate must be null, got {rate}"

    def test_empty_bronze_returns_empty_dataframe(self):
        """Empty Bronze input must return empty DataFrame — no exception."""
        result = build_daily_payment_summary(pd.DataFrame(), MERCHANTS_DF)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
