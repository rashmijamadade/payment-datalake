"""
conftest.py
-----------
Shared pytest fixtures used across all test modules.

Design: all fixtures use tmp_path — no shared state between tests.
Tests work purely in-memory or in temporary directories — no real CSV files required.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

# Ensure src is importable when tests run from the project root
sys.path.insert(0, str(Path(__file__).parent.parent))


# ---------------------------------------------------------------------------
# Minimal valid payment DataFrame
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = [
    "transaction_id",
    "merchant_id",
    "customer_id",
    "customer_email",
    "customer_phone",
    "amount",
    "currency",
    "status",
    "transaction_ts",
    "payment_method",
    "card_last4",
    "gateway_ref",
]


def make_payment_row(
    transaction_id: str = "TXN-001",
    merchant_id: str = "M001",
    customer_id: str = "CUST-001",
    customer_email: str = "test@example.com",
    customer_phone: str = "+1234567890",
    amount: str = "100.00",
    currency: str = "USD",
    status: str = "APPROVED",
    transaction_ts: str = "2024-01-15 09:00:00",
    payment_method: str = "CARD",
    card_last4: str = "1234",
    gateway_ref: str = "GW-001",
    ingest_ts: str = "2024-01-15 10:00:00+00:00",
) -> dict:
    """Return a single valid payment row as a dict."""
    return {
        "transaction_id": transaction_id,
        "merchant_id": merchant_id,
        "customer_id": customer_id,
        "customer_email": customer_email,
        "customer_phone": customer_phone,
        "amount": amount,
        "currency": currency,
        "status": status,
        "transaction_ts": transaction_ts,
        "payment_method": payment_method,
        "card_last4": card_last4,
        "gateway_ref": gateway_ref,
        "ingest_ts": ingest_ts,
    }


@pytest.fixture
def minimal_payments_df() -> pd.DataFrame:
    """A minimal valid payments DataFrame with 3 rows."""
    rows = [
        make_payment_row("TXN-001", status="APPROVED", amount="100.00"),
        make_payment_row("TXN-002", status="DECLINED", amount="50.00"),
        make_payment_row("TXN-003", status="APPROVED", amount="200.00"),
    ]
    return pd.DataFrame(rows)


@pytest.fixture
def minimal_merchants_df() -> pd.DataFrame:
    """A minimal merchants DataFrame."""
    return pd.DataFrame(
        [
            {
                "merchant_id": "M001",
                "merchant_name": "Test Merchant",
                "merchant_category": "RETAIL",
                "country": "US",
                "onboarding_date": "2022-01-01",
                "is_active": True,
                "settlement_currency": "USD",
            }
        ]
    )


@pytest.fixture
def fake_config(tmp_path: Path):
    """
    Returns a Config-like object using tmp_path for all paths.
    Avoids loading config.yaml from disk in unit tests.
    """
    from src.common.config_loader import (
        Config,
        PathsConfig,
        PaymentsConfig,
        MerchantsConfig,
        GoldConfig,
    )

    paths = PathsConfig(
        input_payments=str(tmp_path / "data" / "payments"),
        input_merchants=str(tmp_path / "data" / "merchants" / "merchants.csv"),
        output_bronze_payments=str(tmp_path / "output" / "bronze_payments"),
        output_bronze_merchants=str(tmp_path / "output" / "bronze_merchants"),
        output_gold_daily=str(tmp_path / "output" / "gold" / "daily_payment_summary"),
        output_gold_merchant_7d=str(tmp_path / "output" / "gold" / "merchant_performance_7d"),
        bronze_payments_manifests=str(tmp_path / "output" / "bronze_payments" / "_manifests"),
        bronze_merchants_manifests=str(tmp_path / "output" / "bronze_merchants" / "_manifests"),
        run_reports=str(tmp_path / "output" / "run_reports"),
    )
    payments = PaymentsConfig(
        partition_key="event_date",
        file_pattern="*.csv",
        required_columns=REQUIRED_COLUMNS,
        metadata_columns=["ingest_ts", "ingest_run_id", "source_file", "record_hash"],
        hash_exclude_columns=["ingest_ts", "ingest_run_id", "source_file", "record_hash"],
    )
    merchants = MerchantsConfig(
        required_columns=[
            "merchant_id", "merchant_name", "merchant_category",
            "country", "onboarding_date", "is_active", "settlement_currency",
        ],
        hash_exclude_columns=["ingest_ts", "ingest_run_id", "source_file", "record_hash"],
    )
    gold = GoldConfig(
        window_days=7,
        overwrite_partitions=True,
        approved_status="APPROVED",
        approval_denominator_statuses=["APPROVED", "DECLINED"],
        approval_rate_payment_method="CARD",
        reversed_status="REVERSED",
    )
    return Config(paths=paths, payments=payments, merchants=merchants, gold=gold, project_root=tmp_path)
