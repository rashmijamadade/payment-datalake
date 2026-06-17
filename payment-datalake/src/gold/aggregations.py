"""
aggregations.py  (Gold layer)
------------------------------
Contains all Gold-layer aggregation logic, implemented using DuckDB SQL
queries executed on pandas DataFrames.

Why DuckDB + SQL?
-----------------
  - All business transformations are expressed as plain SQL — easy to
    review, explain to the team, and copy-paste into any SQL tool.
  - DuckDB can query pandas DataFrames directly (no data-copy to a DB file).
  - Results are returned as pandas DataFrames, so the rest of the pipeline
    (Parquet writer, tests) is unchanged.
  - Performance: DuckDB is vectorised and handles larger-than-memory data;
    native pandas loops (e.g. the rolling window) are replaced with a single
    SQL window query.

Design principles (unchanged from original):
  - Every public function is a pure transformation (DataFrame in → DataFrame out).
  - No file I/O in this module — the pipeline.py orchestrator handles that.
  - The rolling window is parameterised (window_days: int) so changing from 7d to
    30d requires only a config.yaml change — no code edits needed (Bonus B4).
  - approval_rate is null-safe: returns NULL/None if no card-initiated transactions
    exist in the relevant status group (guards against division by zero).

Grain documentation:
  daily_payment_summary      : (event_date, merchant_id, currency, status)
  merchant_performance_Nd    : (snapshot_date, merchant_id)
"""

from __future__ import annotations

import logging
from typing import List

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Table 1: daily_payment_summary
# ---------------------------------------------------------------------------

def build_daily_payment_summary(
    bronze_df: pd.DataFrame,
    merchants_df: pd.DataFrame,
    approved_status: str = "APPROVED",
    approval_denominator_statuses: List[str] = None,
    approval_rate_payment_method: str = "CARD",
) -> pd.DataFrame:
    """
    Produce the daily_payment_summary Gold table using DuckDB SQL.

    Grain: one row per (event_date, merchant_id, currency, status).

    approval_rate is computed at the (event_date, merchant_id, currency) grain
    — not per status — because it is a property of the merchant+date+currency
    combo.  It measures what fraction of card-initiated attempts were approved.

    SQL approach
    ------------
    Two CTEs are used:
      1. ``joined``      — payments LEFT JOINed with merchants for name/category.
      2. ``approval``    — a sub-aggregation at the coarser grain that computes
                           the approval rate with a safe NULL (not 0) when the
                           denominator is zero.
    Final SELECT groups at the full (event_date, merchant_id, currency, status)
    grain, then re-joins the approval rate from the coarser CTE.
    """
    if approval_denominator_statuses is None:
        approval_denominator_statuses = ["APPROVED", "DECLINED"]

    if bronze_df.empty:
        logger.warning("Bronze DataFrame is empty — daily_payment_summary will be empty.")
        return pd.DataFrame()

    # DuckDB SQL uses Python variables via f-strings for literals that cannot
    # be parameterised (IN-lists, string literals in CASE).
    denom_statuses_sql = ", ".join(f"'{s}'" for s in approval_denominator_statuses)

    sql = f"""
    -- ── CTE 1: join payments with merchant dimension ─────────────────────
    WITH joined AS (
        SELECT
            CAST(p.event_date AS VARCHAR)           AS event_date,
            p.merchant_id,
            m.merchant_name,
            m.merchant_category,
            p.currency,
            p.status,
            p.transaction_id,
            TRY_CAST(p.amount AS DOUBLE)            AS amount,
            p.payment_method
        FROM bronze_df  AS p
        LEFT JOIN merchants_df AS m
            ON p.merchant_id = m.merchant_id
    ),

    -- ── CTE 2: approval rate at (event_date, merchant_id, currency) grain ─
    -- NULLIF prevents division-by-zero on the denominator;
    -- returns NULL only when there are zero card APPROVED/DECLINED txns.
    -- When approved=0 but declined>0, returns 0.0 correctly (not NULL).
    approval AS (
        SELECT
            event_date,
            merchant_id,
            currency,
            COUNT_IF(payment_method = '{approval_rate_payment_method}'
                     AND status = '{approved_status}') * 1.0
            /
            NULLIF(
                COUNT_IF(payment_method = '{approval_rate_payment_method}'
                         AND status IN ({denom_statuses_sql})),
                0
            )                                       AS approval_rate
        FROM joined
        GROUP BY event_date, merchant_id, currency
    )

    -- ── Final SELECT: full grain aggregation ──────────────────────────────
    SELECT
        j.event_date,
        j.merchant_id,
        j.merchant_name,
        j.merchant_category,
        j.currency,
        j.status,
        COUNT(j.transaction_id)                     AS transaction_count,
        ROUND(SUM(j.amount),      2)                AS total_amount,
        ROUND(AVG(j.amount),      4)                AS avg_amount,
        MAX(j.amount)                               AS max_amount,
        ROUND(a.approval_rate,    4)                AS approval_rate
    FROM joined AS j
    LEFT JOIN approval AS a
        ON  j.event_date   = a.event_date
        AND j.merchant_id  = a.merchant_id
        AND j.currency     = a.currency
    GROUP BY
        j.event_date,
        j.merchant_id,
        j.merchant_name,
        j.merchant_category,
        j.currency,
        j.status,
        a.approval_rate
    ORDER BY j.event_date, j.merchant_id, j.currency, j.status
    """

    result = duckdb.query(sql).df()
    logger.info("daily_payment_summary: %d rows produced.", len(result))
    return result


# ---------------------------------------------------------------------------
# Table 2: merchant_performance_Nd  (parameterised rolling window)
# ---------------------------------------------------------------------------

def build_merchant_performance_rolling(
    bronze_df: pd.DataFrame,
    merchants_df: pd.DataFrame,
    window_days: int = 7,
    approved_status: str = "APPROVED",
    approval_denominator_statuses: List[str] = None,
    approval_rate_payment_method: str = "CARD",
    reversed_status: str = "REVERSED",
) -> pd.DataFrame:
    """
    Produce the merchant_performance_{N}d Gold table using DuckDB SQL.

    Grain: one row per (snapshot_date, merchant_id).
    Each snapshot_date row summarises the rolling *window_days* ending on that date.

    The *window_days* parameter is driven by config — changing it from 7 to 30
    requires only a config.yaml edit (Bonus B4 differentiator).

    SQL approach
    ------------
    A CROSS JOIN between the distinct date list and the distinct merchant list
    produces all (snapshot_date, merchant_id) combinations.  Each combination
    then self-joins back to the transactions table using a date-range predicate
    that implements the rolling window:

        event_date BETWEEN snapshot_date - INTERVAL N DAYS AND snapshot_date

    All aggregations (total transactions, approved amount, approval rate,
    reversal rate, active days) are computed in a single SQL pass — no Python
    loops required.
    """
    if approval_denominator_statuses is None:
        approval_denominator_statuses = ["APPROVED", "DECLINED"]

    if bronze_df.empty:
        logger.warning("Bronze DataFrame is empty — merchant_performance table will be empty.")
        return pd.DataFrame()

    denom_statuses_sql = ", ".join(f"'{s}'" for s in approval_denominator_statuses)

    # Column aliases are built dynamically so they reflect window_days
    total_col        = f"total_transactions_{window_days}d"
    approved_amt_col = f"total_approved_amount_{window_days}d"
    approval_rate_col = f"approval_rate_{window_days}d"
    reversal_rate_col = f"reversal_rate_{window_days}d"
    active_days_col  = f"active_days_{window_days}d"

    sql = f"""
    -- ── CTE 1: cast & join payments with merchants ────────────────────────
    WITH base AS (
        SELECT
            p.merchant_id,
            m.merchant_name,
            CAST(p.event_date AS DATE)              AS event_date,
            TRY_CAST(p.amount AS DOUBLE)            AS amount,
            p.status,
            p.payment_method
        FROM bronze_df  AS p
        LEFT JOIN merchants_df AS m
            ON p.merchant_id = m.merchant_id
    ),

    -- ── CTE 2: all distinct snapshot dates ───────────────────────────────
    dates AS (
        SELECT DISTINCT event_date AS snapshot_date
        FROM base
        WHERE event_date IS NOT NULL
    ),

    -- ── CTE 3: all distinct merchants ────────────────────────────────────
    merchants AS (
        SELECT DISTINCT merchant_id, FIRST(merchant_name) AS merchant_name
        FROM base
        GROUP BY merchant_id
    ),

    -- ── CTE 4: cross-join dates x merchants, then aggregate the window ───
    -- The WHERE clause in the sub-join implements the N-day rolling window:
    --   event_date BETWEEN snapshot_date - (N-1) days AND snapshot_date
    rolling AS (
        SELECT
            d.snapshot_date,
            mer.merchant_id,
            mer.merchant_name,

            -- Total transactions in window
            COUNT(b.event_date)                                     AS {total_col},

            -- Total approved amount in window
            ROUND(
                COALESCE(SUM(CASE WHEN b.status = '{approved_status}'
                                  THEN b.amount ELSE 0 END), 0), 2) AS {approved_amt_col},

            -- Approval rate = approved card txns / (approved + declined card txns)
            -- NULLIF on denominator guards against divide-by-zero → returns NULL.
            -- When approved=0 but declined>0, correctly returns 0.0 (not NULL).
            ROUND(
                COUNT_IF(b.payment_method = '{approval_rate_payment_method}'
                         AND b.status = '{approved_status}') * 1.0
                /
                NULLIF(
                    COUNT_IF(b.payment_method = '{approval_rate_payment_method}'
                             AND b.status IN ({denom_statuses_sql})),
                    0
                ), 4)                                               AS {approval_rate_col},

            -- Reversal rate = reversed txns / total txns
            ROUND(
                NULLIF(
                    COUNT_IF(b.status = '{reversed_status}'), 0
                ) * 1.0
                /
                NULLIF(COUNT(b.event_date), 0), 4)                 AS {reversal_rate_col},

            -- Active days (distinct dates with at least one transaction)
            COUNT(DISTINCT b.event_date)                           AS {active_days_col}

        FROM dates AS d
        CROSS JOIN merchants AS mer
        LEFT JOIN base AS b
            ON  b.merchant_id = mer.merchant_id
            AND b.event_date  BETWEEN
                    (d.snapshot_date - INTERVAL ({window_days} - 1) DAY)
                    AND d.snapshot_date
        GROUP BY d.snapshot_date, mer.merchant_id, mer.merchant_name
    )

    -- ── Final SELECT: exclude rows with zero activity in the window ───────
    SELECT
        CAST(snapshot_date AS DATE)   AS snapshot_date,
        merchant_id,
        merchant_name,
        {total_col},
        {approved_amt_col},
        {approval_rate_col},
        {reversal_rate_col},
        {active_days_col}
    FROM rolling
    WHERE {total_col} > 0
    ORDER BY snapshot_date, merchant_id
    """

    result = duckdb.query(sql).df()
    # Ensure snapshot_date is a plain YYYY-MM-DD string (not a Timestamp with time)
    # so partition directory names are valid filesystem paths.
    if "snapshot_date" in result.columns:
        result["snapshot_date"] = result["snapshot_date"].astype(str).str[:10]
    logger.info("merchant_performance_%dd: %d rows produced.", window_days, len(result))
    return result
