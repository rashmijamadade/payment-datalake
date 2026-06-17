"""
observability.py  (Bonus B1)
-----------------------------
Reusable structured observability framework for Bronze and Gold pipeline stages.

Design goal: a single PipelineObserver class that works as a context manager
for any pipeline stage — no duplicate logging logic between Bronze and Gold.

Emits a structured JSON run report to output/run_reports/ after each stage.

Run report schema:
  {
    "run_id": str,
    "pipeline_stage": str,            # "bronze" | "gold"
    "start_ts": ISO8601 str,
    "end_ts": ISO8601 str,
    "duration_seconds": float,
    "status": "SUCCESS" | "PARTIAL" | "FAILED",
    "tables": {
      "<table_name>": {
        "rows_read": int,
        "rows_written": int,
        "rows_quarantined": int
      }
    }
  }
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Generator

logger = logging.getLogger(__name__)


class TableStats:
    """Accumulates per-table row counts during a pipeline run."""

    def __init__(self) -> None:
        self.rows_read: int = 0
        self.rows_written: int = 0
        self.rows_quarantined: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_quarantined": self.rows_quarantined,
        }


class PipelineObserver:
    """
    Observability context for a single pipeline stage (Bronze or Gold).

    Usage
    -----
    observer = PipelineObserver(run_id="abc", stage="bronze", output_dir="output/run_reports")
    with observer.track():
        observer.record("payments", rows_read=15, rows_written=15)
        ...
    # Report is automatically emitted on context manager exit.

    The same class works identically for Bronze and Gold — no duplication.
    """

    def __init__(
        self,
        run_id: str,
        stage: str,
        output_dir: str | Path,
    ) -> None:
        self.run_id = run_id
        self.stage = stage
        self.output_dir = Path(output_dir)
        self._tables: Dict[str, TableStats] = {}
        self._start_ts: datetime | None = None
        self._end_ts: datetime | None = None
        self._status: str = "SUCCESS"
        self._start_time: float = 0.0

    def record(
        self,
        table_name: str,
        rows_read: int = 0,
        rows_written: int = 0,
        rows_quarantined: int = 0,
    ) -> None:
        """Record row counts for a specific table/source within this stage."""
        if table_name not in self._tables:
            self._tables[table_name] = TableStats()
        stats = self._tables[table_name]
        stats.rows_read += rows_read
        stats.rows_written += rows_written
        stats.rows_quarantined += rows_quarantined

        if rows_quarantined > 0:
            # Any quarantined rows = PARTIAL success
            if self._status == "SUCCESS":
                self._status = "PARTIAL"

    def mark_failed(self) -> None:
        """Mark this stage as FAILED (e.g. on unhandled exception)."""
        self._status = "FAILED"

    def emit_report(self) -> Path:
        """Write the structured JSON run report and return its path."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        end_ts = self._end_ts or datetime.now(timezone.utc)
        start_ts = self._start_ts or end_ts
        duration = (end_ts - start_ts).total_seconds()

        report: Dict[str, Any] = {
            "run_id": self.run_id,
            "pipeline_stage": self.stage,
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "duration_seconds": round(duration, 3),
            "status": self._status,
            "tables": {name: stats.to_dict() for name, stats in self._tables.items()},
        }

        report_path = self.output_dir / f"{self.run_id}_{self.stage}.json"
        with open(report_path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)

        logger.info(
            "Observability report [%s / %s] → %s | status=%s | duration=%.3fs",
            self.stage,
            self.run_id,
            report_path,
            self._status,
            duration,
        )
        return report_path

    @contextmanager
    def track(self) -> Generator[PipelineObserver, None, None]:
        """
        Context manager that records start/end times and emits the report on exit.
        Marks stage as FAILED if an exception propagates.
        """
        self._start_ts = datetime.now(timezone.utc)
        self._start_time = time.monotonic()
        try:
            yield self
        except Exception:
            self.mark_failed()
            raise
        finally:
            self._end_ts = datetime.now(timezone.utc)
            self.emit_report()
