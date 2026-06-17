"""
test_manifest_writer.py
------------------------
Verifies that manifest_writer.write_manifest() produces a correctly structured
JSON file with accurate row_counts, status, files_processed, run_id, and run_ts.

This test was identified as a gap in the review — the manifest is described as
an audit artifact, so its correctness should be explicitly asserted.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.common.manifest_writer import write_manifest


class TestManifestWriterStructure:
    """Tests that write_manifest() produces valid, correctly structured JSON."""

    def test_manifest_written_to_correct_path(self, tmp_path: Path):
        """The manifest file should be created at <output_dir>/<run_id>.json."""
        run_id = "test-run-001"
        path = write_manifest(
            run_id=run_id,
            files_processed=["payments_2024_01_15.csv"],
            row_counts={"payments_2024_01_15.csv": {"read": 15, "written": 15, "skipped": 0}},
            status="SUCCESS",
            output_dir=tmp_path,
        )

        assert path.exists(), "Manifest file should exist after write_manifest()"
        assert path.name == f"{run_id}.json", f"Expected filename '{run_id}.json', got '{path.name}'"

    def test_manifest_is_valid_json(self, tmp_path: Path):
        """The written manifest should be parseable JSON."""
        path = write_manifest(
            run_id="test-run-002",
            files_processed=[],
            row_counts={},
            status="SUCCESS",
            output_dir=tmp_path,
        )

        with open(path, encoding="utf-8") as fh:
            content = json.load(fh)  # must not raise

        assert isinstance(content, dict), "Manifest content should be a JSON object"

    def test_manifest_contains_required_fields(self, tmp_path: Path):
        """The manifest must contain run_id, run_ts, status, files_processed, row_counts."""
        run_id = "test-run-003"
        path = write_manifest(
            run_id=run_id,
            files_processed=["a.csv", "b.csv"],
            row_counts={
                "a.csv": {"read": 10, "written": 10, "skipped": 0},
                "b.csv": {"read": 5, "written": 3, "skipped": 2},
            },
            status="PARTIAL",
            output_dir=tmp_path,
        )

        with open(path, encoding="utf-8") as fh:
            content = json.load(fh)

        assert content["run_id"] == run_id
        assert "run_ts" in content, "Manifest must contain run_ts"
        assert content["status"] == "PARTIAL"
        assert content["files_processed"] == ["a.csv", "b.csv"]
        assert content["row_counts"]["a.csv"]["read"] == 10
        assert content["row_counts"]["a.csv"]["written"] == 10
        assert content["row_counts"]["b.csv"]["skipped"] == 2

    def test_manifest_row_counts_are_accurate(self, tmp_path: Path):
        """Row counts in the manifest must exactly match what was passed in."""
        row_counts = {
            "payments_2024_01_15.csv": {"read": 15, "written": 15, "skipped": 0, "quarantined": 0},
            "payments_2024_01_15_resubmit.csv": {"read": 17, "written": 2, "skipped": 15, "quarantined": 0},
        }
        path = write_manifest(
            run_id="test-run-004",
            files_processed=list(row_counts.keys()),
            row_counts=row_counts,
            status="SUCCESS",
            output_dir=tmp_path,
        )

        with open(path, encoding="utf-8") as fh:
            content = json.load(fh)

        for fname, expected in row_counts.items():
            actual = content["row_counts"][fname]
            assert actual == expected, (
                f"Row counts for '{fname}' mismatch: expected {expected}, got {actual}"
            )

    def test_manifest_status_success(self, tmp_path: Path):
        """Status=SUCCESS is serialised correctly."""
        path = write_manifest(
            run_id="test-run-005",
            files_processed=["x.csv"],
            row_counts={"x.csv": {"read": 5, "written": 5, "skipped": 0}},
            status="SUCCESS",
            output_dir=tmp_path,
        )

        with open(path, encoding="utf-8") as fh:
            content = json.load(fh)

        assert content["status"] == "SUCCESS"

    def test_manifest_status_failed(self, tmp_path: Path):
        """Status=FAILED is serialised correctly."""
        path = write_manifest(
            run_id="test-run-006",
            files_processed=[],
            row_counts={},
            status="FAILED",
            output_dir=tmp_path,
        )

        with open(path, encoding="utf-8") as fh:
            content = json.load(fh)

        assert content["status"] == "FAILED"

    def test_manifest_run_ts_matches_provided_value(self, tmp_path: Path):
        """When run_ts is explicitly provided, it must be serialised correctly."""
        fixed_ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        path = write_manifest(
            run_id="test-run-007",
            files_processed=["y.csv"],
            row_counts={"y.csv": {"read": 3, "written": 3, "skipped": 0}},
            status="SUCCESS",
            output_dir=tmp_path,
            run_ts=fixed_ts,
        )

        with open(path, encoding="utf-8") as fh:
            content = json.load(fh)

        # ISO format includes timezone info
        assert "2024-01-15" in content["run_ts"], (
            f"Expected run_ts to contain '2024-01-15', got {content['run_ts']!r}"
        )

    def test_manifest_extra_fields_are_included(self, tmp_path: Path):
        """Extra key-value pairs passed via 'extra' must appear in the manifest."""
        path = write_manifest(
            run_id="test-run-008",
            files_processed=["z.csv"],
            row_counts={"z.csv": {"read": 1, "written": 1, "skipped": 0}},
            status="SUCCESS",
            output_dir=tmp_path,
            extra={"pipeline_version": "1.0.0", "triggered_by": "scheduler"},
        )

        with open(path, encoding="utf-8") as fh:
            content = json.load(fh)

        assert content.get("pipeline_version") == "1.0.0"
        assert content.get("triggered_by") == "scheduler"

    def test_manifest_output_dir_created_if_missing(self, tmp_path: Path):
        """write_manifest() should create the output directory if it doesn't exist."""
        nested_dir = tmp_path / "deeply" / "nested" / "manifests"
        assert not nested_dir.exists()

        write_manifest(
            run_id="test-run-009",
            files_processed=[],
            row_counts={},
            status="SUCCESS",
            output_dir=nested_dir,
        )

        assert nested_dir.exists(), "write_manifest() should create missing output directories"
