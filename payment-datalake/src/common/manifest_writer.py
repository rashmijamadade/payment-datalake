"""
manifest_writer.py
-------------------
Writes a JSON run manifest to output/bronze/_manifests/ after each Bronze run.

Kept intentionally minimal — one function, one responsibility.
The manifest is the audit trail for Bronze ingestion runs.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)


def write_manifest(
    run_id: str,
    files_processed: list[str],
    row_counts: Dict[str, Dict[str, int]],
    status: str,
    output_dir: str | Path,
    run_ts: datetime | None = None,
    extra: Dict[str, Any] | None = None,
) -> Path:
    """
    Write a JSON run manifest and return the path to the written file.

    Parameters
    ----------
    run_id:
        Unique identifier for this pipeline run.
    files_processed:
        List of source filenames that were attempted in this run.
    row_counts:
        Per-file row count dict:
          { filename: { "read": int, "written": int, "skipped": int } }
    status:
        One of "SUCCESS", "PARTIAL", "FAILED".
    output_dir:
        Directory to write the manifest JSON into.
    run_ts:
        Timestamp for this run. Defaults to utcnow().
    extra:
        Any additional key-value pairs to include in the manifest.

    Returns
    -------
    Path
        Absolute path to the written manifest file.
    """
    if run_ts is None:
        run_ts = datetime.now(timezone.utc)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest: Dict[str, Any] = {
        "run_id": run_id,
        "run_ts": run_ts.isoformat(),
        "status": status,
        "files_processed": files_processed,
        "row_counts": row_counts,
    }
    if extra:
        manifest.update(extra)

    manifest_path = output_dir / f"{run_id}.json"
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, default=str)

    logger.info("Run manifest written → %s", manifest_path)
    return manifest_path
