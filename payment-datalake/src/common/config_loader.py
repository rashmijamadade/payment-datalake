"""
config_loader.py
----------------
Loads and validates the pipeline configuration from config.yaml.
Exposes a typed Config dataclass so all other modules import from here —
zero hardcoded paths or settings anywhere else in the codebase (Bonus B4).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml


@dataclass
class PathsConfig:
    input_payments: str
    input_merchants: str
    output_bronze_payments: str
    output_bronze_merchants: str
    output_gold_daily: str
    output_gold_merchant_7d: str
    bronze_payments_manifests: str
    bronze_merchants_manifests: str
    run_reports: str


@dataclass
class PaymentsConfig:
    """Schema and processing settings specific to the payments CSV dataset."""
    partition_key: str
    file_pattern: str
    required_columns: List[str]
    metadata_columns: List[str]
    hash_exclude_columns: List[str]


@dataclass
class MerchantsConfig:
    required_columns: List[str]
    hash_exclude_columns: List[str]


@dataclass
class GoldConfig:
    window_days: int
    overwrite_partitions: bool
    approved_status: str
    approval_denominator_statuses: List[str]
    approval_rate_payment_method: str
    reversed_status: str
    # Output schema contracts — expected columns in each Gold table
    output_schema_daily_payment_summary: List[str] = field(default_factory=list)
    output_schema_merchant_performance: List[str] = field(default_factory=list)


@dataclass
class Config:
    paths: PathsConfig
    payments: PaymentsConfig
    merchants: MerchantsConfig
    gold: GoldConfig
    # Absolute project root — set at load time
    project_root: Path = field(default_factory=Path.cwd)

    def resolve(self, relative_path: str) -> Path:
        """Return an absolute Path by joining project_root with a relative path."""
        return self.project_root / relative_path


def load_config(config_path: str | Path | None = None, project_root: Path | None = None) -> Config:
    """
    Load the pipeline configuration from config.yaml.

    Parameters
    ----------
    config_path:
        Path to config.yaml. Defaults to <project_root>/config.yaml.
    project_root:
        Root directory for resolving relative paths. Defaults to the directory
        containing config.yaml (or cwd if config_path is not supplied).

    Returns
    -------
    Config
        A fully populated, typed Config object.
    """
    if config_path is None:
        # Default: look for config.yaml alongside run_pipeline.py (project root)
        config_path = Path(__file__).parent.parent.parent / "config.yaml"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    if project_root is None:
        project_root = config_path.parent

    paths = PathsConfig(**raw["paths"])
    payments = PaymentsConfig(**raw["payments"])
    merchants = MerchantsConfig(**raw["merchants"])
    gold = GoldConfig(**raw["gold"])

    return Config(paths=paths, payments=payments, merchants=merchants, gold=gold, project_root=project_root)
