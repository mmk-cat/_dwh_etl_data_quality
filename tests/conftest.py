"""Session-level pipeline run shared by all ETL/DQ tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def artifacts() -> dict:
    from src.pipeline import run_all

    return run_all()


@pytest.fixture(scope="session")
def manifest(artifacts) -> dict:
    return json.loads((ROOT / "data/raw/dirty_manifest.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def dq(artifacts) -> dict:
    return json.loads((ROOT / "reports/dq/dq_summary.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def remediation(artifacts) -> dict:
    return json.loads((ROOT / "reports/remediation.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def kafka(artifacts) -> dict:
    return json.loads((ROOT / "reports/kafka_consistency.json").read_text(encoding="utf-8"))
