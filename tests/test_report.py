"""Report artifact and headline-number consistency checks."""

from __future__ import annotations

import json
from pathlib import Path

import openpyxl
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"

pytestmark = [pytest.mark.report]


def test_markdown_report_contains_headline_numbers():
    text = (REPORTS / "dq_report.md").read_text(encoding="utf-8")
    assert "24" in text
    assert "34" in text
    assert "P0007" in text
    assert "71,199.64" in text
    assert "Kafka" in text
    assert "24/24 条规则通过" in text


def test_excel_workbook_has_expected_sheets():
    workbook = openpyxl.load_workbook(REPORTS / "dq_report.xlsx")
    assert workbook.sheetnames == [
        "Summary",
        "RuleHits",
        "Anomalies",
        "DirtyOrders",
        "Impact",
        "Kafka",
    ]
    assert workbook["Summary"].max_row >= 20
    assert workbook["RuleHits"].max_row == 25
    assert workbook["DirtyOrders"].max_row == 35


def test_anomaly_csv_and_rule_hits_csv():
    anomalies = pd.read_csv(REPORTS / "dq" / "anomalies.csv", dtype=str, keep_default_na=False)
    rules = pd.read_csv(REPORTS / "dq" / "rule_hits.csv", dtype=str, keep_default_na=False)
    assert len(anomalies) == 43
    assert len(rules) == 24
    assert {"rule_code", "record_key", "detail", "severity"}.issubset(anomalies.columns)
    assert "P0007" in set(anomalies["record_key"])
    assert "ODS_ORDER_AMOUNT_CONSISTENT" in set(anomalies["rule_code"])


def test_project_summary_consistent_with_reports():
    summary = json.loads((REPORTS / "project_summary.json").read_text(encoding="utf-8"))
    remediation = json.loads((REPORTS / "remediation.json").read_text(encoding="utf-8"))
    assert summary["rules_total"] == 24
    assert summary["dirty_order_count"] == 34
    assert summary["dimension_anomaly_count"] == 1
    assert summary["dq_after_anomalies"] == 0
    assert summary["gmv_delta"] == remediation["gmv_delta"]
    assert summary["dq_passed"] is True
    assert summary["kafka_passed"] is True


def test_no_placeholder_tokens_in_deliverables():
    for relative in ("reports/dq_report.md", "README.md"):
        path = ROOT / relative
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        assert "\u3010X\u3011" not in text
        assert "\u3010Y\u3011" not in text

