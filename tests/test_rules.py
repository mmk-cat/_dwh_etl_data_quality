"""Contract tests for each of the 24 data-quality rules."""

from __future__ import annotations

from collections import Counter

import pytest

from src.dq.rules import RULES

pytestmark = [pytest.mark.rules]


@pytest.mark.parametrize("rule", RULES, ids=[rule.code for rule in RULES])
def test_rule_hits_match_expected(rule, dq):
    meta = dq["rule_hits"][rule.code]
    if rule.expected_hits is None:
        assert meta["hits"] >= 0
    else:
        assert meta["hits"] == rule.expected_hits, (
            f"{rule.code}: expected {rule.expected_hits}, got {meta['hits']}"
        )
    assert isinstance(meta["sample_keys"], list)


def test_rule_catalog_is_complete(dq):
    assert len(RULES) == 24
    assert len({rule.code for rule in RULES}) == 24
    assert dq["summary"]["rules_total"] == 24
    assert all(rule.sql.strip().upper().startswith("SELECT") or "WITH" in rule.sql.upper() for rule in RULES)
    assert all(rule.description for rule in RULES)
    assert all(rule.severity in {"critical", "high", "medium"} for rule in RULES)


def test_category_distribution():
    counts = Counter(rule.category for rule in RULES)
    assert counts == {
        "非空率": 6,
        "唯一性": 3,
        "枚举合法": 3,
        "金额符号": 4,
        "表间一致": 4,
        "时间": 2,
        "维度一致性": 1,
        "日环比阈值": 1,
    }


def test_dirty_orders_match_manifest(dq, manifest):
    assert dq["summary"]["dirty_order_count"] == 34
    assert set(dq["dirty_order_ids"]) == set(manifest["dirty_order_ids"])
    assert len(dq["dirty_order_ids"]) == manifest["dirty_order_count"]


def test_dimension_mismatch_is_exactly_one(dq, manifest):
    assert dq["summary"]["dimension_anomaly_count"] == 1
    assert dq["dimension_keys"] == [manifest["dimension_defect"]["product_id"]]
    assert manifest["dimension_defect"]["product_id"] == "P0007"


def test_rules_triggered_and_metric_alerts(dq):
    assert dq["summary"]["rules_triggered"] == 13
    assert dq["summary"]["rules_passed"] == 11
    assert dq["summary"]["anomaly_rows"] == 43
    assert dq["summary"]["metric_alert_count"] == 2
    assert {item["record_key"] for item in dq["metric_alerts"]} == {"2099-01-01", "INVALID"}
