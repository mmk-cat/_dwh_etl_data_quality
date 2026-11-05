"""ETL completeness, DWS reconciliation, quarantine and GMV impact tests."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]

pytestmark = [pytest.mark.pipeline]


def _csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")


def test_source_and_dwd_row_counts(artifacts, manifest):
    metrics = json.loads((ROOT / "data/warehouse/etl_metrics.json").read_text(encoding="utf-8"))
    assert manifest["n_base_orders"] == 2000
    assert manifest["source_order_rows"] == 2002
    assert metrics["source_order_rows"] == 2002
    assert metrics["dwd_rows"] == 2002
    assert metrics["distinct_order_ids"] == 2000
    assert metrics["dws_daily_rows"] == 79
    assert metrics["dws_channel_rows"] == 44


def test_dimension_misalignment_is_visible():
    source = _csv(ROOT / "data/raw/ods_products.csv")
    dim = _csv(ROOT / "data/warehouse/dim_product.csv")
    joined = source.merge(dim, on="product_id", suffixes=("_source", "_dim"))
    mismatches = joined[joined["category_source"] != joined["category_dim"]]
    assert len(mismatches) == 1
    assert mismatches.iloc[0]["product_id"] == "P0007"
    assert mismatches.iloc[0]["category_source"] == "数码"
    assert mismatches.iloc[0]["category_dim"] == "家电"


def test_invalid_and_future_partitions():
    metrics = json.loads((ROOT / "data/warehouse/etl_metrics.json").read_text(encoding="utf-8"))
    assert metrics["invalid_date_rows"] == 2
    assert metrics["future_rows"] == 1
    dwd = _csv(ROOT / "data/warehouse/dwd_order_detail.csv")
    assert (dwd["order_date"] == "INVALID").sum() == 2
    assert (dwd["order_date"] == "2099-01-01").sum() == 1
    dws = _csv(ROOT / "data/warehouse/dws_daily_sales.csv")
    assert "INVALID" in set(dws["order_date"])
    assert "2099-01-01" in set(dws["order_date"])


def test_dws_daily_gmv_reconciles_to_dwd():
    dwd = _csv(ROOT / "data/warehouse/dwd_order_detail.csv")
    dws = _csv(ROOT / "data/warehouse/dws_daily_sales.csv")
    dwd["amount_num"] = pd.to_numeric(dwd["amount"], errors="coerce")
    expected = (
        dwd.groupby(["order_date", "category"])["amount_num"]
        .sum()
        .round(2)
        .rename("expected_gmv")
        .reset_index()
    )
    actual = dws[["order_date", "category", "gmv"]].copy()
    actual["gmv"] = pd.to_numeric(actual["gmv"])
    merged = actual.merge(expected, on=["order_date", "category"], how="outer")
    assert len(merged) == len(actual) == len(expected)
    assert (merged["gmv"] - merged["expected_gmv"]).abs().max() <= 0.01


def test_quarantine_and_clean_row_counts(remediation):
    quarantine = _csv(ROOT / "data/quarantine/orders.csv")
    clean = _csv(ROOT / "data/quarantine/ods_orders_clean.csv")
    assert remediation["detected_dirty_order_count"] == 34
    assert remediation["quarantined_order_rows"] == 36
    assert len(quarantine) == 36
    assert quarantine["order_id"].nunique() == 34
    assert len(clean) == 1966
    assert "quarantine_reason" in quarantine.columns
    assert quarantine["quarantine_reason"].str.len().min() > 0


def test_clean_warehouse_has_no_anomalies(remediation):
    after = remediation["dq_after"]
    assert after["summary"]["dirty_order_count"] == 0
    assert after["summary"]["dimension_anomaly_count"] == 0
    assert after["summary"]["anomaly_rows"] == 0
    assert after["summary"]["rules_passed"] == 24
    clean = _csv(ROOT / "data/warehouse_clean/dwd_order_detail.csv")
    assert len(clean) == 1966
    assert (clean["order_date"] == "INVALID").sum() == 0
    assert (clean["order_date"] == "2099-01-01").sum() == 0


def test_gmv_delta_equals_dirty_amount(remediation):
    assert remediation["gmv_before"] > remediation["gmv_after_clean"]
    assert remediation["gmv_delta"] == pytest.approx(remediation["impact"]["dirty_amount"], abs=0.01)
    assert remediation["gmv_delta"] == pytest.approx(71199.64, abs=0.01)
    assert remediation["impact"]["affected_dws_cells"] == 29


def test_etl_rebuild_is_deterministic(tmp_path):
    from src.etl.local_pipeline import build_warehouse

    metrics = build_warehouse(warehouse_dir=tmp_path)
    baseline = json.loads((ROOT / "data/warehouse/etl_metrics.json").read_text(encoding="utf-8"))
    assert metrics["dwd_rows"] == baseline["dwd_rows"]
    assert metrics["dwd_gmv"] == baseline["dwd_gmv"]
    assert metrics["dws_daily_rows"] == baseline["dws_daily_rows"]
    assert len(_csv(tmp_path / "dwd_order_detail.csv")) == 2002
