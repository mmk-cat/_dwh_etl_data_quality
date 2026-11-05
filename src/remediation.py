"""Quarantine detected dirty orders, rebuild a clean warehouse and verify again."""

from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .dq.engine import run_dq
from .etl.local_pipeline import build_dimensions, build_warehouse, _read_csv

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
QUARANTINE_DIR = ROOT / "data" / "quarantine"
CLEAN_RAW_DIR = QUARANTINE_DIR / "raw_clean"
CLEAN_WAREHOUSE_DIR = ROOT / "data" / "warehouse_clean"
REPORTS_DIR = ROOT / "reports"


def _dws_pairs(dwd: pd.DataFrame, dirty_ids: set[str]) -> dict[str, Any]:
    dirty = dwd[dwd["order_id"].isin(dirty_ids)]
    pairs = (
        dirty[["order_date", "category"]]
        .drop_duplicates()
        .sort_values(["order_date", "category"])
        .apply(lambda row: f"{row['order_date']}/{row['category']}", axis=1)
        .tolist()
    )
    return {
        "affected_dws_cells": len(pairs),
        "affected_pairs": pairs,
        "affected_dates": sorted(dirty["order_date"].unique().tolist()),
        "dirty_amount": round(float(pd.to_numeric(dirty["amount"], errors="coerce").sum()), 2),
    }


def remediate() -> dict[str, Any]:
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    CLEAN_RAW_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = REPORTS_DIR / "dq" / "dq_summary.json"
    if not summary_path.exists():
        run_dq()
    dq = json.loads(summary_path.read_text(encoding="utf-8"))
    dirty_ids = set(dq["dirty_order_ids"])
    anomalies = pd.read_csv(REPORTS_DIR / "dq" / "anomalies.csv", dtype=str, keep_default_na=False)

    orders = _read_csv(RAW_DIR / "ods_orders.csv")
    dirty_mask = orders["order_id"].isin(dirty_ids)
    quarantine = orders[dirty_mask].copy()
    reasons: dict[str, list[str]] = {}
    for _, row in anomalies.iterrows():
        key = str(row["record_key"])
        reasons.setdefault(key, [])
        label = f"{row['rule_code']}:{row['rule_name']}"
        if label not in reasons[key]:
            reasons[key].append(label)
    quarantine["quarantine_reason"] = quarantine["order_id"].map(
        lambda key: "; ".join(reasons.get(str(key), ["detected by DQ rules"]))
    )
    quarantine.to_csv(QUARANTINE_DIR / "orders.csv", index=False, encoding="utf-8")
    clean_orders = orders[~dirty_mask].copy()
    clean_orders.to_csv(QUARANTINE_DIR / "ods_orders_clean.csv", index=False, encoding="utf-8")

    # DQ needs a raw directory with all three source tables.
    shutil.copy2(QUARANTINE_DIR / "ods_orders_clean.csv", CLEAN_RAW_DIR / "ods_orders.csv")
    shutil.copy2(RAW_DIR / "ods_users.csv", CLEAN_RAW_DIR / "ods_users.csv")
    shutil.copy2(RAW_DIR / "ods_products.csv", CLEAN_RAW_DIR / "ods_products.csv")

    before = pd.read_csv(
        ROOT / "data" / "warehouse" / "dwd_order_detail.csv",
        dtype=str,
        keep_default_na=False,
        encoding="utf-8",
    )
    impact = _dws_pairs(before, dirty_ids)
    metrics = build_warehouse(
        orders_path=QUARANTINE_DIR / "ods_orders_clean.csv",
        warehouse_dir=CLEAN_WAREHOUSE_DIR,
        corrupt_dimension=False,
    )
    after_dq = run_dq(
        raw_dir=CLEAN_RAW_DIR,
        warehouse_dir=CLEAN_WAREHOUSE_DIR,
        output_dir=REPORTS_DIR / "dq_after",
    )
    after = pd.read_csv(
        CLEAN_WAREHOUSE_DIR / "dwd_order_detail.csv",
        dtype=str,
        keep_default_na=False,
        encoding="utf-8",
    )
    gmv_before = round(float(pd.to_numeric(before["amount"], errors="coerce").sum()), 2)
    gmv_after = round(float(pd.to_numeric(after["amount"], errors="coerce").sum()), 2)
    result = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "detected_dirty_order_count": len(dirty_ids),
        "quarantined_order_rows": int(len(quarantine)),
        "clean_order_rows": int(len(clean_orders)),
        "source_order_rows": int(len(orders)),
        "dimension_fix": {
            "product_id": "P0007",
            "from": "家电",
            "to": "数码",
        },
        "impact": impact,
        "gmv_before": gmv_before,
        "gmv_after_clean": gmv_after,
        "gmv_delta": round(gmv_before - gmv_after, 2),
        "warehouse_after": metrics,
        "dq_after": {
            "summary": after_dq["summary"],
            "metric_alerts": after_dq["metric_alerts"],
            "dirty_order_ids": after_dq["dirty_order_ids"],
            "dimension_keys": after_dq["dimension_keys"],
        },
    }
    (REPORTS_DIR / "remediation.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return result


if __name__ == "__main__":
    print(json.dumps(remediate(), ensure_ascii=False, indent=2))
