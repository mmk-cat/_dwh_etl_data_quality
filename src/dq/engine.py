"""Local DQ runner: load CSV tables into SQLite and execute the rules."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from .rules import RULES, Rule

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
WAREHOUSE_DIR = ROOT / "data" / "warehouse"
REPORTS_DIR = ROOT / "reports"


@dataclass
class RuleResult:
    rule: Rule
    rows: list[dict[str, Any]] = field(default_factory=list)

    @property
    def hits(self) -> int:
        return len(self.rows)

    @property
    def sample_keys(self) -> list[str]:
        keys: list[str] = []
        for row in self.rows:
            key = row.get("record_key")
            if key is not None and str(key) not in keys:
                keys.append(str(key))
        return keys[:8]


def _read(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")


def load_tables(raw_dir: Path = RAW_DIR, warehouse_dir: Path = WAREHOUSE_DIR) -> dict[str, pd.DataFrame]:
    required = {
        "ods_orders": raw_dir / "ods_orders.csv",
        "ods_users": raw_dir / "ods_users.csv",
        "ods_products": raw_dir / "ods_products.csv",
        "dim_user": warehouse_dir / "dim_user.csv",
        "dim_product": warehouse_dir / "dim_product.csv",
        "dwd_order_detail": warehouse_dir / "dwd_order_detail.csv",
        "dws_daily_sales": warehouse_dir / "dws_daily_sales.csv",
        "dws_channel_daily": warehouse_dir / "dws_channel_daily.csv",
    }
    missing = [str(path) for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing warehouse inputs:\n" + "\n".join(missing))
    return {name: _read(path) for name, path in required.items()}


class DQEngine:
    def __init__(self, tables: dict[str, pd.DataFrame]) -> None:
        self.tables = tables
        self.conn = sqlite3.connect(":memory:")
        for name, frame in tables.items():
            frame.to_sql(name, self.conn, index=False, if_exists="replace")

    def close(self) -> None:
        self.conn.close()

    def execute(self, rule: Rule) -> RuleResult:
        try:
            frame = pd.read_sql_query(rule.sql, self.conn)
        except Exception as exc:
            raise RuntimeError(f"Rule {rule.code} SQL failed: {exc}\n{rule.sql}") from exc
        rows = frame.to_dict(orient="records")
        return RuleResult(rule=rule, rows=rows)

    def run_all(self) -> dict[str, Any]:
        results = [self.execute(rule) for rule in RULES]
        anomalies: list[dict[str, Any]] = []
        for result in results:
            for row in result.rows:
                item = {
                    "rule_code": result.rule.code,
                    "rule_name": result.rule.name,
                    "category": result.rule.category,
                    "scope": result.rule.scope,
                    "table": result.rule.table,
                    "severity": result.rule.severity,
                    "record_key": str(row.get("record_key", "")),
                    "detail": str(row.get("detail", "")),
                }
                if row.get("source_value") not in (None, ""):
                    item["source_value"] = str(row["source_value"])
                if row.get("dim_value") not in (None, ""):
                    item["dim_value"] = str(row["dim_value"])
                if row.get("change_pct") not in (None, ""):
                    item["change_pct"] = str(row["change_pct"])
                anomalies.append(item)

        order_ids = sorted(
            {
                item["record_key"]
                for item in anomalies
                if item["scope"] == "orders" and item["record_key"]
            }
        )
        dimension_ids = sorted(
            {
                item["record_key"]
                for item in anomalies
                if item["scope"] == "dimension" and item["record_key"]
            }
        )
        metric_alerts = [item for item in anomalies if item["scope"] == "metrics"]
        rule_hits = {
            result.rule.code: {
                "name": result.rule.name,
                "category": result.rule.category,
                "scope": result.rule.scope,
                "severity": result.rule.severity,
                "hits": result.hits,
                "sample_keys": result.sample_keys,
                "description": result.rule.description,
            }
            for result in results
        }
        return {
            "summary": {
                "rules_total": len(RULES),
                "rules_triggered": sum(1 for result in results if result.hits > 0),
                "rules_passed": sum(1 for result in results if result.hits == 0),
                "dirty_order_count": len(order_ids),
                "dimension_anomaly_count": len(dimension_ids),
                "anomaly_rows": len(anomalies),
                "metric_alert_count": len(metric_alerts),
            },
            "dirty_order_ids": order_ids,
            "dimension_keys": dimension_ids,
            "metric_alerts": metric_alerts,
            "anomalies": anomalies,
            "rule_hits": rule_hits,
            "order_rows": int(len(self.tables["ods_orders"])),
            "distinct_order_ids": int(self.tables["ods_orders"]["order_id"].nunique()),
        }

    def save(self, result: dict[str, Any], output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        anomalies = pd.DataFrame(result["anomalies"])
        anomalies.to_csv(output_dir / "anomalies.csv", index=False, encoding="utf-8")
        rule_rows = [
            {
                "rule_code": code,
                "name": meta["name"],
                "category": meta["category"],
                "scope": meta["scope"],
                "severity": meta["severity"],
                "hits": meta["hits"],
                "sample_keys": ", ".join(meta["sample_keys"]),
            }
            for code, meta in result["rule_hits"].items()
        ]
        pd.DataFrame(rule_rows).to_csv(output_dir / "rule_hits.csv", index=False, encoding="utf-8")
        payload = {
            "summary": result["summary"],
            "dirty_order_ids": result["dirty_order_ids"],
            "dimension_keys": result["dimension_keys"],
            "metric_alerts": result["metric_alerts"],
            "rule_hits": result["rule_hits"],
            "order_rows": result["order_rows"],
            "distinct_order_ids": result["distinct_order_ids"],
        }
        (output_dir / "dq_summary.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )


def run_dq(
    *,
    raw_dir: Path = RAW_DIR,
    warehouse_dir: Path = WAREHOUSE_DIR,
    output_dir: Path = REPORTS_DIR / "dq",
) -> dict[str, Any]:
    engine = DQEngine(load_tables(raw_dir, warehouse_dir))
    try:
        result = engine.run_all()
    finally:
        engine.close()
    engine.save(result, output_dir)
    return result


if __name__ == "__main__":
    result = run_dq()
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
