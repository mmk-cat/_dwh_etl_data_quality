"""Run the 24 portable SQL rules against Spark temp views (optional)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .rules import RULES

ROOT = Path(__file__).resolve().parents[2]


def run(raw: Path, warehouse: Path, output: Path) -> dict:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise SystemExit("PySpark 未安装: pip install pyspark==4.0.1") from exc

    spark = (
        SparkSession.builder.appName("dwh-dq")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .getOrCreate()
    )
    try:
        tables = {
            "ods_orders": raw / "ods_orders.csv",
            "ods_users": raw / "ods_users.csv",
            "ods_products": raw / "ods_products.csv",
            "dim_user": warehouse / "dim_user.csv",
            "dim_product": warehouse / "dim_product.csv",
            "dwd_order_detail": warehouse / "dwd_order_detail.csv",
            "dws_daily_sales": warehouse / "dws_daily_sales.csv",
            "dws_channel_daily": warehouse / "dws_channel_daily.csv",
        }
        for name, path in tables.items():
            (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(str(path))
                .createOrReplaceTempView(name)
            )
        results = []
        for rule in RULES:
            rows = [row.asDict() for row in spark.sql(rule.sql).collect()]
            results.append({"rule_code": rule.code, "hits": len(rows), "sample": rows[:5]})
        payload = {"rules": results, "total_hits": sum(item["hits"] for item in results)}
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )
        return payload
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", default=str(ROOT / "data" / "raw"))
    parser.add_argument("--warehouse", default=str(ROOT / "data" / "warehouse"))
    parser.add_argument("--output", default=str(ROOT / "reports" / "dq_spark.json"))
    args = parser.parse_args()
    print(json.dumps(run(Path(args.raw), Path(args.warehouse), Path(args.output)), indent=2, default=str))


if __name__ == "__main__":
    main()
