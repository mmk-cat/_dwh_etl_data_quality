"""Load local CSV warehouse tables into MySQL and execute the same 24 rules.

Verified against MariaDB 11.4.13 (MySQL protocol).  This closes the gap between
the lightweight SQLite test runner and the MySQL stack named in the resume.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymysql
from pymysql.cursors import DictCursor

from .rules import RULES

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
WAREHOUSE = ROOT / "data" / "warehouse"
SCHEMA = ROOT / "sql" / "mysql" / "schema.sql"
REPORTS = ROOT / "reports"

TABLES = {
    "ods_orders": RAW / "ods_orders.csv",
    "ods_users": RAW / "ods_users.csv",
    "ods_products": RAW / "ods_products.csv",
    "dim_user": WAREHOUSE / "dim_user.csv",
    "dim_product": WAREHOUSE / "dim_product.csv",
    "dwd_order_detail": WAREHOUSE / "dwd_order_detail.csv",
    "dws_daily_sales": WAREHOUSE / "dws_daily_sales.csv",
    "dws_channel_daily": WAREHOUSE / "dws_channel_daily.csv",
}


def _connect(host: str, port: int, user: str, password: str, database: str):
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=DictCursor,
        autocommit=False,
    )


def _ensure_schema(host: str, port: int, user: str, password: str, database: str) -> None:
    admin = pymysql.connect(
        host=host, port=port, user=user, password=password, charset="utf8mb4", autocommit=True
    )
    try:
        with admin.cursor() as cursor:
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{database}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
    finally:
        admin.close()
    statements = [part.strip() for part in SCHEMA.read_text(encoding="utf-8").split(";") if part.strip()]
    conn = _connect(host, port, user, password, database)
    try:
        with conn.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        conn.commit()
    finally:
        conn.close()


def _load_table(conn, table: str, path: Path) -> int:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")
    columns = list(frame.columns)
    placeholders = ", ".join(["%s"] * len(columns))
    column_sql = ", ".join(f"`{column}`" for column in columns)
    rows = []
    for row in frame.itertuples(index=False, name=None):
        normalized = []
        for value in row:
            if value is None or value == "" or pd.isna(value):
                normalized.append(None)
            else:
                normalized.append(value)
        rows.append(tuple(normalized))
    with conn.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE `{table}`")
        cursor.executemany(
            f"INSERT INTO `{table}` ({column_sql}) VALUES ({placeholders})", rows
        )
    return len(rows)


def run(host: str, port: int, user: str, password: str, database: str) -> dict:
    _ensure_schema(host, port, user, password, database)
    conn = _connect(host, port, user, password, database)
    try:
        counts = {table: _load_table(conn, table, path) for table, path in TABLES.items()}
        conn.commit()
        rule_rows = []
        anomalies = []
        for rule in RULES:
            with conn.cursor() as cursor:
                cursor.execute(rule.sql)
                rows = cursor.fetchall()
            rule_rows.append({"rule_code": rule.code, "hits": len(rows)})
            for row in rows:
                row = {key: (str(value) if value is not None else "") for key, value in row.items()}
                row.update({"rule_code": rule.code, "scope": rule.scope})
                anomalies.append(row)
        conn.commit()
    finally:
        conn.close()

    order_ids = sorted(
        {
            item["record_key"]
            for item in anomalies
            if item.get("scope") == "orders" and item.get("record_key")
        }
    )
    dim_ids = sorted(
        {
            item["record_key"]
            for item in anomalies
            if item.get("scope") == "dimension" and item.get("record_key")
        }
    )
    metric_alerts = [item for item in anomalies if item.get("scope") == "metrics"]
    result = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "engine": "MySQL/MariaDB via PyMySQL",
        "database": database,
        "table_rows": counts,
        "rules_total": len(RULES),
        "rules_triggered": sum(1 for item in rule_rows if item["hits"] > 0),
        "anomaly_rows": len(anomalies),
        "dirty_order_count": len(order_ids),
        "dimension_anomaly_count": len(dim_ids),
        "metric_alert_count": len(metric_alerts),
        "rule_hits": {item["rule_code"]: item["hits"] for item in rule_rows},
        "dirty_order_ids": order_ids,
        "dimension_keys": dim_ids,
        "passed": len(order_ids) == 34 and len(dim_ids) == 1 and len(anomalies) == 43,
    }
    REPORTS.mkdir(parents=True, exist_ok=True)
    (REPORTS / "mysql_dq.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="book_admin")
    parser.add_argument("--password", default="book_admin_pwd")
    parser.add_argument("--database", default="dwh_dq")
    args = parser.parse_args()
    result = run(args.host, args.port, args.user, args.password, args.database)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
