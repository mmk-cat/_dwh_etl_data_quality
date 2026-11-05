"""Pandas reference ETL for local execution and tests.

The naive job intentionally performs **no defensive cleaning**: malformed rows
flow into DWD/DWS so that the data-quality suite can detect them.  A separate
remediation step quarantines them and rebuilds clean marts.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
WAREHOUSE_DIR = ROOT / "data" / "warehouse"

ORDER_DATE_INVALID = "INVALID"


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def build_dimensions(
    users: pd.DataFrame, products: pd.DataFrame, *, corrupt_dimension: bool = True
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dim_user = users.copy()
    dim_product = products.copy()
    if corrupt_dimension:
        mask = dim_product["product_id"] == "P0007"
        dim_product.loc[mask, "category"] = "家电"
    return dim_user, dim_product


def build_dwd(
    orders: pd.DataFrame,
    dim_user: pd.DataFrame,
    dim_product: pd.DataFrame,
) -> pd.DataFrame:
    df = orders.copy()
    df["quantity_num"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price_num"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["amount_num"] = pd.to_numeric(df["amount"], errors="coerce")
    df["coupon_num"] = pd.to_numeric(df["coupon_amount"], errors="coerce").fillna(0.0)
    parsed = pd.to_datetime(df["order_time"], errors="coerce", format="%Y-%m-%d %H:%M:%S")
    df["order_date"] = parsed.dt.strftime("%Y-%m-%d").fillna(ORDER_DATE_INVALID)
    df["order_ts"] = parsed

    user_cols = dim_user[["user_id", "user_name", "city", "status"]].rename(
        columns={"status": "user_status"}
    )
    product_cols = dim_product[
        ["product_id", "product_name", "category", "list_price", "status"]
    ].rename(columns={"status": "product_status"})
    merged = df.merge(user_cols, on="user_id", how="left").merge(
        product_cols, on="product_id", how="left"
    )
    for column in ("user_name", "city", "user_status", "product_name", "category", "product_status"):
        merged[column] = merged[column].fillna("UNKNOWN")
    merged["list_price_num"] = pd.to_numeric(merged["list_price"], errors="coerce")

    dwd = pd.DataFrame(
        {
            "order_id": merged["order_id"],
            "user_id": merged["user_id"],
            "user_name": merged["user_name"],
            "city": merged["city"],
            "product_id": merged["product_id"],
            "product_name": merged["product_name"],
            "category": merged["category"],
            "quantity": merged["quantity_num"],
            "unit_price": merged["unit_price_num"],
            "list_price": merged["list_price_num"],
            "amount": merged["amount_num"],
            "coupon_amount": merged["coupon_num"],
            "order_status": merged["order_status"],
            "order_time": merged["order_time"],
            "order_date": merged["order_date"],
            "pay_time": merged["pay_time"],
            "channel": merged["channel"],
            "province": merged["province"],
            "user_status": merged["user_status"],
            "product_status": merged["product_status"],
        }
    )
    dwd["quantity"] = dwd["quantity"].astype("Int64")
    return dwd


def build_dws(dwd: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily = (
        dwd.groupby(["order_date", "category"], dropna=False)
        .agg(
            order_count=("order_id", "count"),
            buyer_count=("user_id", "nunique"),
            gmv=("amount", lambda series: round(series.sum(), 2)),
            refund_amount=(
                "amount",
                lambda series: round(
                    float(
                        dwd.loc[series.index, "order_status"]
                        .eq("refunded")
                        .mul(series.abs())
                        .sum()
                    ),
                    2,
                ),
            ),
            avg_order_amount=("amount", lambda series: round(series.mean(), 2)),
        )
        .reset_index()
    )
    channel = (
        dwd.groupby(["order_date", "channel"], dropna=False)
        .agg(
            order_count=("order_id", "count"),
            gmv=("amount", lambda series: round(series.sum(), 2)),
        )
        .reset_index()
    )
    return daily, channel


def build_warehouse(
    *,
    orders_path: Path | None = None,
    warehouse_dir: Path | None = None,
    corrupt_dimension: bool = True,
) -> dict[str, Any]:
    orders_path = orders_path or RAW_DIR / "ods_orders.csv"
    warehouse_dir = warehouse_dir or WAREHOUSE_DIR
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    orders = _read_csv(orders_path)
    users = _read_csv(RAW_DIR / "ods_users.csv")
    products = _read_csv(RAW_DIR / "ods_products.csv")
    dim_user, dim_product = build_dimensions(
        users, products, corrupt_dimension=corrupt_dimension
    )
    dwd = build_dwd(orders, dim_user, dim_product)
    daily, channel = build_dws(dwd)

    _write_csv(dim_user, warehouse_dir / "dim_user.csv")
    _write_csv(dim_product, warehouse_dir / "dim_product.csv")
    _write_csv(dwd, warehouse_dir / "dwd_order_detail.csv")
    _write_csv(daily, warehouse_dir / "dws_daily_sales.csv")
    _write_csv(channel, warehouse_dir / "dws_channel_daily.csv")

    invalid_dates = int((dwd["order_date"] == ORDER_DATE_INVALID).sum())
    future_rows = int(
        ((dwd["order_date"] != ORDER_DATE_INVALID) & (dwd["order_date"] > "2026-05-31")).sum()
    )
    metrics = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "orders_path": _display_path(orders_path),
        "warehouse_dir": _display_path(warehouse_dir),
        "source_order_rows": int(len(orders)),
        "dwd_rows": int(len(dwd)),
        "dws_daily_rows": int(len(daily)),
        "dws_channel_rows": int(len(channel)),
        "dwd_gmv": round(float(dwd["amount"].sum()), 2),
        "invalid_date_rows": invalid_dates,
        "future_rows": future_rows,
        "distinct_order_ids": int(dwd["order_id"].nunique()),
        "dimension_rows": {"dim_user": int(len(dim_user)), "dim_product": int(len(dim_product))},
    }
    (warehouse_dir / "etl_metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return metrics


if __name__ == "__main__":
    print(json.dumps(build_warehouse(), ensure_ascii=False, indent=2))
