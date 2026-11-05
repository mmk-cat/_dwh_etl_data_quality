"""Deterministic ODS data generator with a documented dirty-data manifest.

Business facts are synthetic and contain no real personal data.  A fixed seed
makes the anomaly counts reproducible: **34 distinct dirty order IDs** and
**1 wrong product dimension row**.
"""

from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
SEED = 20260517
N_USERS = 80
N_PRODUCTS = 30
N_ORDERS = 2000
DAYS = 14
START = datetime(2026, 5, 1, 0, 0, 0)
CATEGORIES = ["数码", "家电", "服饰", "美妆", "食品"]
CHANNELS = ["app", "web", "mini_program"]
PAY_STATUSES = ["paid", "refunded", "cancelled", "pending"]
PROVINCES = ["安徽", "江苏", "浙江", "上海", "广东", "北京", "四川", "湖北"]
CITIES = ["合肥", "南京", "杭州", "上海", "广州", "北京", "成都", "武汉"]

DIRTY_GROUPS = [
    ("null_user_id", 6),
    ("null_product_id", 4),
    ("duplicate_order_id", 2),
    ("invalid_order_status", 4),
    ("negative_amount", 3),
    ("amount_mismatch", 5),
    ("orphan_user_id", 4),
    ("orphan_product_id", 3),
    ("invalid_order_time", 2),
    ("future_order_time", 1),
]


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _build_users(rnd: random.Random) -> list[dict[str, str]]:
    rows = []
    for index in range(1, N_USERS + 1):
        register = START - timedelta(days=rnd.randint(30, 900), hours=rnd.randint(0, 23))
        rows.append(
            {
                "user_id": f"U{index:04d}",
                "user_name": f"用户{index:03d}",
                "city": CITIES[index % len(CITIES)],
                "register_time": register.strftime("%Y-%m-%d %H:%M:%S"),
                "status": rnd.choices(["active", "inactive", "blocked"], [8, 1, 1])[0],
            }
        )
    return rows


def _build_products(rnd: random.Random) -> list[dict[str, str]]:
    rows = []
    for index in range(1, N_PRODUCTS + 1):
        category = CATEGORIES[index % len(CATEGORIES)]
        list_price = round(rnd.uniform(29, 1899), 2)
        rows.append(
            {
                "product_id": f"P{index:04d}",
                "product_name": f"商品{index:03d}",
                "category": category,
                "list_price": f"{list_price:.2f}",
                "status": "on_sale" if index % 11 else "off_sale",
            }
        )
    # P0007 is the single dimension row that upstream maps to the wrong category.
    rows[6]["category"] = "数码"
    return rows


def _build_orders(
    rnd: random.Random, users: list[dict[str, str]], products: list[dict[str, str]]
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    product_by_id = {row["product_id"]: row for row in products}
    orders: list[dict[str, str]] = []
    for index in range(1, N_ORDERS + 1):
        day = (index - 1) % DAYS
        order_time = START + timedelta(
            days=day, hours=rnd.randint(8, 22), minutes=rnd.randint(0, 59), seconds=rnd.randint(0, 59)
        )
        user_id = f"U{rnd.randint(1, N_USERS):04d}"
        product_id = f"P{rnd.randint(1, N_PRODUCTS):04d}"
        product = product_by_id[product_id]
        quantity = rnd.randint(1, 5)
        unit_price = round(float(product["list_price"]) * rnd.uniform(0.85, 1.0), 2)
        amount = round(quantity * unit_price, 2)
        status = rnd.choices(PAY_STATUSES, weights=[8, 2, 1, 1])[0]
        pay_time = (order_time + timedelta(minutes=rnd.randint(1, 15))).strftime(
            "%Y-%m-%d %H:%M:%S"
        ) if status in {"paid", "refunded"} else ""
        orders.append(
            {
                "order_id": f"O{index:06d}",
                "user_id": user_id,
                "product_id": product_id,
                "quantity": str(quantity),
                "unit_price": f"{unit_price:.2f}",
                "amount": f"{amount:.2f}",
                "order_status": status,
                "order_time": order_time.strftime("%Y-%m-%d %H:%M:%S"),
                "pay_time": pay_time,
                "channel": rnd.choice(CHANNELS),
                "coupon_amount": f"{rnd.choice([0, 0, 0, 5, 10, 20]):.2f}",
                "province": rnd.choice(PROVINCES),
            }
        )

    indices = rnd.sample(range(N_ORDERS), sum(count for _, count in DIRTY_GROUPS))
    cursor = 0
    defects: dict[str, dict[str, Any]] = {}
    dirty_ids: list[str] = []
    for defect, count in DIRTY_GROUPS:
        selected = indices[cursor : cursor + count]
        cursor += count
        keys = [orders[index]["order_id"] for index in selected]
        defects[defect] = {"count": count, "keys": keys}
        dirty_ids.extend(keys)
        for index in selected:
            row = orders[index]
            if defect == "null_user_id":
                row["user_id"] = ""
            elif defect == "null_product_id":
                row["product_id"] = ""
            elif defect == "invalid_order_status":
                row["order_status"] = "unknown_status"
            elif defect == "negative_amount":
                row["amount"] = f"-{abs(float(row['amount'])):.2f}"
            elif defect == "amount_mismatch":
                row["amount"] = f"{float(row['amount']) + 5.0:.2f}"
            elif defect == "orphan_user_id":
                row["user_id"] = "U9999"
            elif defect == "orphan_product_id":
                row["product_id"] = "P9999"
            elif defect == "invalid_order_time":
                row["order_time"] = "2026-13-40 99:99:99"
                row["pay_time"] = ""
            elif defect == "future_order_time":
                row["order_time"] = "2099-01-01 00:00:00"
                row["pay_time"] = ""
            elif defect == "duplicate_order_id":
                pass
            else:
                raise AssertionError(defect)

    duplicate_ids = set(defects["duplicate_order_id"]["keys"])
    duplicates = [dict(row) for row in orders if row["order_id"] in duplicate_ids]
    orders.extend(duplicates)

    if len(set(dirty_ids)) != 34:
        raise AssertionError(f"expected 34 distinct dirty orders, got {len(set(dirty_ids))}")
    if len(duplicates) != 2:
        raise AssertionError("expected exactly two duplicated source rows")

    manifest = {
        "seed": SEED,
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "n_users": N_USERS,
        "n_products": N_PRODUCTS,
        "n_base_orders": N_ORDERS,
        "source_order_rows": len(orders),
        "dirty_order_count": len(set(dirty_ids)),
        "dirty_order_ids": sorted(set(dirty_ids)),
        "duplicate_source_rows": len(duplicates),
        "defects": defects,
        "dimension_defect": {
            "product_id": "P0007",
            "source_category": "数码",
            "dim_category": "家电",
            "count": 1,
        },
    }
    return orders, manifest


def generate(reset: bool = True) -> dict[str, Any]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if reset:
        for path in RAW_DIR.glob("*.csv"):
            path.unlink()

    rnd = random.Random(SEED)
    users = _build_users(rnd)
    products = _build_products(rnd)
    orders, manifest = _build_orders(rnd, users, products)

    _write_csv(
        RAW_DIR / "ods_users.csv",
        users,
        ["user_id", "user_name", "city", "register_time", "status"],
    )
    _write_csv(
        RAW_DIR / "ods_products.csv",
        products,
        ["product_id", "product_name", "category", "list_price", "status"],
    )
    _write_csv(
        RAW_DIR / "ods_orders.csv",
        orders,
        [
            "order_id",
            "user_id",
            "product_id",
            "quantity",
            "unit_price",
            "amount",
            "order_status",
            "order_time",
            "pay_time",
            "channel",
            "coupon_amount",
            "province",
        ],
    )
    (RAW_DIR / "dirty_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        f"generated: users={len(users)}, products={len(products)}, "
        f"order_rows={len(orders)}, dirty_orders={manifest['dirty_order_count']}"
    )
    return manifest


if __name__ == "__main__":
    generate()
