"""PySpark ETL: ODS CSV -> DWD -> DWS, driven by SQL files.

Run with:
    python -m src.etl.spark_job --input data/raw --warehouse data/warehouse \
        --output data/warehouse_spark

Requires Java 17+ and ``pip install pyspark``.  The local Pandas pipeline is
kept as the fast, dependency-light reference used by the test suite.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "sql" / "etl"


def _ensure_dimensions(warehouse: Path) -> None:
    dim_product = warehouse / "dim_product.csv"
    dim_user = warehouse / "dim_user.csv"
    if dim_product.exists() and dim_user.exists():
        return
    warehouse.mkdir(parents=True, exist_ok=True)
    users = pd.read_csv(ROOT / "data/raw/ods_users.csv", dtype=str, keep_default_na=False)
    products = pd.read_csv(ROOT / "data/raw/ods_products.csv", dtype=str, keep_default_na=False)
    products.loc[products["product_id"] == "P0007", "category"] = "家电"
    users.to_csv(dim_user, index=False, encoding="utf-8")
    products.to_csv(dim_product, index=False, encoding="utf-8")


def run_spark(input_dir: Path, warehouse_dir: Path, output_dir: Path) -> None:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise SystemExit(
            "PySpark 未安装。执行: pip install pyspark==4.0.1 "
            "（需要 Java 17+）"
        ) from exc

    _ensure_dimensions(warehouse_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("book-dwh-etl")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        # Windows without a Hadoop native DLL: use the Java implementation.
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .getOrCreate()
    )
    try:
        for name, path in (
            ("ods_orders", input_dir / "ods_orders.csv"),
            ("ods_users", input_dir / "ods_users.csv"),
            ("dim_product", warehouse_dir / "dim_product.csv"),
        ):
            frame = (
                spark.read.option("header", True)
                .option("inferSchema", False)
                .option("multiLine", False)
                .csv(str(path))
            )
            frame.createOrReplaceTempView(name)

        dwd_sql = (SQL_DIR / "dwd_order_detail.sql").read_text(encoding="utf-8")
        dws_sql = (SQL_DIR / "dws_daily_sales.sql").read_text(encoding="utf-8")
        spark.sql(dwd_sql)
        spark.sql(dws_sql)

        (
            spark.sql("SELECT * FROM dwd_order_detail")
            .coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(output_dir / "dwd_order_detail"))
        )
        (
            spark.sql("SELECT * FROM dws_daily_sales")
            .coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(output_dir / "dws_daily_sales"))
        )
        print(f"Spark ETL written to {output_dir}")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(ROOT / "data" / "raw"))
    parser.add_argument("--warehouse", default=str(ROOT / "data" / "warehouse"))
    parser.add_argument("--output", default=str(ROOT / "data" / "warehouse_spark"))
    args = parser.parse_args()
    run_spark(Path(args.input), Path(args.warehouse), Path(args.output))


if __name__ == "__main__":
    main()
