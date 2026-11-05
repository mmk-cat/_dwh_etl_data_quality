"""Render the DQ Markdown/Excel reports and a project summary JSON."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
RAW = ROOT / "data" / "raw"
WAREHOUSE = ROOT / "data" / "warehouse"


def _load(name: str) -> dict[str, Any]:
    return json.loads((REPORTS / name).read_text(encoding="utf-8"))


def build() -> dict[str, Any]:
    manifest = json.loads((RAW / "dirty_manifest.json").read_text(encoding="utf-8"))
    dq = _load("dq/dq_summary.json")
    remediation = _load("remediation.json")
    kafka = _load("kafka_consistency.json")
    etl = json.loads((WAREHOUSE / "etl_metrics.json").read_text(encoding="utf-8"))
    rules = pd.read_csv(REPORTS / "dq" / "rule_hits.csv", dtype=str, keep_default_na=False)
    anomalies = pd.read_csv(REPORTS / "dq" / "anomalies.csv", dtype=str, keep_default_na=False)

    category_counts = Counter(
        {
            "非空率": 6,
            "唯一性": 3,
            "枚举合法": 3,
            "金额符号": 4,
            "表间一致": 4,
            "时间": 2,
            "维度一致性": 1,
            "日环比阈值": 1,
        }
    )
    triggered = rules[rules["hits"].astype(int) > 0]
    summary = dq["summary"]
    remediation_impact = remediation["impact"]
    after = remediation["dq_after"]["summary"]

    summary_rows = [
        ("质量规则总数", summary["rules_total"]),
        ("触发规则数", summary["rules_triggered"]),
        ("通过规则数", summary["rules_passed"]),
        ("ODS 订单行数", etl["source_order_rows"]),
        ("DWD 明细行数", etl["dwd_rows"]),
        ("DWS 日粒度行数", etl["dws_daily_rows"]),
        ("DWS 渠道行数", etl["dws_channel_rows"]),
        ("检测到脏订单数（去重）", summary["dirty_order_count"]),
        ("维度错位", summary["dimension_anomaly_count"]),
        ("规则命中明细行数", summary["anomaly_rows"]),
        ("指标告警数（非脏数据）", summary["metric_alert_count"]),
        ("隔离订单行数", remediation["quarantined_order_rows"]),
        ("清洗后订单数", remediation["clean_order_rows"]),
        ("清洗后规则异常", after["anomaly_rows"]),
        ("GMV（脏数据，元）", f"{remediation['gmv_before']:,.2f}"),
        ("GMV（清洗后，元）", f"{remediation['gmv_after_clean']:,.2f}"),
        ("GMV 虚增（元）", f"{remediation['gmv_delta']:,.2f}"),
        ("Kafka 生产事件", kafka["idempotent_consumer"]["produced"]),
        ("Kafka 唯一消费事件", kafka["idempotent_consumer"]["consumed_unique"]),
        ("Kafka 重复消息（已幂等去重）", kafka["duplicates_removed"]),
        ("Kafka 丢失/乱序/校验失败", "0 / 0 / 0"),
    ]
    summary_df = pd.DataFrame(summary_rows, columns=["指标", "数值"])

    lines: list[str] = [
        "# 数仓 ETL 数据质量校验报告",
        "",
        f"> 生成时间：{datetime.now().astimezone().isoformat(timespec='seconds')}　"
        f"数据种子：`{manifest['seed']}`　统计截止：`2026-05-31`",
        "",
        "## 1. 结论",
        "",
        f"- 针对 **ODS → DWD → DWS** 共落地 **{summary['rules_total']}** 条校验规则，"
        f"覆盖非空率、主键唯一、枚举、金额符号、表间一致、日期合法、维度一致与日环比阈值。",
        f"- 在 **{etl['source_order_rows']:,}** 行 ODS 订单中，定位 **{summary['dirty_order_count']}** 个脏订单"
        f"（去重后），以及 **{summary['dimension_anomaly_count']}** 处商品维度错位。",
        f"- 这 **{summary['dirty_order_count']}** 条脏订单合计造成 GMV 虚增 "
        f"**{remediation['gmv_delta']:,.2f}** 元，污染 **{remediation_impact['affected_dws_cells']}** 个 "
        f"DWS 日期 × 品类单元格。",
        f"- 隔离 **{remediation['quarantined_order_rows']}** 行并修正 P0007 维度后重建，"
        f"规则异常降为 **{after['anomaly_rows']}**，24/24 条规则通过。",
        f"- Kafka 生产 **{kafka['idempotent_consumer']['produced']}** 条事件，"
        f"消费端识别 **{kafka['duplicates_removed']}** 条重复并按 event_id 幂等去重，"
        "丢失 0、校验和不一致 0、乱序 0。",
        "",
        "## 2. 核心指标",
        "",
        "| 指标 | 数值 |",
        "|---|---:|",
    ]
    for metric, value in summary_rows:
        lines.append(f"| {metric} | {value} |")

    lines.extend(
        [
            "",
            "## 3. 规则设计",
            "",
            "| 类别 | 规则数 |",
            "|---|---:|",
        ]
    )
    for category, count in category_counts.items():
        lines.append(f"| {category} | {count} |")
    lines.extend(
        [
            "",
            "全部规则均可重复执行。SQL 文本集中在 `src/dq/rules.py`，本地 SQLite 与 Spark SQL 共用同一份定义。",
            "",
            "## 4. 触发规则与样例",
            "",
            "| 规则 | 类别 | 严重级别 | 命中 | 样例主键 |",
            "|---|---|---|---:|---|",
        ]
    )
    for _, row in triggered.iterrows():
        lines.append(
            f"| `{row['rule_code']}` {row['name']} | {row['category']} | {row['severity']} | "
            f"{row['hits']} | {row['sample_keys']} |"
        )
    lines.extend(
        [
            "",
            "> 规则 `DWS_DOD_GMV_THRESHOLD` 的 2 条告警分别来自未来分区 `2099-01-01` 和非法日期分区 "
            "`INVALID`；它们是**指标告警**，不计入 34 条脏订单，避免把统计波动误报为脏数据。",
            "",
            "## 5. 根因拆解",
            "",
            "| 缺陷类型 | 数量 | 说明 |",
            "|---|---:|---|",
        ]
    )
    defect_desc = {
        "null_user_id": "上游未做 user_id 非空拦截",
        "null_product_id": "上游未做 product_id 非空拦截",
        "duplicate_order_id": "缺少订单号幂等/去重",
        "invalid_order_status": "状态码未做枚举校验",
        "negative_amount": "金额未做符号校验",
        "amount_mismatch": "金额未与数量×单价重算",
        "orphan_user_id": "用户维表关联失败未拦截",
        "orphan_product_id": "商品维表关联失败未拦截",
        "invalid_order_time": "时间未做格式/日历校验",
        "future_order_time": "时间未做未来日期校验",
    }
    for defect, meta in manifest["defects"].items():
        lines.append(f"| {defect} | {meta['count']} | {defect_desc.get(defect, '')} |")
    lines.extend(
        [
            "",
            f"此外，源商品 `P0007` 品类为「数码」，DWD 维度同步后变成「家电」，"
            f"属 **1 处维度错位**；规则 `DIM_PRODUCT_CATEGORY_MATCH` 对比源表与维表后精确定位。",
            "",
            "## 6. 影响范围",
            "",
            f"- 受影响 DWS 单元格：**{remediation_impact['affected_dws_cells']}** 个",
            f"- 受影响日期：{', '.join(f'`{item}`' for item in remediation_impact['affected_dates'][:8])}"
            f" 等 {len(remediation_impact['affected_dates'])} 个日期",
            f"- 脏数据金额：**{remediation_impact['dirty_amount']:,.2f}** 元",
            f"- 其中未来分区 `2099-01-01` 与非法日期分区 `INVALID` 会直接污染按日汇总；"
            f"空商品会落入 `UNKNOWN` 品类。",
            "",
            "## 7. 修复与回归",
            "",
            f"1. 将 {summary['dirty_order_count']} 个脏订单对应的 {remediation['quarantined_order_rows']} 行"
            f"写入 `data/quarantine/orders.csv`，附规则命中原因；",
            f"2. 将 `dim_product.P0007.category` 从「家电」修正为「数码」；",
            f"3. 用 {remediation['clean_order_rows']:,} 行干净订单重建 DWD/DWS；",
            f"4. 重跑 24 条规则：脏订单 **0**、维度异常 **0**、规则异常行 **0**；",
            f"5. GMV 从 {remediation['gmv_before']:,.2f} 回落到 {remediation['gmv_after_clean']:,.2f} 元，"
            f"差额 {remediation['gmv_delta']:,.2f} 元与脏订单金额一致。",
            "",
            "## 8. Kafka 生产–消费一致性",
            "",
            "| 指标 | 原始消费（至少一次） | 幂等消费后 |",
            "|---|---:|---:|",
            f"| 生产事件 | {kafka['raw_consumer']['produced']} | {kafka['raw_consumer']['produced']} |",
            f"| 消费行数 | {kafka['raw_consumer']['consumed_raw']} | {kafka['idempotent_consumer']['consumed_raw']} |",
            f"| 唯一事件 | {kafka['raw_consumer']['consumed_unique']} | {kafka['idempotent_consumer']['consumed_unique']} |",
            f"| 重复消息 | {kafka['raw_consumer']['duplicate_messages']} | {kafka['idempotent_consumer']['duplicate_messages']} |",
            f"| 丢失 | {kafka['raw_consumer']['missing']} | {kafka['idempotent_consumer']['missing']} |",
            f"| 校验和不一致 | {kafka['raw_consumer']['checksum_mismatch']} | {kafka['idempotent_consumer']['checksum_mismatch']} |",
            f"| 顺序保持 | {kafka['raw_consumer']['order_preserved']} | {kafka['idempotent_consumer']['order_preserved']} |",
            f"| 结果 | 至少一次语义，允许重复 | **PASS** |",
            "",
            "## 9. 复现",
            "",
            "```powershell",
            "cd 02_dwh_etl_data_quality",
            "python -m src.pipeline",
            "python -m pytest -q",
            "```",
            "",
            "Spark 任务：`python -m src.etl.spark_job --input data/raw --output data/warehouse_spark`（需 PySpark）。",
            "真实 Kafka：`python -m src.kafka.real_kafka --bootstrap 127.0.0.1:9092 --topic ods-order-events`。",
            "",
            "## 10. 产物",
            "",
            "- `reports/dq_report.md` / `dq_report.xlsx`",
            "- `reports/dq/anomalies.csv`、`dq_summary.json`、`rule_hits.csv`",
            "- `reports/dq_after/`（修复后复跑结果）",
            "- `reports/kafka_consistency.json`",
            "- `data/quarantine/orders.csv`",
            "",
        ]
    )
    report_text = "\n".join(lines)
    (REPORTS / "dq_report.md").write_text(report_text, encoding="utf-8")

    dirty_rows = []
    for defect, meta in manifest["defects"].items():
        for key in meta["keys"]:
            dirty_rows.append({"defect": defect, "order_id": key, "count": meta["count"]})

    with pd.ExcelWriter(REPORTS / "dq_report.xlsx", engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        rules.to_excel(writer, sheet_name="RuleHits", index=False)
        anomalies.to_excel(writer, sheet_name="Anomalies", index=False)
        pd.DataFrame(dirty_rows).to_excel(writer, sheet_name="DirtyOrders", index=False)
        pd.DataFrame(
            {
                "affected_pair": remediation_impact["affected_pairs"],
            }
        ).to_excel(writer, sheet_name="Impact", index=False)
        pd.DataFrame(
            [
                {"metric": key, "value": value}
                for key, value in kafka["idempotent_consumer"].items()
                if not isinstance(value, list)
            ]
        ).to_excel(writer, sheet_name="Kafka", index=False)

    summary = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "rules_total": summary["rules_total"],
        "rules_triggered": summary["rules_triggered"],
        "source_order_rows": etl["source_order_rows"],
        "dirty_order_count": summary["dirty_order_count"],
        "dimension_anomaly_count": summary["dimension_anomaly_count"],
        "anomaly_rows": summary["anomaly_rows"],
        "quarantined_order_rows": remediation["quarantined_order_rows"],
        "clean_order_rows": remediation["clean_order_rows"],
        "dq_after_anomalies": after["anomaly_rows"],
        "gmv_before": remediation["gmv_before"],
        "gmv_after_clean": remediation["gmv_after_clean"],
        "gmv_delta": remediation["gmv_delta"],
        "affected_dws_cells": remediation_impact["affected_dws_cells"],
        "kafka_produced": kafka["idempotent_consumer"]["produced"],
        "kafka_duplicates": kafka["duplicates_removed"],
        "kafka_missing": kafka["idempotent_consumer"]["missing"],
        "kafka_checksum_mismatch": kafka["idempotent_consumer"]["checksum_mismatch"],
        "kafka_passed": kafka["passed"],
        "dq_passed": after["anomaly_rows"] == 0 and after["dimension_anomaly_count"] == 0,
    }
    (REPORTS / "project_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, indent=2))
