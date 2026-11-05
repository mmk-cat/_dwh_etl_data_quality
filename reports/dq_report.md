# 数仓 ETL 数据质量校验报告

> 生成时间：2026-11-05T22:50:38+08:00　数据种子：`20260517`　统计截止：`2026-05-31`

## 1. 结论

- 针对 **ODS → DWD → DWS** 共落地 **24** 条校验规则，覆盖非空率、主键唯一、枚举、金额符号、表间一致、日期合法、维度一致与日环比阈值。
- 在 **2,002** 行 ODS 订单中，定位 **34** 个脏订单（去重后），以及 **1** 处商品维度错位。
- 这 **34** 条脏订单合计造成 GMV 虚增 **71,199.64** 元，污染 **29** 个 DWS 日期 × 品类单元格。
- 隔离 **36** 行并修正 P0007 维度后重建，规则异常降为 **0**，24/24 条规则通过。
- Kafka 生产 **1000** 条事件，消费端识别 **1** 条重复并按 event_id 幂等去重，丢失 0、校验和不一致 0、乱序 0。

## 2. 核心指标

| 指标 | 数值 |
|---|---:|
| 质量规则总数 | 24 |
| 触发规则数 | 13 |
| 通过规则数 | 11 |
| ODS 订单行数 | 2002 |
| DWD 明细行数 | 2002 |
| DWS 日粒度行数 | 79 |
| DWS 渠道行数 | 44 |
| 检测到脏订单数（去重） | 34 |
| 维度错位 | 1 |
| 规则命中明细行数 | 43 |
| 指标告警数（非脏数据） | 2 |
| 隔离订单行数 | 36 |
| 清洗后订单数 | 1966 |
| 清洗后规则异常 | 0 |
| GMV（脏数据，元） | 5,129,141.12 |
| GMV（清洗后，元） | 5,057,941.48 |
| GMV 虚增（元） | 71,199.64 |
| Kafka 生产事件 | 1000 |
| Kafka 唯一消费事件 | 1000 |
| Kafka 重复消息（已幂等去重） | 1 |
| Kafka 丢失/乱序/校验失败 | 0 / 0 / 0 |

## 3. 规则设计

| 类别 | 规则数 |
|---|---:|
| 非空率 | 6 |
| 唯一性 | 3 |
| 枚举合法 | 3 |
| 金额符号 | 4 |
| 表间一致 | 4 |
| 时间 | 2 |
| 维度一致性 | 1 |
| 日环比阈值 | 1 |

全部规则均可重复执行。SQL 文本集中在 `src/dq/rules.py`，本地 SQLite 与 Spark SQL 共用同一份定义。

## 4. 触发规则与样例

| 规则 | 类别 | 严重级别 | 命中 | 样例主键 |
|---|---|---|---:|---|
| `ODS_ORDER_USER_NOT_NULL` 订单 user_id 非空 | 非空率 | high | 6 | O000074, O000353, O000546, O000559, O000746, O000858 |
| `ODS_ORDER_PRODUCT_NOT_NULL` 订单 product_id 非空 | 非空率 | high | 4 | O001166, O001257, O001327, O001923 |
| `ODS_ORDER_ID_UNIQUE` 订单主键唯一 | 唯一性 | critical | 2 | O000168, O000537 |
| `ODS_ORDER_STATUS_ENUM` 订单状态在枚举范围内 | 枚举合法 | high | 4 | O000128, O000686, O000986, O001941 |
| `ODS_ORDER_AMOUNT_POSITIVE` 订单金额为正 | 金额符号 | critical | 3 | O000321, O000671, O001498 |
| `ODS_ORDER_AMOUNT_CONSISTENT` 金额 = 数量 × 单价 | 表间一致 | critical | 8 | O000163, O000321, O000420, O000486, O000671, O000805, O001468, O001498 |
| `ODS_ORDER_USER_EXISTS` 订单用户存在于用户维表 | 表间一致 | critical | 4 | O000744, O001331, O001487, O001547 |
| `ODS_ORDER_PRODUCT_EXISTS` 订单商品存在于商品维表 | 表间一致 | critical | 3 | O000583, O001728, O001772 |
| `ODS_ORDER_TIME_FORMAT` 订单时间格式合法 | 时间 | high | 2 | O000770, O000841 |
| `ODS_ORDER_TIME_NOT_FUTURE` 订单时间不晚于统计截止日 | 时间 | high | 1 | O001267 |
| `DIM_PRODUCT_CATEGORY_MATCH` 商品维度品类与源表一致 | 维度一致性 | critical | 1 | P0007 |
| `DWD_ORDER_AMOUNT_NONNEGATIVE` DWD 金额非负 | 金额符号 | critical | 3 | O000321, O000671, O001498 |
| `DWS_DOD_GMV_THRESHOLD` 日 GMV 环比波动阈值 | 日环比阈值 | medium | 2 | 2099-01-01, INVALID |

> 规则 `DWS_DOD_GMV_THRESHOLD` 的 2 条告警分别来自未来分区 `2099-01-01` 和非法日期分区 `INVALID`；它们是**指标告警**，不计入 34 条脏订单，避免把统计波动误报为脏数据。

## 5. 根因拆解

| 缺陷类型 | 数量 | 说明 |
|---|---:|---|
| null_user_id | 6 | 上游未做 user_id 非空拦截 |
| null_product_id | 4 | 上游未做 product_id 非空拦截 |
| duplicate_order_id | 2 | 缺少订单号幂等/去重 |
| invalid_order_status | 4 | 状态码未做枚举校验 |
| negative_amount | 3 | 金额未做符号校验 |
| amount_mismatch | 5 | 金额未与数量×单价重算 |
| orphan_user_id | 4 | 用户维表关联失败未拦截 |
| orphan_product_id | 3 | 商品维表关联失败未拦截 |
| invalid_order_time | 2 | 时间未做格式/日历校验 |
| future_order_time | 1 | 时间未做未来日期校验 |

此外，源商品 `P0007` 品类为「数码」，DWD 维度同步后变成「家电」，属 **1 处维度错位**；规则 `DIM_PRODUCT_CATEGORY_MATCH` 对比源表与维表后精确定位。

## 6. 影响范围

- 受影响 DWS 单元格：**29** 个
- 受影响日期：`2026-05-01`, `2026-05-02`, `2026-05-03`, `2026-05-04`, `2026-05-05`, `2026-05-06`, `2026-05-07`, `2026-05-08` 等 16 个日期
- 脏数据金额：**71,199.64** 元
- 其中未来分区 `2099-01-01` 与非法日期分区 `INVALID` 会直接污染按日汇总；空商品会落入 `UNKNOWN` 品类。

## 7. 修复与回归

1. 将 34 个脏订单对应的 36 行写入 `data/quarantine/orders.csv`，附规则命中原因；
2. 将 `dim_product.P0007.category` 从「家电」修正为「数码」；
3. 用 1,966 行干净订单重建 DWD/DWS；
4. 重跑 24 条规则：脏订单 **0**、维度异常 **0**、规则异常行 **0**；
5. GMV 从 5,129,141.12 回落到 5,057,941.48 元，差额 71,199.64 元与脏订单金额一致。

## 8. Kafka 生产–消费一致性

| 指标 | 原始消费（至少一次） | 幂等消费后 |
|---|---:|---:|
| 生产事件 | 1000 | 1000 |
| 消费行数 | 1001 | 1000 |
| 唯一事件 | 1000 | 1000 |
| 重复消息 | 1 | 0 |
| 丢失 | 0 | 0 |
| 校验和不一致 | 0 | 0 |
| 顺序保持 | True | True |
| 结果 | 至少一次语义，允许重复 | **PASS** |

## 9. 复现

```powershell
cd 02_dwh_etl_data_quality
python -m src.pipeline
python -m pytest -q
```

Spark 任务：`python -m src.etl.spark_job --input data/raw --output data/warehouse_spark`（需 PySpark）。
真实 Kafka：`python -m src.kafka.real_kafka --bootstrap 127.0.0.1:9092 --topic ods-order-events`。

## 10. 产物

- `reports/dq_report.md` / `dq_report.xlsx`
- `reports/dq/anomalies.csv`、`dq_summary.json`、`rule_hits.csv`
- `reports/dq_after/`（修复后复跑结果）
- `reports/kafka_consistency.json`
- `data/quarantine/orders.csv`
