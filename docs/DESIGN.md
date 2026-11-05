# 数仓 ETL 与数据质量设计说明

## 1. 目标

在 ODS → DWD → DWS 三层链路中，用可重复执行的规则发现“接口/文件能入仓、但数据不可用”的问题，并量化对下游 GMV、维度、分区的影响。

- 每层都有明确主键/粒度：ODS 订单行、DWD 订单明细、DWS 日期 × 品类 / 日期 × 渠道。
- 规则和 ETL 解耦：规则 SQL 独立于 Pandas/Spark 执行器，SQLite、Spark SQL、MySQL 三引擎共用。
- 所有异常可回溯：规则代码 → 记录主键 → 样例 → 隔离文件 → 修复后复跑。

## 2. 分层与字段

### ODS（贴源层）

| 表 | 粒度 | 关键字段 |
|---|---|---|
| `ods_orders` | 订单行（允许重复，保留脏数据） | order_id, user_id, product_id, quantity, unit_price, amount, order_status, order_time, channel |
| `ods_users` | 用户 | user_id, user_name, city, register_time, status |
| `ods_products` | 商品 | product_id, product_name, category, list_price, status |

ODS 不做任何防御性清洗，保证问题可见。重复订单以多行形式保留。

### DWD（明细层）

`dwd_order_detail` 字段：订单/用户/商品主键、用户名/城市、商品名/品类、数量、单价、标价、金额、状态、时间、`order_date`、渠道、省份、用户/商品状态。

- 空关联保留，维度字段填 `UNKNOWN`；
- 非法时间归入 `INVALID` 分区，**不静默丢弃**；
- 金额/数量转数值，但负金额和金额不一致穿透，等待规则发现；
- `P0007` 的品类从源表「数码」错误同步为「家电」，用于验证维度一致性规则。

### DWS（汇总层）

- `dws_daily_sales`：`order_date × category`，订单数、买家数、GMV、退款额、客单价。
- `dws_channel_daily`：`order_date × channel`，订单数、GMV。
- 脏数据会进入 DWS，因此 34 条脏订单污染 29 个日期 × 品类单元格，未来/非法日期形成异常分区。

## 3. 数据生成与缺陷注入

- 固定随机种子 `20260517`；基础订单 2,000 条，日期 2026-05-01 至 2026-05-14。
- 从 2,000 条中不重复抽取 34 条，按以下 10 类缺陷注入；另有 2 条重复行追加，源文件共 2,002 行。
- `data/raw/dirty_manifest.json` 保存每个缺陷的订单号，测试直接和 manifest 对账。

| 缺陷 | 数量 | 影响的规则 |
|---|---:|---|
| null user_id | 6 | ODS_ORDER_USER_NOT_NULL |
| null product_id | 4 | ODS_ORDER_PRODUCT_NOT_NULL |
| duplicate order_id | 2 | ODS_ORDER_ID_UNIQUE |
| invalid order_status | 4 | ODS_ORDER_STATUS_ENUM |
| negative amount | 3 | AMOUNT_POSITIVE / AMOUNT_CONSISTENT / DWD_NONNEGATIVE |
| amount ≠ qty × price | 5 | ODS_ORDER_AMOUNT_CONSISTENT |
| orphan user_id | 4 | ODS_ORDER_USER_EXISTS |
| orphan product_id | 3 | ODS_ORDER_PRODUCT_EXISTS |
| invalid order_time | 2 | TIME_FORMAT |
| future order_time | 1 | TIME_NOT_FUTURE |
| **去重合计** | **34** | |

维度错位只注入 1 处：`dim_product.P0007.category = 家电`，源表为 `数码`。

## 4. 24 条规则

| # | code | 类别 | 预期命中 |
|---:|---|---|---:|
| 1 | ODS_ORDER_USER_NOT_NULL | 非空率 | 6 |
| 2 | ODS_ORDER_PRODUCT_NOT_NULL | 非空率 | 4 |
| 3 | ODS_ORDER_ID_NOT_NULL | 非空率 | 0 |
| 4 | ODS_ORDER_QTY_NOT_NULL | 非空率 | 0 |
| 5 | ODS_ORDER_AMOUNT_NOT_NULL | 非空率 | 0 |
| 6 | ODS_ORDER_TIME_NOT_NULL | 非空率 | 0 |
| 7 | ODS_ORDER_ID_UNIQUE | 唯一性 | 2 |
| 8 | ODS_USER_ID_UNIQUE | 唯一性 | 0 |
| 9 | ODS_PRODUCT_ID_UNIQUE | 唯一性 | 0 |
| 10 | ODS_ORDER_STATUS_ENUM | 枚举 | 4 |
| 11 | ODS_CHANNEL_ENUM | 枚举 | 0 |
| 12 | ODS_USER_STATUS_ENUM | 枚举 | 0 |
| 13 | ODS_ORDER_AMOUNT_POSITIVE | 金额符号 | 3 |
| 14 | ODS_ORDER_QTY_POSITIVE | 金额符号 | 0 |
| 15 | ODS_ORDER_AMOUNT_CONSISTENT | 表间一致 | 8 |
| 16 | ODS_ORDER_PRICE_RANGE | 金额符号 | 0 |
| 17 | ODS_ORDER_USER_EXISTS | 表间一致 | 4 |
| 18 | ODS_ORDER_PRODUCT_EXISTS | 表间一致 | 3 |
| 19 | ODS_ORDER_TIME_FORMAT | 时间 | 2 |
| 20 | ODS_ORDER_TIME_NOT_FUTURE | 时间 | 1 |
| 21 | DIM_PRODUCT_CATEGORY_MATCH | 维度一致 | 1 |
| 22 | DWD_ORDER_AMOUNT_NONNEGATIVE | 金额符号 | 3 |
| 23 | DWS_DAILY_GMV_RECONCILE | 表间一致 | 0 |
| 24 | DWS_DOD_GMV_THRESHOLD | 日环比阈值 | 2（指标告警） |

规则 23 的预期为 0 是“通过”而不是没有检查：DWS 任意一行与 DWD 重算不一致就会返回记录。规则 24 的 2 条告警来自 `2099-01-01` 和 `INVALID` 分区，不计入 34 条脏订单。

## 5. 异常归集与去重口径

- 规则命中行数 `anomaly_rows = 43`（同一订单可命中多条规则）。
- 脏订单 `dirty_order_count = 34`：按 `order_id` 去重，只统计订单/明细层规则。
- 维度异常 `dimension_anomaly_count = 1`：按维度主键去重。
- 指标告警不计入脏订单，单独列在 `metric_alerts`。

这三个口径在报告中分开，避免把“43 行命中”误写成“43 条脏订单”。

## 6. 修复

1. 按 34 个脏订单号隔离全部对应行（含重复行），实际隔离 36 行，写入 `data/quarantine/orders.csv` 并附原因；
2. 将 `dim_product.P0007.category` 修正为「数码」；
3. 用 1,966 行干净订单重建 `warehouse_clean`；
4. 24 条规则复跑：脏订单 0、维度 0、异常行 0、指标告警 0；
5. GMV 从 5,129,141.12 降至 5,057,941.48，差额 71,199.64 与脏订单金额精确一致。

## 7. Kafka 一致性

- 事件：`event_id`、`sequence`、`order_id`、payload、`payload_checksum`（SHA-256）、`produced_at`。
- 模拟 at-least-once：原始消费 1,001 行，其中 1 条重复。
- 消费端按 `event_id` 幂等去重后 1,000 行。
- 校验：生产/消费集合差、checksum、顺序、lag。
- 真实 Kafka 版本复用同一个 `check_consistency`，只在传输层换成 KafkaProducer/Consumer。

## 8. 测试设计

49 条 pytest：

- 24 条逐规则断言预期命中数（规则契约）；
- 6 条规则目录/分类/根因/指标告警；
- 8 条 ETL、对账、隔离、GMV、确定性测试；
- 7 条 Kafka 一致性/篡改检测；
- 5 条报告产物与关键数字一致性。

执行时 session fixture 先跑完整 pipeline，再从实际产物断言，避免测试与报告各算一套。
