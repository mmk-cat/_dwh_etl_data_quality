# 数仓 ETL 数据质量校验（ODS → DWD → DWS）

基于 **Python + Pandas + Spark SQL + MySQL 建模思想 + Kafka + pytest** 的数据质量项目。  
以电商订单为主题，构造可重复的 ODS 数据，落地 DWD 订单明细与 DWS 日销售/渠道汇总，使用 **24 条规则** 发现 **34 条脏订单 + 1 处维度错位**，完成隔离修复、GMV 对账和 Kafka 生产–消费一致性校验。

## 已验收结果

| 指标 | 实测值 |
|---|---:|
| 质量规则 | **24** 条 |
| ODS 订单行数 | **2,002** 行（2,000 基础订单 + 2 条重复） |
| 脏订单（去重） | **34** 条 |
| 维度错位 | **1** 处（商品 P0007：数码 → 家电） |
| 规则命中 | 13 条规则、43 行明细 |
| 隔离行数 | 36 行 |
| 清洗后订单 | 1,966 条；24/24 规则通过 |
| GMV 虚增 | **71,199.64** 元（与脏订单金额精确对账） |
| 受影响 DWS 单元格 | 29 个（日期 × 品类） |
| Kafka | 生产 1,000 / 消费 1,000，1 条重复被幂等去重，丢失 0、乱序 0、校验失败 0 |
| SQLite DQ | 24 条规则 / 43 行命中 / 34 条脏订单 / 1 处维度错位 |
| Spark SQL DQ | 与 SQLite 完全一致（43 行命中，GMV 5,129,141.12） |
| MySQL/MariaDB DQ | 与 SQLite 完全一致（43 行命中，`passed=true`） |
| pytest | **49 passed in 2.19 s** |
| 全链路 pipeline | 0.65 s |

> 报告：`reports/dq_report.md`、`reports/dq_report.xlsx`、`reports/dq/anomalies.csv`、`reports/kafka_consistency.json`、`reports/platform_validation.md`。  
> 分引擎原始结果：`reports/dq/dq_summary.json`、`reports/dq_spark.json`、`reports/mysql_dq.json`。  
> JUnit：`reports/junit.xml`；pytest-html：`reports/pytest-report.html`。

## 快速开始

```powershell
cd "C:\Users\LENOVO\Desktop\项目\02_dwh_etl_data_quality"
python -m pip install -r requirements.txt
python -m src.pipeline
python -m pytest -q
```

Pipeline 会依次执行：生成 ODS → 构建脏 DWD/DWS → 运行 24 条 DQ 规则 → 隔离脏订单/修复维度 → 重建干净数仓 → 复跑 DQ → Kafka 一致性校验 → 生成 Markdown/Excel 报告。

## 数据链路

```
data/raw/ods_orders.csv       ┐
data/raw/ods_users.csv        ├─ Pandas ETL ─► data/warehouse/dwd_order_detail.csv
data/raw/ods_products.csv     ┘                 ├─► dws_daily_sales.csv
                                                 ├─► dws_channel_daily.csv
                                                 └─► dim_user / dim_product

data/warehouse_clean/…         ◄─ 隔离 36 行 + 修复 P0007 后重建
data/kafka/producer.jsonl      ─► consumer_raw.jsonl（1 条重复）─► consumer_deduped.jsonl
```

## 目录

```
02_dwh_etl_data_quality/
├─ src/
│  ├─ data_generator.py       # 固定 seed、34 条脏数据注入、manifest
│  ├─ pipeline.py             # 一键全链路
│  ├─ remediation.py          # 隔离、维度修复、干净数仓重建
│  ├─ report.py               # Markdown + Excel 报告
│  ├─ etl/local_pipeline.py   # Pandas 参考 ETL（测试默认）
│  ├─ etl/spark_job.py        # PySpark + Spark SQL（可选）
│  ├─ dq/rules.py             # 24 条可移植 SQL 规则
│  ├─ dq/engine.py            # SQLite 执行器 + 异常归集
│  ├─ dq/spark_runner.py      # Spark SQL 复跑规则（可选）
│  └─ kafka/                  # 文件总线演示 + 真实 Kafka 客户端
├─ sql/etl/                   # Spark SQL 文件
├─ tests/                     # 49 条 pytest
├─ data/                      # 生成数据（可直接重建）
├─ reports/                   # 实测报告
└─ docker/docker-compose.kafka.yml
```

## 34 条脏数据的构造

`src/data_generator.py` 使用固定随机种子 `20260517`，从 2,000 条基础订单中抽取互不重叠的 34 个订单，注入 10 类缺陷；另有 2 条重复订单行追加在文件末尾。

| 缺陷 | 数量 |
|---|---:|
| user_id 为空 | 6 |
| product_id 为空 | 4 |
| order_id 重复 | 2 |
| 非法订单状态 | 4 |
| 金额为负 | 3 |
| 金额 ≠ 数量 × 单价 | 5 |
| 孤儿 user_id | 4 |
| 孤儿 product_id | 3 |
| 非法日期 | 2 |
| 未来日期 | 1 |
| **脏订单合计（去重）** | **34** |

上述数量由 `data/raw/dirty_manifest.json` 记录，测试直接与 manifest 对账，避免“报告写 34、实际不是 34”。

## 24 条规则

| 维度 | 规则数 | 示例 |
|---|---:|---|
| 非空率 | 6 | user_id、product_id、order_id、quantity、amount、order_time |
| 主键唯一 | 3 | order_id、user_id、product_id |
| 枚举合法 | 3 | order_status、channel、user.status |
| 金额符号/范围 | 4 | amount > 0、quantity > 0、金额恒等式、单价区间 |
| 表间一致 | 4 | 用户/商品存在性、DWS GMV 对账 |
| 时间 | 2 | 格式/日历合法、不能是未来 |
| 维度一致 | 1 | 源商品品类 vs dim_product 品类 |
| 日环比阈值 | 1 | 日 GMV 环比波动 > 50% |

> 规则 SQL 在 `src/dq/rules.py`，本地 SQLite 与 Spark SQL 共用一份定义；`DWS_DOD_GMV_THRESHOLD` 的 2 条告警来自未来分区和 INVALID 分区，属于指标告警，不计入 34 条脏数据。

## 报告内容

`reports/dq_report.md` / `dq_report.xlsx` 包含：

- 24 条规则、13 条触发规则及样例主键；
- 34 条脏订单的类型、数量、根因；
- 29 个受影响 DWS 单元格、GMV 清洗前后差额；
- 36 行隔离明细（`data/quarantine/orders.csv`，附规则命中原因）；
- 清洗后 24/24 规则通过；
- Kafka 生产/消费、重复、丢失、校验和、顺序比表。

## Spark SQL（已实测）

需要 Java 17+ 和 PySpark：

```powershell
python -m pip install -r requirements-optional.txt
python -m src.etl.spark_job --input data/raw --warehouse data/warehouse --output data/warehouse_spark
python -m src.dq.spark_runner --raw data/raw --warehouse data/warehouse --output reports/dq_spark.json
```

Spark 输出为 CSV 目录（coalesce(1)），DWD 2,002 行、DWS 79 行、GMV 5,129,141.12，24 条规则命中 43 行，与本地 Pandas/SQLite 链路完全一致。Windows 上 PySpark 额外需要 `HADOOP_HOME` 下的 `winutils.exe` / `hadoop.dll`；Linux 通常无需该步骤。

## MySQL / MariaDB 验证（已实测）

```powershell
python -m src.dq.mysql_engine --host 127.0.0.1 --port 3307 `
  --user root --password root_pwd --database dwh_dq
```

将 8 张 ODS/DIM/DWD/DWS 表载入 MySQL/MariaDB 后执行同一套 24 条规则。实测 ODS 2,002 行、DWD 2,002 行、DWS 79 行、渠道 44 行，脏订单 34、维度错位 1、命中 43 行，`passed=true`。DDL 在 `sql/mysql/schema.sql`，结果在 `reports/mysql_dq.json`。

## 真实 Kafka（可选）

```powershell
docker compose -f docker/docker-compose.kafka.yml up -d
python -m pip install -r requirements-optional.txt
python -m src.kafka.real_kafka --bootstrap 127.0.0.1:9092 --topic ods-order-events
```

默认测试使用 `LocalKafkaBus` 模拟 at-least-once 语义：重复投递 1 条事件，消费端按 `event_id` 幂等去重，再校验 checksum、缺失、额外、顺序和 lag。真实 Kafka 客户端复用同一套校验函数。

## 简历占位符回填

| 原占位符 | 本项目实测 |
|---|---|
| 校验规则数占位符 | **24** 条 |
| 上游清洗缺失导致的脏数据占位符 | **34** 条脏订单（另有 36 行被隔离） |
| 维度错位 | 商品 **P0007** 源表「数码」→ 维度「家电」 |
| Kafka 一致性 | 1,000 条事件，重复 1 条幂等去重，丢失/乱序/校验失败均为 0 |

## 说明

- 数据为固定 seed 生成的合成数据，不含真实用户信息。
- 项目已在 **SQLite、Spark SQL、MySQL/MariaDB** 三个引擎上验证同一套 24 条规则，三者的 34/1/43 结果一致；MySQL 实测使用 MariaDB 11.4.13（本机无 Docker），DDL 兼容 MySQL 8.4。
- 金额、GMV、订单号均为演示数据，不对应真实业务。
- 详细命令与环境见 `reports/platform_validation.md`、`docs/DESIGN.md`。



