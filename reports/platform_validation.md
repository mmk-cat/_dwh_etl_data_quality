# 多引擎验收记录

本项目不止在 SQLite 上跑通：24 条规则分别用 **SQLite、Spark SQL、MySQL/MariaDB** 执行过，命中结果一致。

## 1. 三引擎结果对比

| 引擎 | 版本 | 订单行 | 脏订单 | 维度错位 | 规则命中 | 结果文件 |
|---|---|---:|---:|---:|---:|---|
| SQLite | Python 3.12 内置 | 2,002 | 34 | 1 | 43 | `reports/dq/dq_summary.json` |
| Spark SQL | PySpark 4.0.1 / Java 21 | 2,002 | 34 | 1 | 43 | `reports/dq_spark.json` |
| MySQL/MariaDB | MariaDB 11.4.13 + PyMySQL 1.2.3 | 2,002 | 34 | 1 | 43 | `reports/mysql_dq.json` |

三者的 13 条触发规则及每条命中数完全一致：

```
USER_NOT_NULL=6, PRODUCT_NOT_NULL=4, ORDER_ID_UNIQUE=2,
STATUS_ENUM=4, AMOUNT_POSITIVE=3, AMOUNT_CONSISTENT=8,
USER_EXISTS=4, PRODUCT_EXISTS=3, TIME_FORMAT=2, TIME_NOT_FUTURE=1,
DIM_CATEGORY_MATCH=1, DWD_AMOUNT_NONNEGATIVE=3, DOD_THRESHOLD=2
```

## 2. Spark SQL 验证

环境：Windows 11、Java 21、PySpark 4.0.1、Hadoop 3.3.6 winutils。

```powershell
$env:JAVA_HOME='C:\Program Files\Microsoft\jdk-21.0.12.101-hotspot'
$env:HADOOP_HOME='D:\dsh_tmp\hadoop'
python -m src.etl.spark_job --input data/raw --warehouse data/warehouse --output data/warehouse_spark
python -m src.dq.spark_runner --raw data/raw --warehouse data/warehouse --output reports/dq_spark.json
```

实测：

- Spark DWD = 2,002 行，DWS = 79 行，GMV = **5,129,141.12**，与 Pandas 参考实现一致；
- 非法日期 `2026-13-40` 被 Spark SQL 正确归入 `INVALID` 分区（2 行），未来日期 1 行；
- 24 条规则在 Spark SQL 上总命中 **43** 行，与 SQLite/MySQL 完全一致。

> Windows 上 PySpark 需要 `HADOOP_HOME` 下的 `winutils.exe` / `hadoop.dll`；Linux 环境通常无需额外安装。若没有 Spark，本项目仍可用 Pandas + SQLite 完整运行。

## 3. MySQL / MariaDB 验证

本机无 Docker，使用便携版 MariaDB 11.4.13（MySQL 协议）验证；正式部署可用 Docker/MySQL 8.4。

```powershell
python -m src.dq.mysql_engine --host 127.0.0.1 --port 3307 `
  --user root --password root_pwd --database dwh_dq
```

实测：

- 建表 8 张（ODS/DIM/DWD/DWS），编码 `utf8mb4`；
- 载入 ODS 2,002 行、DWD 2,002 行、DWS 日粒度 79 行、渠道 44 行；
- 24 条规则执行成功，脏订单 34、维度错位 1、命中明细 43，`passed=true`；
- DDL 见 `sql/mysql/schema.sql`，执行器见 `src/dq/mysql_engine.py`。

## 4. Kafka 验证

默认测试使用文件总线模拟 at-least-once；真实 Kafka 代码与 compose 文件已提供。本机未启动 Kafka broker，因此不宣称真实集群实测；文件总线的结果如下：

- 生产 1,000 条唯一事件；
- 原始消费 1,001 条（1 条重复）；
- 按 `event_id` 幂等去重后 1,000 条；
- 丢失 0、额外 0、checksum 不一致 0、顺序保持、lag=0。

如启动 `docker/docker-compose.kafka.yml`，可执行：

```powershell
python -m src.kafka.real_kafka --bootstrap 127.0.0.1:9092 --topic ods-order-events
```
