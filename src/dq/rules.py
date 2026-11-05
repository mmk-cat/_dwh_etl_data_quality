"""24 data-quality rules expressed as portable ANSI SQL.

The same SQL is executed by SQLite in the local runner and can be registered as
Spark temporary views by ``src.dq.spark_runner``.  Keeping the rule text in one
place prevents the classic "test checks a different definition than production"
drift.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Rule:
    code: str
    name: str
    category: str
    scope: str  # orders | dimension | metrics
    table: str
    severity: str
    sql: str
    description: str
    expected_hits: int | None = None


RULES: tuple[Rule, ...] = (
    Rule(
        "ODS_ORDER_USER_NOT_NULL",
        "订单 user_id 非空",
        "非空率",
        "orders",
        "ods_orders",
        "high",
        """
        SELECT order_id AS record_key, 'user_id is null/blank' AS detail
        FROM ods_orders
        WHERE user_id IS NULL OR TRIM(user_id) = ''
        """,
        "用户主键是订单归属和用户维度关联的基础，缺失会导致无法归因。",
        6,
    ),
    Rule(
        "ODS_ORDER_PRODUCT_NOT_NULL",
        "订单 product_id 非空",
        "非空率",
        "orders",
        "ods_orders",
        "high",
        """
        SELECT order_id AS record_key, 'product_id is null/blank' AS detail
        FROM ods_orders
        WHERE product_id IS NULL OR TRIM(product_id) = ''
        """,
        "商品主键缺失会导致品类维度和 DWS 分类汇总失真。",
        4,
    ),
    Rule(
        "ODS_ORDER_ID_NOT_NULL",
        "订单主键非空",
        "非空率",
        "orders",
        "ods_orders",
        "critical",
        """
        SELECT order_id AS record_key, 'order_id is null/blank' AS detail
        FROM ods_orders
        WHERE order_id IS NULL OR TRIM(order_id) = ''
        """,
        "订单主键必须存在，否则无法去重或对账。",
        0,
    ),
    Rule(
        "ODS_ORDER_QTY_NOT_NULL",
        "订单数量非空",
        "非空率",
        "orders",
        "ods_orders",
        "high",
        """
        SELECT order_id AS record_key, 'quantity is null/blank' AS detail
        FROM ods_orders
        WHERE quantity IS NULL OR TRIM(quantity) = ''
        """,
        "数量是金额一致性和销量指标的基础。",
        0,
    ),
    Rule(
        "ODS_ORDER_AMOUNT_NOT_NULL",
        "订单金额非空",
        "非空率",
        "orders",
        "ods_orders",
        "critical",
        """
        SELECT order_id AS record_key, 'amount is null/blank' AS detail
        FROM ods_orders
        WHERE amount IS NULL OR TRIM(amount) = ''
        """,
        "金额为空会直接导致 GMV 缺口。",
        0,
    ),
    Rule(
        "ODS_ORDER_TIME_NOT_NULL",
        "订单时间非空",
        "非空率",
        "orders",
        "ods_orders",
        "high",
        """
        SELECT order_id AS record_key, 'order_time is null/blank' AS detail
        FROM ods_orders
        WHERE order_time IS NULL OR TRIM(order_time) = ''
        """,
        "时间缺失会导致日期分区和日环比失效。",
        0,
    ),
    Rule(
        "ODS_ORDER_ID_UNIQUE",
        "订单主键唯一",
        "唯一性",
        "orders",
        "ods_orders",
        "critical",
        """
        SELECT order_id AS record_key, 'duplicate order_id' AS detail
        FROM ods_orders
        GROUP BY order_id
        HAVING COUNT(*) > 1
        """,
        "重复订单会重复计入 GMV。",
        2,
    ),
    Rule(
        "ODS_USER_ID_UNIQUE",
        "用户主键唯一",
        "唯一性",
        "dimension",
        "ods_users",
        "high",
        """
        SELECT user_id AS record_key, 'duplicate user_id' AS detail
        FROM ods_users
        GROUP BY user_id
        HAVING COUNT(*) > 1
        """,
        "用户维度必须一对一。",
        0,
    ),
    Rule(
        "ODS_PRODUCT_ID_UNIQUE",
        "商品主键唯一",
        "唯一性",
        "dimension",
        "ods_products",
        "high",
        """
        SELECT product_id AS record_key, 'duplicate product_id' AS detail
        FROM ods_products
        GROUP BY product_id
        HAVING COUNT(*) > 1
        """,
        "商品维度必须一对一。",
        0,
    ),
    Rule(
        "ODS_ORDER_STATUS_ENUM",
        "订单状态在枚举范围内",
        "枚举合法",
        "orders",
        "ods_orders",
        "high",
        """
        SELECT order_id AS record_key, 'invalid order_status' AS detail, order_status AS source_value
        FROM ods_orders
        WHERE order_status IS NULL OR TRIM(order_status) = ''
           OR order_status NOT IN ('paid', 'refunded', 'cancelled', 'pending')
        """,
        "未知状态会污染退款率和有效订单口径。",
        4,
    ),
    Rule(
        "ODS_CHANNEL_ENUM",
        "渠道在枚举范围内",
        "枚举合法",
        "orders",
        "ods_orders",
        "medium",
        """
        SELECT order_id AS record_key, 'invalid channel' AS detail, channel AS source_value
        FROM ods_orders
        WHERE channel IS NULL OR TRIM(channel) = ''
           OR channel NOT IN ('app', 'web', 'mini_program')
        """,
        "渠道值必须与埋点/投放口径对齐。",
        0,
    ),
    Rule(
        "ODS_USER_STATUS_ENUM",
        "用户状态合法",
        "枚举合法",
        "dimension",
        "ods_users",
        "medium",
        """
        SELECT user_id AS record_key, 'invalid user status' AS detail, status AS source_value
        FROM ods_users
        WHERE status NOT IN ('active', 'inactive', 'blocked')
        """,
        "用户状态参与黑名单和活跃用户口径。",
        0,
    ),
    Rule(
        "ODS_ORDER_AMOUNT_POSITIVE",
        "订单金额为正",
        "金额符号",
        "orders",
        "ods_orders",
        "critical",
        """
        SELECT order_id AS record_key, 'amount <= 0' AS detail
        FROM ods_orders
        WHERE amount IS NULL OR TRIM(amount) = ''
           OR CAST(amount AS DECIMAL(18,2)) <= 0
        """,
        "负金额会造成 GMV 与退款口径反向。",
        3,
    ),
    Rule(
        "ODS_ORDER_QTY_POSITIVE",
        "订单数量为正",
        "金额符号",
        "orders",
        "ods_orders",
        "high",
        """
        SELECT order_id AS record_key, 'quantity <= 0' AS detail
        FROM ods_orders
        WHERE quantity IS NULL OR TRIM(quantity) = ''
           OR CAST(quantity AS DECIMAL(18,0)) <= 0
        """,
        "零/负数量会产生无意义的销量。",
        0,
    ),
    Rule(
        "ODS_ORDER_AMOUNT_CONSISTENT",
        "金额 = 数量 × 单价",
        "表间一致",
        "orders",
        "ods_orders",
        "critical",
        """
        SELECT order_id AS record_key, 'amount != quantity * unit_price' AS detail
        FROM ods_orders
        WHERE amount IS NULL OR quantity IS NULL OR unit_price IS NULL
           OR ABS(CAST(amount AS DECIMAL(18,2))
                  - ROUND(CAST(quantity AS DECIMAL(18,2)) * CAST(unit_price AS DECIMAL(18,2)), 2)) > 0.01
        """,
        "订单金额与明细计算不一致会破坏财务对账。",
        8,
    ),
    Rule(
        "ODS_ORDER_PRICE_RANGE",
        "成交单价在合理区间",
        "金额符号",
        "orders",
        "ods_orders",
        "medium",
        """
        SELECT o.order_id AS record_key, 'unit_price out of range' AS detail
        FROM ods_orders o
        JOIN ods_products p ON p.product_id = o.product_id
        WHERE CAST(o.unit_price AS DECIMAL(18,2)) <= 0
           OR CAST(o.unit_price AS DECIMAL(18,2)) > CAST(p.list_price AS DECIMAL(18,2)) * 1.2
        """,
        "成交价显著高于标价通常意味着价表或采集异常。",
        0,
    ),
    Rule(
        "ODS_ORDER_USER_EXISTS",
        "订单用户存在于用户维表",
        "表间一致",
        "orders",
        "ods_orders",
        "critical",
        """
        SELECT o.order_id AS record_key, 'user_id not found' AS detail
        FROM ods_orders o
        WHERE o.user_id IS NOT NULL AND TRIM(o.user_id) <> ''
          AND NOT EXISTS (SELECT 1 FROM ods_users u WHERE u.user_id = o.user_id)
        """,
        "孤儿用户会造成用户维度丢失。",
        4,
    ),
    Rule(
        "ODS_ORDER_PRODUCT_EXISTS",
        "订单商品存在于商品维表",
        "表间一致",
        "orders",
        "ods_orders",
        "critical",
        """
        SELECT o.order_id AS record_key, 'product_id not found' AS detail
        FROM ods_orders o
        WHERE o.product_id IS NOT NULL AND TRIM(o.product_id) <> ''
          AND NOT EXISTS (SELECT 1 FROM ods_products p WHERE p.product_id = o.product_id)
        """,
        "孤儿商品会造成品类维度错位或 UNKNOWN。",
        3,
    ),
    Rule(
        "ODS_ORDER_TIME_FORMAT",
        "订单时间格式合法",
        "时间",
        "orders",
        "ods_orders",
        "high",
        """
        SELECT order_id AS record_key, 'invalid order_time format/calendar' AS detail
        FROM ods_orders
        WHERE order_time IS NOT NULL AND TRIM(order_time) <> ''
          AND (
              LENGTH(order_time) <> 19
              OR CAST(SUBSTR(order_time, 6, 2) AS DECIMAL(10,0)) NOT BETWEEN 1 AND 12
              OR CAST(SUBSTR(order_time, 9, 2) AS DECIMAL(10,0)) NOT BETWEEN 1 AND 31
              OR CAST(SUBSTR(order_time, 12, 2) AS DECIMAL(10,0)) NOT BETWEEN 0 AND 23
              OR CAST(SUBSTR(order_time, 15, 2) AS DECIMAL(10,0)) NOT BETWEEN 0 AND 59
          )
        """,
        "非法日期会进入 INVALID 分区或丢失。",
        2,
    ),
    Rule(
        "ODS_ORDER_TIME_NOT_FUTURE",
        "订单时间不晚于统计截止日",
        "时间",
        "orders",
        "ods_orders",
        "high",
        """
        SELECT order_id AS record_key, 'future order_time' AS detail
        FROM ods_orders
        WHERE order_time > '2026-05-31 23:59:59'
          AND SUBSTR(order_time, 6, 2) BETWEEN '01' AND '12'
          AND SUBSTR(order_time, 9, 2) BETWEEN '01' AND '31'
        """,
        "未来订单会污染未来分区和当日口径。",
        1,
    ),
    Rule(
        "DIM_PRODUCT_CATEGORY_MATCH",
        "商品维度品类与源表一致",
        "维度一致性",
        "dimension",
        "dim_product",
        "critical",
        """
        SELECT p.product_id AS record_key,
               'category mismatch' AS detail,
               p.category AS source_value,
               d.category AS dim_value
        FROM ods_products p
        JOIN dim_product d ON d.product_id = p.product_id
        WHERE p.category <> d.category
        """,
        "源商品 P0007 的品类在维度同步时被错误映射。",
        1,
    ),
    Rule(
        "DWD_ORDER_AMOUNT_NONNEGATIVE",
        "DWD 金额非负",
        "金额符号",
        "orders",
        "dwd_order_detail",
        "critical",
        """
        SELECT order_id AS record_key, 'dwd amount < 0' AS detail
        FROM dwd_order_detail
        WHERE amount IS NULL OR CAST(amount AS DECIMAL(18,2)) < 0
        """,
        "ODS 负金额若未被清洗会穿透到明细层。",
        3,
    ),
    Rule(
        "DWS_DAILY_GMV_RECONCILE",
        "DWS 日 GMV 与 DWD 可对账",
        "表间一致",
        "metrics",
        "dws_daily_sales",
        "critical",
        """
        WITH expected AS (
            SELECT order_date, category,
                   ROUND(SUM(CAST(amount AS DECIMAL(18,2))), 2) AS expected_gmv
            FROM dwd_order_detail
            GROUP BY order_date, category
        )
        SELECT d.order_date AS record_key,
               d.category AS detail,
               d.gmv AS source_value,
               e.expected_gmv AS dim_value
        FROM dws_daily_sales d
        LEFT JOIN expected e
          ON e.order_date = d.order_date AND e.category = d.category
        WHERE e.expected_gmv IS NULL
           OR ABS(CAST(d.gmv AS DECIMAL(18,2)) - e.expected_gmv) > 0.01
        """,
        "DWS 汇总必须能从 DWD 重算。",
        0,
    ),
    Rule(
        "DWS_DOD_GMV_THRESHOLD",
        "日 GMV 环比波动阈值",
        "日环比阈值",
        "metrics",
        "dws_daily_sales",
        "medium",
        """
        WITH daily AS (
            SELECT order_date, ROUND(SUM(CAST(gmv AS DECIMAL(18,2))), 2) AS gmv
            FROM dws_daily_sales
            GROUP BY order_date
        ), lagged AS (
            SELECT order_date, gmv,
                   LAG(gmv) OVER (ORDER BY order_date) AS prev_gmv
            FROM daily
        )
        SELECT order_date AS record_key,
               'day-over-day change exceeds 50%' AS detail,
               ROUND((gmv - prev_gmv) / prev_gmv * 100, 2) AS change_pct
        FROM lagged
        WHERE prev_gmv IS NOT NULL AND prev_gmv <> 0
          AND ABS(gmv - prev_gmv) / ABS(prev_gmv) > 0.5
        """,
        "阈值告警用于发现上游断流/重复灌数，不等同于脏数据。",
        None,
    ),
)

assert len(RULES) == 24
assert len({rule.code for rule in RULES}) == 24

RULES_BY_CODE = {rule.code: rule for rule in RULES}
