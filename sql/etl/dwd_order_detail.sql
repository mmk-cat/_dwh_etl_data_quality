-- Spark SQL: ODS -> DWD order detail.
-- Temp views required: ods_orders, ods_users, dim_product.
CREATE OR REPLACE TEMP VIEW dwd_order_detail AS
SELECT
    o.order_id,
    o.user_id,
    COALESCE(u.user_name, 'UNKNOWN') AS user_name,
    COALESCE(u.city, 'UNKNOWN') AS city,
    o.product_id,
    COALESCE(p.product_name, 'UNKNOWN') AS product_name,
    COALESCE(p.category, 'UNKNOWN') AS category,
    CAST(o.quantity AS INT) AS quantity,
    CAST(o.unit_price AS DECIMAL(18, 2)) AS unit_price,
    CAST(p.list_price AS DECIMAL(18, 2)) AS list_price,
    CAST(o.amount AS DECIMAL(18, 2)) AS amount,
    CAST(o.coupon_amount AS DECIMAL(18, 2)) AS coupon_amount,
    o.order_status,
    o.order_time,
    CASE
        WHEN o.order_time RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
             AND CAST(SUBSTRING(o.order_time, 6, 2) AS INT) BETWEEN 1 AND 12
             AND CAST(SUBSTRING(o.order_time, 9, 2) AS INT) BETWEEN 1 AND 31
             AND CAST(SUBSTRING(o.order_time, 12, 2) AS INT) BETWEEN 0 AND 23
             AND CAST(SUBSTRING(o.order_time, 15, 2) AS INT) BETWEEN 0 AND 59
            THEN SUBSTRING(o.order_time, 1, 10)
        ELSE 'INVALID'
    END AS order_date,
    o.pay_time,
    o.channel,
    o.province,
    COALESCE(u.status, 'UNKNOWN') AS user_status,
    COALESCE(p.status, 'UNKNOWN') AS product_status
FROM ods_orders o
LEFT JOIN ods_users u ON u.user_id = o.user_id
LEFT JOIN dim_product p ON p.product_id = o.product_id;
