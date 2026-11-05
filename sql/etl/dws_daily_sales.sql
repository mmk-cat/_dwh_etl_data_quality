-- Spark SQL: DWD -> DWS daily sales.
-- Temp view required: dwd_order_detail.
CREATE OR REPLACE TEMP VIEW dws_daily_sales AS
SELECT
    order_date,
    category,
    COUNT(*) AS order_count,
    COUNT(DISTINCT user_id) AS buyer_count,
    ROUND(SUM(amount), 2) AS gmv,
    ROUND(SUM(CASE WHEN order_status = 'refunded' THEN ABS(amount) ELSE 0 END), 2) AS refund_amount,
    ROUND(AVG(amount), 2) AS avg_order_amount
FROM dwd_order_detail
GROUP BY order_date, category;
