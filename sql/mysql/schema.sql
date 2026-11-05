-- MySQL 8.x / MariaDB schema for the DQ warehouse.
-- Dirty ODS strings are preserved (VARCHAR) so quality rules can detect them.
CREATE TABLE IF NOT EXISTS ods_orders (
    order_id VARCHAR(20),
    user_id VARCHAR(20),
    product_id VARCHAR(20),
    quantity INT,
    unit_price DECIMAL(18,2),
    amount DECIMAL(18,2),
    order_status VARCHAR(30),
    order_time VARCHAR(32),
    pay_time VARCHAR(32),
    channel VARCHAR(30),
    coupon_amount DECIMAL(18,2),
    province VARCHAR(30),
    KEY idx_ods_orders_id (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ods_users (
    user_id VARCHAR(20),
    user_name VARCHAR(100),
    city VARCHAR(50),
    register_time VARCHAR(32),
    status VARCHAR(20),
    KEY idx_ods_users_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ods_products (
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(30),
    list_price DECIMAL(18,2),
    status VARCHAR(20),
    KEY idx_ods_products_id (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_user (
    user_id VARCHAR(20),
    user_name VARCHAR(100),
    city VARCHAR(50),
    register_time VARCHAR(32),
    status VARCHAR(20),
    KEY idx_dim_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_product (
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(30),
    list_price DECIMAL(18,2),
    status VARCHAR(20),
    KEY idx_dim_product_id (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dwd_order_detail (
    order_id VARCHAR(20),
    user_id VARCHAR(20),
    user_name VARCHAR(100),
    city VARCHAR(50),
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(30),
    quantity INT,
    unit_price DECIMAL(18,2),
    list_price DECIMAL(18,2),
    amount DECIMAL(18,2),
    coupon_amount DECIMAL(18,2),
    order_status VARCHAR(30),
    order_time VARCHAR(32),
    order_date VARCHAR(20),
    pay_time VARCHAR(32),
    channel VARCHAR(30),
    province VARCHAR(30),
    user_status VARCHAR(20),
    product_status VARCHAR(20),
    KEY idx_dwd_order_id (order_id),
    KEY idx_dwd_date_category (order_date, category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dws_daily_sales (
    order_date VARCHAR(20),
    category VARCHAR(30),
    order_count INT,
    buyer_count INT,
    gmv DECIMAL(18,2),
    refund_amount DECIMAL(18,2),
    avg_order_amount DECIMAL(18,2),
    KEY idx_dws_date (order_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dws_channel_daily (
    order_date VARCHAR(20),
    channel VARCHAR(30),
    order_count INT,
    gmv DECIMAL(18,2),
    KEY idx_dws_channel_date (order_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
