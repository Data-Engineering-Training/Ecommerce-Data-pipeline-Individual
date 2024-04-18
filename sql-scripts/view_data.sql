CREATE DATABASE IF NOT EXISTS Ransbet;
USE  Ransbet;

-- DESCRIBE orders
select order_id, customer_id, last_used_platform
FROM orders
LIMIT 100