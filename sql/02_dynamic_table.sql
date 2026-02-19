-- ============================================================================
-- DATA ENGINEERING DEMO - PART 2: DYNAMIC TABLE
-- ============================================================================
-- Creates a Dynamic Table for real-time hourly sales metrics aggregation
-- The DT automatically refreshes based on changes to source Iceberg tables
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE TLV_BUILD_HOL;
USE WAREHOUSE COMPUTE_WH;

CREATE SCHEMA IF NOT EXISTS TLV_BUILD_HOL.DATA_ENG_DEMO;
USE SCHEMA DATA_ENG_DEMO;

-- Create dedicated warehouse for Dynamic Tables
CREATE WAREHOUSE IF NOT EXISTS TLV_DT_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- ============================================================================
-- DYNAMIC TABLE: HOURLY SALES METRICS
-- ============================================================================
-- Aggregates order data by hour and product category
-- TARGET_LAG: Refreshes within 5 minutes of source changes
-- WAREHOUSE: Uses dedicated warehouse for refresh operations

CREATE OR REPLACE DYNAMIC TABLE dt_hourly_sales_metrics
    TARGET_LAG = '5 minutes'
    WAREHOUSE = TLV_DT_WH
AS
SELECT
    DATE_TRUNC('HOUR', o.order_date) AS sales_hour,
    p.category,
    p.subcategory,
    
    -- Order metrics
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    
    -- Volume metrics
    SUM(o.quantity) AS units_sold,
    
    -- Revenue metrics
    SUM(o.quantity * o.unit_price) AS gross_revenue,
    SUM(o.quantity * o.unit_price * (1 - o.discount_pct/100)) AS net_revenue,
    SUM(o.quantity * o.unit_price * o.discount_pct/100) AS total_discounts,
    
    -- Averages
    AVG(o.quantity * o.unit_price) AS avg_order_value,
    AVG(o.quantity) AS avg_units_per_order,
    
    -- Cost and margin (joining with product cost)
    SUM(o.quantity * p.cost_price) AS total_cost,
    SUM(o.quantity * o.unit_price * (1 - o.discount_pct/100)) - SUM(o.quantity * p.cost_price) AS gross_profit,
    
    -- Status breakdown
    COUNT(CASE WHEN o.status = 'COMPLETED' THEN 1 END) AS completed_orders,
    COUNT(CASE WHEN o.status = 'PENDING' THEN 1 END) AS pending_orders,
    COUNT(CASE WHEN o.status = 'PROCESSING' THEN 1 END) AS processing_orders,
    COUNT(CASE WHEN o.status = 'SHIPPED' THEN 1 END) AS shipped_orders

FROM EXTERNAL_ICEBERG.ext_orders o
INNER JOIN EXTERNAL_ICEBERG.ext_products p 
    ON o.product_id = p.product_id
GROUP BY 
    DATE_TRUNC('HOUR', o.order_date),
    p.category,
    p.subcategory;

-- ============================================================================
-- VERIFICATION & MONITORING
-- ============================================================================

-- Check Dynamic Table status
SHOW DYNAMIC TABLES LIKE 'DT_HOURLY_SALES_METRICS';

-- View refresh history
SELECT * 
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'TLV_BUILD_HOL.DATA_ENG_DEMO.DT_HOURLY_SALES_METRICS'
))
ORDER BY REFRESH_START_TIME DESC
LIMIT 10;

-- Sample the aggregated data
SELECT 
    sales_hour,
    category,
    total_orders,
    unique_customers,
    gross_revenue,
    gross_profit,
    ROUND(gross_profit / NULLIF(net_revenue, 0) * 100, 2) AS profit_margin_pct
FROM dt_hourly_sales_metrics
ORDER BY sales_hour DESC, gross_revenue DESC
LIMIT 20;

-- Daily summary from the hourly DT
SELECT 
    DATE(sales_hour) AS sales_date,
    SUM(total_orders) AS daily_orders,
    SUM(unique_customers) AS daily_customers,
    SUM(gross_revenue) AS daily_revenue,
    SUM(gross_profit) AS daily_profit
FROM dt_hourly_sales_metrics
GROUP BY DATE(sales_hour)
ORDER BY sales_date DESC;

-- ============================================================================
-- MANUAL REFRESH (if needed)
-- ============================================================================
-- ALTER DYNAMIC TABLE dt_hourly_sales_metrics REFRESH;
