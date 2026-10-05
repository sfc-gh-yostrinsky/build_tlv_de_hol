-- Dynamic table DAG over the shared raw lake (Glue Iceberg tables via the CLD).
--
--   lake.orders ─┐
--   lake.products├─► DT_ORDER_LINES ─► DT_HOURLY_SALES_METRICS ─► DT_DAILY_CATEGORY_REGION
--   lake.customers┘     (DOWNSTREAM)        (DOWNSTREAM)               (leaf, task-driven)
--
-- All three: REFRESH_MODE = ADAPTIVE (incremental, with per-refresh fallback to a
-- full recompute only when a change is large enough that incremental stops paying
-- off) and INITIALIZE = ON_SCHEDULE (no initial refresh at deploy time).
--
-- Every DT is TARGET_LAG = DOWNSTREAM. Nothing consumes the leaf, so it does NOT
-- refresh on its own: the serverless task T_REFRESH_DT refreshes the leaf, and a
-- manual refresh of the leaf refreshes its upstream DTs first.

{% set S = 'TLV_BUILD_HOL' ~ env_suffix ~ '.DATA_ENG_DEMO' %}

DEFINE DYNAMIC TABLE {{S}}.DT_ORDER_LINES
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = TLV_DT_WH{{env_suffix}}
    REFRESH_MODE = ADAPTIVE
    INITIALIZE = ON_SCHEDULE
    COMMENT = 'TLV DE HOL DT 1/3: order lines enriched with product + customer ({{dbt_target}}). Managed by DCM.'
AS
SELECT
    o.order_id,
    o.order_date,
    DATE_TRUNC('HOUR', o.order_date)                           AS sales_hour,
    o.status,
    o.customer_id,
    c.segment,
    c.region,
    o.product_id,
    p.category,
    p.subcategory,
    o.quantity,
    o.unit_price,
    o.discount_pct,
    p.cost_price,
    o.quantity * o.unit_price                                  AS gross_amount,
    o.quantity * o.unit_price * (1 - o.discount_pct / 100)     AS net_amount,
    o.quantity * o.unit_price * o.discount_pct / 100           AS discount_amount,
    o.quantity * p.cost_price                                  AS cost_amount
FROM {{lake_db}}.{{lake_schema}}.orders o
INNER JOIN {{lake_db}}.{{lake_schema}}.products p ON o.product_id = p.product_id
INNER JOIN {{lake_db}}.{{lake_schema}}.customers c ON o.customer_id = c.customer_id;

DEFINE DYNAMIC TABLE {{S}}.DT_HOURLY_SALES_METRICS
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = TLV_DT_WH{{env_suffix}}
    REFRESH_MODE = ADAPTIVE
    INITIALIZE = ON_SCHEDULE
    COMMENT = 'TLV DE HOL DT 2/3: hourly sales by category, subcategory and region ({{dbt_target}}). Managed by DCM.'
AS
SELECT
    sales_hour,
    category,
    subcategory,
    region,
    COUNT(*)                                                   AS order_lines,
    SUM(quantity)                                              AS units_sold,
    SUM(gross_amount)                                          AS gross_revenue,
    SUM(net_amount)                                            AS net_revenue,
    SUM(discount_amount)                                       AS total_discounts,
    SUM(cost_amount)                                           AS total_cost,
    SUM(net_amount) - SUM(cost_amount)                         AS gross_profit,
    COUNT_IF(status = 'COMPLETED')                             AS completed_orders,
    COUNT_IF(status = 'PENDING')                               AS pending_orders,
    COUNT_IF(status = 'PROCESSING')                            AS processing_orders,
    COUNT_IF(status = 'SHIPPED')                               AS shipped_orders
FROM {{S}}.DT_ORDER_LINES
GROUP BY sales_hour, category, subcategory, region;

DEFINE DYNAMIC TABLE {{S}}.DT_DAILY_CATEGORY_REGION
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE = TLV_DT_WH{{env_suffix}}
    REFRESH_MODE = ADAPTIVE
    INITIALIZE = ON_SCHEDULE
    COMMENT = 'TLV DE HOL DT 3/3 (leaf): daily sales by category and region; refreshed by serverless task T_REFRESH_DT ({{dbt_target}}). Managed by DCM.'
AS
SELECT
    DATE_TRUNC('DAY', sales_hour)                              AS sales_date,
    category,
    region,
    SUM(order_lines)                                           AS order_lines,
    SUM(units_sold)                                            AS units_sold,
    SUM(net_revenue)                                           AS net_revenue,
    SUM(gross_profit)                                          AS gross_profit,
    SUM(completed_orders)                                      AS completed_orders
FROM {{S}}.DT_HOURLY_SALES_METRICS
GROUP BY DATE_TRUNC('DAY', sales_hour), category, region;
