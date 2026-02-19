{{
    config(
        materialized='table',
        tags=['marts', 'customer_analytics']
    )
}}

WITH customer_orders AS (
    SELECT
        c.customer_id,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        c.email,
        c.segment,
        c.region,
        c.created_at AS customer_since,
        DATEDIFF('day', c.created_at, CURRENT_TIMESTAMP()) AS days_as_customer,
        
        COUNT(DISTINCT o.order_id) AS total_orders,
        COALESCE(SUM(o.quantity * o.unit_price * (1 - o.discount_pct/100)), 0) AS total_revenue,
        COALESCE(SUM(o.quantity), 0) AS total_units,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date
        
    FROM {{ source('ecommerce', 'ext_customers') }} c
    LEFT JOIN {{ source('ecommerce', 'ext_orders') }} o 
        ON c.customer_id = o.customer_id
        AND o.status IN ('COMPLETED', 'SHIPPED')
    GROUP BY 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.segment,
        c.region,
        c.created_at
),

rfm_metrics AS (
    SELECT
        *,
        DATEDIFF('day', last_order_date, CURRENT_TIMESTAMP()) AS days_since_last_order,
        
        CASE 
            WHEN total_orders > 0 THEN total_revenue / total_orders 
            ELSE 0 
        END AS avg_order_value,
        
        CASE 
            WHEN total_orders > 1 THEN 
                DATEDIFF('day', first_order_date, last_order_date) / (total_orders - 1)
            ELSE NULL 
        END AS avg_days_between_orders
        
    FROM customer_orders
),

rfm_scores AS (
    SELECT
        *,
        
        CASE 
            WHEN days_since_last_order IS NULL THEN 1
            WHEN days_since_last_order <= 7 THEN 5
            WHEN days_since_last_order <= 30 THEN 4
            WHEN days_since_last_order <= 60 THEN 3
            WHEN days_since_last_order <= 90 THEN 2
            ELSE 1
        END AS recency_score,
        
        CASE 
            WHEN total_orders >= 10 THEN 5
            WHEN total_orders >= 5 THEN 4
            WHEN total_orders >= 3 THEN 3
            WHEN total_orders >= 1 THEN 2
            ELSE 1
        END AS frequency_score,
        
        CASE 
            WHEN total_revenue >= 5000 THEN 5
            WHEN total_revenue >= 2000 THEN 4
            WHEN total_revenue >= 500 THEN 3
            WHEN total_revenue > 0 THEN 2
            ELSE 1
        END AS monetary_score
        
    FROM rfm_metrics
),

final AS (
    SELECT
        customer_id,
        customer_name,
        email,
        segment,
        region,
        customer_since,
        days_as_customer,
        
        total_orders,
        ROUND(total_revenue, 2) AS total_revenue,
        total_units,
        first_order_date,
        last_order_date,
        days_since_last_order,
        ROUND(avg_order_value, 2) AS avg_order_value,
        ROUND(avg_days_between_orders, 1) AS avg_days_between_orders,
        
        recency_score,
        frequency_score,
        monetary_score,
        (recency_score + frequency_score + monetary_score) AS rfm_score,
        
        ROUND(
            (recency_score * 0.3 + frequency_score * 0.3 + monetary_score * 0.4) * 
            (total_revenue / NULLIF(days_as_customer, 0) * 365),
            2
        ) AS ltv_score,
        
        CASE 
            WHEN (recency_score + frequency_score + monetary_score) >= 12 THEN 'Platinum'
            WHEN (recency_score + frequency_score + monetary_score) >= 9 THEN 'Gold'
            WHEN (recency_score + frequency_score + monetary_score) >= 6 THEN 'Silver'
            ELSE 'Bronze'
        END AS customer_tier
        
    FROM rfm_scores
)

SELECT * FROM final
