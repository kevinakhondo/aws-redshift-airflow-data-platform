SELECT
    order_year, order_month, category, region, payment_method,
    COUNT(DISTINCT order_id)        AS total_orders,
    COUNT(DISTINCT customer_id)     AS unique_customers,
    SUM(quantity)                   AS total_units_sold,
    SUM(gross_amount)               AS total_gross_revenue,
    SUM(discount_amt)               AS total_discounts,
    SUM(net_amount)                 AS total_net_revenue,
    AVG(net_amount)                 AS avg_order_value,
    SUM(CASE WHEN status = 'COMPLETED' THEN net_amount ELSE 0 END) AS completed_revenue,
    COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS cancelled_orders
FROM {{ ref('fct_orders') }}
GROUP BY 1,2,3,4,5
