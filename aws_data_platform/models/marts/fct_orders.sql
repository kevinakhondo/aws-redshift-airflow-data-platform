SELECT
    s.order_id, s.order_date, s.order_year, s.order_month,
    s.order_day_of_week, s.customer_id,
    c.full_name AS customer_name, c.city, c.loyalty_tier,
    c.country_name, c.region,
    s.product_id, p.product_name, p.category, p.price_tier,
    s.quantity, s.unit_price, s.discount_pct,
    s.gross_amount, s.discount_amt, s.net_amount,
    s.revenue_category, s.status, s.payment_method
FROM {{ ref('clean_sales') }} s
LEFT JOIN {{ ref('dim_customers') }} c ON s.customer_id = c.customer_id
LEFT JOIN {{ ref('dim_products') }}  p ON s.product_id  = p.product_id
