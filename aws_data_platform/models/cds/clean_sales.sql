SELECT
    order_id, customer_id, product_id, product_name,
    UPPER(category) AS category, quantity, unit_price, discount_pct,
    gross_amount, discount_amt, net_amount, order_date,
    UPPER(status) AS status,
    LOWER(payment_method) AS payment_method,
    INITCAP(city) AS city,
    CASE
        WHEN net_amount < 20  THEN 'low'
        WHEN net_amount < 100 THEN 'medium'
        ELSE 'high'
    END AS revenue_category,
    EXTRACT(YEAR  FROM order_date) AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month,
    EXTRACT(DOW   FROM order_date) AS order_day_of_week,
    _ingested_at, _source_file
FROM {{ ref('src_sales') }}
WHERE order_id IS NOT NULL
