SELECT order_id, customer_id, product_id, product_name, category, quantity,
       unit_price, discount_pct, gross_amount, discount_amt, net_amount,
       order_date, status, payment_method, city, _ingested_at, _source_file
FROM landing.raw_sales
