SELECT product_id, product_name, category, price, price_tier
FROM {{ ref('clean_products') }}
