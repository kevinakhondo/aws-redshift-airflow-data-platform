SELECT
    product_id, name AS product_name,
    UPPER(category) AS category, price,
    CASE
        WHEN price < 30  THEN 'budget'
        WHEN price < 80  THEN 'mid-range'
        ELSE 'premium'
    END AS price_tier,
    _ingested_at, _source_file
FROM {{ ref('src_products') }}
WHERE product_id IS NOT NULL
