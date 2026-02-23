SELECT
    customer_id, first_name, last_name,
    first_name || ' ' || last_name AS full_name,
    LOWER(email) AS email,
    INITCAP(city) AS city,
    signup_date,
    UPPER(loyalty_tier) AS loyalty_tier,
    DATEDIFF(day, signup_date, CURRENT_DATE) AS days_since_signup,
    _ingested_at, _source_file
FROM {{ ref('src_customers') }}
WHERE customer_id IS NOT NULL
