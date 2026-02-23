SELECT
    c.customer_id, c.full_name, c.email, c.city,
    c.signup_date, c.loyalty_tier, c.days_since_signup,
    co.country_name, co.region, co.subregion
FROM {{ ref('clean_customers') }} c
LEFT JOIN {{ ref('clean_countries') }} co ON INITCAP(c.city) = co.capital
