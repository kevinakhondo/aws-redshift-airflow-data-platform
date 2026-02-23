SELECT
    country_code, country_code_3, country_name, official_name,
    UPPER(region) AS region, subregion, capital, population, area_km2,
    currency_codes, languages, timezones, is_un_member, _ingested_at
FROM {{ ref('src_countries') }}
WHERE country_code IS NOT NULL
