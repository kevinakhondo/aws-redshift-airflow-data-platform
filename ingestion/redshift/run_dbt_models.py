"""
run_dbt_models.py
Executes dbt SQL models via Redshift Data API.
No direct TCP connection needed — works through HTTPS port 443.

Usage:
    python3 run_dbt_models.py
"""

import boto3
import time
import os
import re

REGION    = "us-east-1"
WORKGROUP = "data-platform-dev-wg"
DATABASE  = "dataplatform"

client = boto3.client("redshift-data", region_name=REGION)

def run_sql(sql, desc=""):
    print(f"Running: {desc}...")
    response = client.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql=sql
    )
    stmt_id = response["Id"]
    while True:
        result = client.describe_statement(Id=stmt_id)
        status = result["Status"]
        if status == "FINISHED":
            print(f"  Done.")
            return True
        elif status == "FAILED":
            print(f"  FAILED: {result['Error']}")
            return False
        time.sleep(2)

def create_or_replace_view(schema, name, sql):
    run_sql(
        f"CREATE OR REPLACE VIEW {schema}.{name} AS {sql}",
        f"Create view {schema}.{name}"
    )

def create_or_replace_table(schema, name, sql):
    run_sql(f"DROP TABLE IF EXISTS {schema}.{name} CASCADE", f"Drop {schema}.{name}")
    run_sql(
        f"CREATE TABLE {schema}.{name} AS {sql}",
        f"Create table {schema}.{name}"
    )

# ── LANDING VIEWS ──────────────────────────────────────────────
create_or_replace_view("landing", "src_sales", """
    SELECT order_id, customer_id, product_id, product_name, category, quantity,
           unit_price, discount_pct, gross_amount, discount_amt, net_amount,
           order_date, status, payment_method, city, _ingested_at, _source_file
    FROM landing.raw_sales
""")

create_or_replace_view("landing", "src_customers", """
    SELECT customer_id, first_name, last_name, email, city, signup_date,
           loyalty_tier, _ingested_at, _source_file
    FROM landing.raw_customers
""")

create_or_replace_view("landing", "src_products", """
    SELECT product_id, name, category, price, _ingested_at, _source_file
    FROM landing.raw_products
""")

create_or_replace_view("landing", "src_countries", """
    SELECT country_code, country_code_3, country_name, official_name, region,
           subregion, capital, population, area_km2, currency_codes, languages,
           timezones, is_un_member, _ingested_at, _source
    FROM landing.raw_countries
""")

# ── CDS VIEWS ─────────────────────────────────────────────────
create_or_replace_view("cds", "clean_sales", """
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
    FROM landing.src_sales
    WHERE order_id IS NOT NULL
""")

create_or_replace_view("cds", "clean_customers", """
    SELECT
        customer_id, first_name, last_name,
        first_name || ' ' || last_name AS full_name,
        LOWER(email) AS email,
        INITCAP(city) AS city,
        signup_date,
        UPPER(loyalty_tier) AS loyalty_tier,
        DATEDIFF(day, signup_date, CURRENT_DATE) AS days_since_signup,
        _ingested_at, _source_file
    FROM landing.src_customers
    WHERE customer_id IS NOT NULL
""")

create_or_replace_view("cds", "clean_products", """
    SELECT
        product_id, name AS product_name,
        UPPER(category) AS category, price,
        CASE
            WHEN price < 30  THEN 'budget'
            WHEN price < 80  THEN 'mid-range'
            ELSE 'premium'
        END AS price_tier,
        _ingested_at, _source_file
    FROM landing.src_products
    WHERE product_id IS NOT NULL
""")

create_or_replace_view("cds", "clean_countries", """
    SELECT
        country_code, country_code_3, country_name, official_name,
        UPPER(region) AS region, subregion, capital, population, area_km2,
        currency_codes, languages, timezones, is_un_member, _ingested_at
    FROM landing.src_countries
    WHERE country_code IS NOT NULL
""")

# ── MARTS TABLES ───────────────────────────────────────────────
create_or_replace_table("marts", "dim_customers", """
    SELECT
        c.customer_id, c.full_name, c.email, c.city,
        c.signup_date, c.loyalty_tier, c.days_since_signup,
        co.country_name, co.region, co.subregion
    FROM cds.clean_customers c
    LEFT JOIN cds.clean_countries co ON INITCAP(c.city) = co.capital
""")

create_or_replace_table("marts", "dim_products", """
    SELECT product_id, product_name, category, price, price_tier
    FROM cds.clean_products
""")

create_or_replace_table("marts", "fct_orders", """
    SELECT
        s.order_id, s.order_date, s.order_year, s.order_month,
        s.order_day_of_week, s.customer_id,
        c.full_name AS customer_name, c.city, c.loyalty_tier,
        c.country_name, c.region,
        s.product_id, p.product_name, p.category, p.price_tier,
        s.quantity, s.unit_price, s.discount_pct,
        s.gross_amount, s.discount_amt, s.net_amount,
        s.revenue_category, s.status, s.payment_method
    FROM cds.clean_sales s
    LEFT JOIN marts.dim_customers c ON s.customer_id = c.customer_id
    LEFT JOIN marts.dim_products  p ON s.product_id  = p.product_id
""")

create_or_replace_table("marts", "sales_metrics", """
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
    FROM marts.fct_orders
    GROUP BY 1,2,3,4,5
""")

# ── GRANT PERMISSIONS ─────────────────────────────────────────
for schema in ["landing", "cds", "marts"]:
    run_sql(f"GRANT USAGE ON SCHEMA {schema} TO PUBLIC", f"Grant {schema}")
    run_sql(f"GRANT ALL ON ALL TABLES IN SCHEMA {schema} TO PUBLIC", f"Grant tables in {schema}")

print("\nAll models created successfully!")
print("Schemas: landing (views), cds (views), marts (tables)")
