"""
data_platform_pipeline.py
Daily pipeline DAG — orchestrates the full data platform.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import time
import json

REGION      = "us-east-1"
WORKGROUP   = "data-platform-dev-wg"
DATABASE    = "dataplatform"
BUCKET      = "data-platform-dev-landing-63165c77"
ROLE_ARN    = "arn:aws:iam::884038419396:role/data-platform-redshift-s3-role"
GLUE_JOB    = "data-platform-csv-ingest"
LAMBDA_FUNC = "data-platform-fakestore-ingest"

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

def run_sql(client, sql, desc=""):
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
            raise Exception(f"SQL failed: {result['Error']}")
        time.sleep(2)

def trigger_lambda(**context):
    client = boto3.client("lambda", region_name=REGION)
    response = client.invoke(
        FunctionName=LAMBDA_FUNC,
        InvocationType="RequestResponse"
    )
    result = json.loads(response["Payload"].read())
    print(f"Lambda result: {result}")
    if response["StatusCode"] != 200:
        raise Exception(f"Lambda failed: {result}")

def trigger_glue(**context):
    client = boto3.client("glue", region_name=REGION)
    response = client.start_job_run(JobName=GLUE_JOB)
    run_id = response["JobRunId"]
    print(f"Glue job started: {run_id}")
    while True:
        status = client.get_job_run(JobName=GLUE_JOB, RunId=run_id)
        state = status["JobRun"]["JobRunState"]
        print(f"Glue status: {state}")
        if state == "SUCCEEDED":
            break
        elif state in ["FAILED", "ERROR", "TIMEOUT"]:
            raise Exception(f"Glue job failed: {state}")
        time.sleep(15)

def load_raw_data(**context):
    import datetime as dt
    s3 = boto3.client("s3", region_name=REGION)
    rc = boto3.client("redshift-data", region_name=REGION)
    today = dt.datetime.utcnow().strftime("%Y-%m-%d")

    def fix_timestamp(val):
        if not val:
            return None
        return str(val).replace("T", " ")[:19]

    def flatten_record(record):
        cleaned = {}
        for k, v in record.items():
            if isinstance(v, (list, dict)):
                cleaned[k] = json.dumps(v)
            elif k in ["_ingested_at", "ingested_at"]:
                cleaned[k] = fix_timestamp(v)
            else:
                cleaned[k] = v
        return cleaned

    def convert_to_ndjson(s3_key):
        response = s3.get_object(Bucket=BUCKET, Key=s3_key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        cleaned = [flatten_record(r) for r in data]
        ndjson = "\n".join(json.dumps(r) for r in cleaned)
        nd_key = s3_key.replace(".json", "_nd.json")
        s3.put_object(Bucket=BUCKET, Key=nd_key, Body=ndjson)
        print(f"Converted {len(cleaned)} records → {nd_key}")
        return nd_key

    entities = [
        {"table": "landing.raw_sales",     "s3_key": f"raw/sales/{today}/sales.json"},
        {"table": "landing.raw_customers", "s3_key": f"raw/customers/{today}/customers.json"},
        {"table": "landing.raw_products",  "s3_key": f"raw/products/{today}/products.json"},
        {"table": "landing.raw_countries", "s3_key": f"raw/countries/{today}/countries.json"},
    ]

    for cmd in entities:
        run_sql(rc, f"TRUNCATE {cmd['table']}", f"Truncate {cmd['table']}")
        nd_key = convert_to_ndjson(cmd["s3_key"])
        run_sql(rc, f"""
            COPY {cmd['table']}
            FROM 's3://{BUCKET}/{nd_key}'
            IAM_ROLE '{ROLE_ARN}'
            JSON 'auto ignorecase'
            TIMEFORMAT 'auto'
            REGION '{REGION}'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        """, f"COPY {cmd['table']}")

def run_transformations(**context):
    client = boto3.client("redshift-data", region_name=REGION)

    def create_view(schema, name, sql):
        # Drop first to avoid column mismatch errors
        run_sql(client, f"DROP VIEW IF EXISTS {schema}.{name} CASCADE", f"Drop view {schema}.{name}")
        run_sql(client, f"CREATE VIEW {schema}.{name} AS {sql}", f"View {schema}.{name}")

    def create_table(schema, name, sql):
        run_sql(client, f"DROP TABLE IF EXISTS {schema}.{name} CASCADE", f"Drop {schema}.{name}")
        run_sql(client, f"CREATE TABLE {schema}.{name} AS {sql}", f"Table {schema}.{name}")

    # Landing views
    create_view("landing", "src_sales", "SELECT * FROM landing.raw_sales")
    create_view("landing", "src_customers", "SELECT * FROM landing.raw_customers")
    create_view("landing", "src_products", "SELECT * FROM landing.raw_products")
    create_view("landing", "src_countries", "SELECT * FROM landing.raw_countries")

    # CDS views
    create_view("cds", "clean_sales", """
        SELECT order_id, customer_id, product_id, product_name,
               UPPER(category) AS category, quantity, unit_price, discount_pct,
               gross_amount, discount_amt, net_amount, order_date,
               UPPER(status) AS status, LOWER(payment_method) AS payment_method,
               INITCAP(city) AS city,
               CASE WHEN net_amount < 20 THEN 'low'
                    WHEN net_amount < 100 THEN 'medium'
                    ELSE 'high' END AS revenue_category,
               EXTRACT(YEAR FROM order_date) AS order_year,
               EXTRACT(MONTH FROM order_date) AS order_month,
               EXTRACT(DOW FROM order_date) AS order_day_of_week,
               _ingested_at, _source_file
        FROM landing.src_sales WHERE order_id IS NOT NULL
    """)

    create_view("cds", "clean_customers", """
        SELECT customer_id, first_name, last_name,
               first_name || ' ' || last_name AS full_name,
               LOWER(email) AS email, INITCAP(city) AS city, signup_date,
               UPPER(loyalty_tier) AS loyalty_tier,
               DATEDIFF(day, signup_date, CURRENT_DATE) AS days_since_signup,
               _ingested_at, _source_file
        FROM landing.src_customers WHERE customer_id IS NOT NULL
    """)

    create_view("cds", "clean_products", """
        SELECT product_id, name AS product_name, UPPER(category) AS category, price,
               CASE WHEN price < 30 THEN 'budget'
                    WHEN price < 80 THEN 'mid-range'
                    ELSE 'premium' END AS price_tier,
               _ingested_at, _source_file
        FROM landing.src_products WHERE product_id IS NOT NULL
    """)

    create_view("cds", "clean_countries", """
        SELECT country_code, country_code_3, country_name, official_name,
               UPPER(region) AS region, subregion, capital, population,
               area_km2, currency_codes, languages, timezones, is_un_member
        FROM landing.src_countries WHERE country_code IS NOT NULL
    """)

    # Marts tables
    create_table("marts", "dim_customers", """
        SELECT c.customer_id, c.full_name, c.email, c.city,
               c.signup_date, c.loyalty_tier, c.days_since_signup,
               co.country_name, co.region, co.subregion
        FROM cds.clean_customers c
        LEFT JOIN cds.clean_countries co ON INITCAP(c.city) = co.capital
    """)

    create_table("marts", "dim_products", """
        SELECT product_id, product_name, category, price, price_tier
        FROM cds.clean_products
    """)

    create_table("marts", "fct_orders", """
        SELECT s.order_id, s.order_date, s.order_year, s.order_month,
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

    create_table("marts", "sales_metrics", """
        SELECT order_year, order_month, category, region, payment_method,
               COUNT(DISTINCT order_id)   AS total_orders,
               COUNT(DISTINCT customer_id) AS unique_customers,
               SUM(quantity)              AS total_units_sold,
               SUM(gross_amount)          AS total_gross_revenue,
               SUM(discount_amt)          AS total_discounts,
               SUM(net_amount)            AS total_net_revenue,
               AVG(net_amount)            AS avg_order_value,
               SUM(CASE WHEN status = 'COMPLETED' THEN net_amount ELSE 0 END) AS completed_revenue,
               COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) AS cancelled_orders
        FROM marts.fct_orders
        GROUP BY 1,2,3,4,5
    """)

    # Grant permissions
    for schema in ["landing", "cds", "marts"]:
        run_sql(client, f"GRANT USAGE ON SCHEMA {schema} TO PUBLIC", f"Grant {schema}")
        run_sql(client, f"GRANT ALL ON ALL TABLES IN SCHEMA {schema} TO PUBLIC", f"Grant tables {schema}")

    print("All transformations complete!")

with DAG(
    dag_id="data_platform_pipeline",
    default_args=default_args,
    description="Daily data platform pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-platform", "production"],
) as dag:

    t1 = PythonOperator(task_id="trigger_lambda", python_callable=trigger_lambda)
    t2 = PythonOperator(task_id="trigger_glue", python_callable=trigger_glue)
    t3 = PythonOperator(task_id="load_raw_data", python_callable=load_raw_data)
    t4 = PythonOperator(task_id="run_transformations", python_callable=run_transformations)

    t1 >> t2 >> t3 >> t4
