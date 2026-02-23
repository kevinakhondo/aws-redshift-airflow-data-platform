"""
load_raw_data.py
Loads raw JSON files from S3 into Redshift landing schema
using the Redshift Data API (no direct connection needed).

Usage:
    python3 load_raw_data.py
    python3 load_raw_data.py --date 2026-02-21
"""

import boto3
import json
import time
import datetime
import argparse

REGION    = "us-east-1"
WORKGROUP = "data-platform-dev-wg"
DATABASE  = "dataplatform"
BUCKET    = "data-platform-dev-landing-63165c77"
ROLE_ARN  = "arn:aws:iam::884038419396:role/data-platform-redshift-s3-role"

client = boto3.client("redshift-data", region_name=REGION)
s3     = boto3.client("s3", region_name=REGION)

def run_sql(sql, desc=""):
    print(f"Running: {desc or sql[:60]}...")
    response = client.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql=sql
    )
    stmt_id = response["Id"]
    while True:
        status = client.describe_statement(Id=stmt_id)["Status"]
        if status == "FINISHED":
            print(f"  Done.")
            return stmt_id
        elif status == "FAILED":
            error = client.describe_statement(Id=stmt_id)["Error"]
            print(f"  FAILED: {error}")
            return None
        time.sleep(2)

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
    data     = json.loads(response["Body"].read().decode("utf-8"))
    cleaned  = [flatten_record(r) for r in data]
    ndjson   = "\n".join(json.dumps(r) for r in cleaned)
    nd_key   = s3_key.replace(".json", "_nd.json")
    s3.put_object(Bucket=BUCKET, Key=nd_key, Body=ndjson)
    print(f"  Converted {len(cleaned)} records → {nd_key}")
    return nd_key

def get_count(table):
    response = client.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql=f"SELECT COUNT(*) FROM {table}"
    )
    stmt_id = response["Id"]
    while True:
        status = client.describe_statement(Id=stmt_id)["Status"]
        if status == "FINISHED":
            result = client.get_statement_result(Id=stmt_id)
            return result["Records"][0][0]["longValue"]
        elif status == "FAILED":
            return -1
        time.sleep(2)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=datetime.datetime.utcnow().strftime("%Y-%m-%d"))
    args = parser.parse_args()
    today = args.date

    print(f"Loading data for date: {today}")

    # Create schemas
    for schema in ["landing", "cds", "marts"]:
        run_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}", f"Create schema {schema}")

    # Create tables
    run_sql("""
        CREATE TABLE IF NOT EXISTS landing.raw_sales (
            order_id       VARCHAR(20),
            customer_id    VARCHAR(20),
            product_id     VARCHAR(20),
            product_name   VARCHAR(200),
            category       VARCHAR(100),
            quantity       INTEGER,
            unit_price     DECIMAL(10,2),
            discount_pct   DECIMAL(5,2),
            gross_amount   DECIMAL(10,2),
            discount_amt   DECIMAL(10,2),
            net_amount     DECIMAL(10,2),
            order_date     DATE,
            status         VARCHAR(50),
            payment_method VARCHAR(50),
            city           VARCHAR(100),
            _ingested_at   TIMESTAMP,
            _source_file   VARCHAR(500)
        )
    """, "Create landing.raw_sales")

    run_sql("""
        CREATE TABLE IF NOT EXISTS landing.raw_customers (
            customer_id  VARCHAR(20),
            first_name   VARCHAR(100),
            last_name    VARCHAR(100),
            email        VARCHAR(200),
            city         VARCHAR(100),
            signup_date  DATE,
            loyalty_tier VARCHAR(50),
            _ingested_at TIMESTAMP,
            _source_file VARCHAR(500)
        )
    """, "Create landing.raw_customers")

    run_sql("""
        CREATE TABLE IF NOT EXISTS landing.raw_products (
            product_id   VARCHAR(20),
            name         VARCHAR(200),
            category     VARCHAR(100),
            price        DECIMAL(10,2),
            _ingested_at TIMESTAMP,
            _source_file VARCHAR(500)
        )
    """, "Create landing.raw_products")

    run_sql("""
        CREATE TABLE IF NOT EXISTS landing.raw_countries (
            country_code   VARCHAR(10),
            country_code_3 VARCHAR(10),
            country_name   VARCHAR(200),
            official_name  VARCHAR(200),
            region         VARCHAR(100),
            subregion      VARCHAR(100),
            capital        VARCHAR(100),
            population     BIGINT,
            area_km2       DECIMAL(15,2),
            currency_codes VARCHAR(500),
            languages      VARCHAR(500),
            timezones      VARCHAR(500),
            is_un_member   BOOLEAN,
            _ingested_at   TIMESTAMP,
            _source        VARCHAR(500)
        )
    """, "Create landing.raw_countries")

    # Truncate
    for table in ["landing.raw_sales", "landing.raw_customers",
                  "landing.raw_products", "landing.raw_countries"]:
        run_sql(f"TRUNCATE {table}", f"Truncate {table}")

    # Load
    entities = [
        {"table": "landing.raw_sales",     "s3_key": f"raw/sales/{today}/sales.json"},
        {"table": "landing.raw_customers", "s3_key": f"raw/customers/{today}/customers.json"},
        {"table": "landing.raw_products",  "s3_key": f"raw/products/{today}/products.json"},
        {"table": "landing.raw_countries", "s3_key": f"raw/countries/{today}/countries.json"},
    ]

    for cmd in entities:
        print(f"\nProcessing {cmd['table']}...")
        nd_key = convert_to_ndjson(cmd["s3_key"])
        run_sql(f"""
            COPY {cmd['table']}
            FROM 's3://{BUCKET}/{nd_key}'
            IAM_ROLE '{ROLE_ARN}'
            JSON 'auto ignorecase'
            TIMEFORMAT 'auto'
            REGION '{REGION}'
            TRUNCATECOLUMNS
            BLANKSASNULL
            EMPTYASNULL
        """, f"COPY into {cmd['table']}")

    # Row counts
    print("\nRow counts:")
    for table in ["landing.raw_sales", "landing.raw_customers",
                  "landing.raw_products", "landing.raw_countries"]:
        count = get_count(table)
        print(f"  {table}: {count} rows")

    print("\nAll done!")

if __name__ == "__main__":
    main()

def grant_permissions():
    """Grant schema access to all users — needed for query editor visibility."""
    for schema in ["landing", "cds", "marts"]:
        run_sql(f"GRANT USAGE ON SCHEMA {schema} TO PUBLIC", f"Grant usage on {schema}")
        run_sql(f"GRANT ALL ON ALL TABLES IN SCHEMA {schema} TO PUBLIC", f"Grant tables in {schema}")

def grant_permissions():
    """Grant schema access to all users."""
    grants = [
        "GRANT USAGE ON SCHEMA landing TO PUBLIC",
        "GRANT USAGE ON SCHEMA cds TO PUBLIC",
        "GRANT USAGE ON SCHEMA marts TO PUBLIC",
        "GRANT ALL ON ALL TABLES IN SCHEMA landing TO PUBLIC",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA landing GRANT ALL ON TABLES TO PUBLIC",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA cds GRANT ALL ON TABLES TO PUBLIC",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT ALL ON TABLES TO PUBLIC",
    ]
    for sql in grants:
        run_sql(sql, sql[:60])

grant_permissions()
