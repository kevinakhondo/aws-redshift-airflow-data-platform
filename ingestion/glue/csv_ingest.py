"""
Glue job: CSV files in S3 → cleaned JSON in S3 landing zone

What it does:
  1. Reads CSV files from S3 uploads folder
  2. Cleans and type-casts every field
  3. Adds audit fields (ingested_at, source_file)
  4. Writes clean JSON to raw/ folder for dbt to pick up

Why JSON output and not CSV?
  JSON handles nested structures, nulls, and mixed types
  better than CSV. dbt and Redshift both handle JSON well.

Why not load directly to Redshift here?
  Loading to S3 first decouples ingestion from the warehouse.
  If Redshift is down, data is safe in S3. We can replay it.
  This is called the 'landing zone' pattern.
"""

import sys
import json
import boto3
from datetime import datetime

# In a real Glue job these come from getResolvedOptions.
# For our setup we read them from environment or hardcode for simplicity.
import os

S3_BUCKET = os.environ.get("S3_BUCKET", "data-platform-dev-landing-63165c77")

s3 = boto3.client("s3")

# -------------------------------------------------------
# Schema definitions — what type each field should be.
# Explicit typing prevents silent bugs downstream.
# -------------------------------------------------------
SALES_SCHEMA = {
    "order_id"       : str,
    "customer_id"    : str,
    "product_id"     : str,
    "product_name"   : str,
    "category"       : str,
    "quantity"       : int,
    "unit_price"     : float,
    "discount_pct"   : float,
    "gross_amount"   : float,
    "discount_amt"   : float,
    "net_amount"     : float,
    "order_date"     : str,
    "status"         : str,
    "payment_method" : str,
    "city"           : str,
}

CUSTOMERS_SCHEMA = {
    "customer_id"  : str,
    "first_name"   : str,
    "last_name"    : str,
    "email"        : str,
    "city"         : str,
    "signup_date"  : str,
    "loyalty_tier" : str,
}

PRODUCTS_SCHEMA = {
    "product_id" : str,
    "name"       : str,
    "category"   : str,
    "price"      : float,
}

def read_csv_from_s3(bucket, key):
    """Read CSV from S3, return list of raw string dicts."""
    print(f"Reading s3://{bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    content  = response["Body"].read().decode("utf-8")
    lines    = [l for l in content.strip().split("\n") if l.strip()]
    headers  = lines[0].split(",")
    records  = []
    for line in lines[1:]:
        values = line.split(",")
        # zip pairs headers with values — handles any number of columns
        record = dict(zip(headers, values))
        records.append(record)
    print(f"  Read {len(records)} records")
    return records

def apply_schema(record, schema, source_key):
    """
    Cast every field to its correct type using the schema.
    Add audit fields for lineage tracking.
    
    If a cast fails we log it and skip the record —
    bad data goes to a dead letter log, not into the warehouse.
    """
    cleaned = {}
    for field, cast_fn in schema.items():
        raw_value = record.get(field, "").strip()
        try:
            cleaned[field] = cast_fn(raw_value) if raw_value else None
        except (ValueError, TypeError) as e:
            print(f"  Warning: could not cast {field}='{raw_value}' to {cast_fn.__name__}: {e}")
            cleaned[field] = None

    # Audit fields — added to every record regardless of entity type
    cleaned["_ingested_at"] = datetime.utcnow().isoformat()
    cleaned["_source_file"] = source_key
    return cleaned

def write_json_to_s3(records, entity, bucket):
    """Write cleaned records as JSON to the landing zone."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key   = f"raw/{entity}/{today}/{entity}.json"
    s3.put_object(
        Bucket      = bucket,
        Key         = key,
        Body        = json.dumps(records, indent=2),
        ContentType = "application/json"
    )
    print(f"  Written {len(records)} records to s3://{bucket}/{key}")
    return key

def process_entity(entity, schema, bucket):
    """End-to-end processing for one entity."""
    source_key = f"uploads/{entity}/{entity}.csv"
    raw        = read_csv_from_s3(bucket, source_key)
    cleaned    = [apply_schema(r, schema, source_key) for r in raw]

    # Filter out completely empty records
    valid = [r for r in cleaned if any(
        v is not None for k, v in r.items() if not k.startswith("_")
    )]
    print(f"  Valid records: {len(valid)} / {len(cleaned)}")
    return write_json_to_s3(valid, entity, bucket)

def main():
    print(f"Glue CSV ingestion started at {datetime.utcnow().isoformat()}")
    print(f"Target bucket: {S3_BUCKET}")

    results = {}
    for entity, schema in [
        ("sales",     SALES_SCHEMA),
        ("customers", CUSTOMERS_SCHEMA),
        ("products",  PRODUCTS_SCHEMA),
    ]:
        key = process_entity(entity, schema, S3_BUCKET)
        results[entity] = key

    print("Glue job complete:", json.dumps(results, indent=2))

main()
