"""
Lambda function: REST Countries API → S3

What it does:
  Calls restcountries.com (free, no auth, no rate limits)
  Fetches country data for all countries our customers are in
  Writes raw JSON to S3 landing zone

Why this is valuable:
  Our customers have cities across Africa and Europe.
  Country data lets us enrich customer records with:
  - Region and subregion
  - Population (market size)
  - Currencies (for multi-currency reporting)
  - Languages
  - Timezones

This becomes dim_countries in our dbt marts layer.
"""

import json
import boto3
import urllib.request
import os
from datetime import datetime

S3_BUCKET = os.environ["S3_BUCKET"]
s3        = boto3.client("s3")

# Countries our customers are based in — from our generated data
# In a real system this would be dynamic, queried from the warehouse
CUSTOMER_COUNTRIES = [
    "Kenya", "Nigeria", "Ghana", "South Africa", "Egypt",
    "United Kingdom", "France", "Germany", "Netherlands", "Spain"
]

BASE_URL = "https://restcountries.com/v3.1"

def fetch_country(country_name):
    """
    Fetch a single country by name from REST Countries API.
    Returns the first match — the API returns a list.
    """
    # URL encode the country name (spaces → %20)
    encoded = urllib.parse.quote(country_name)
    url     = f"{BASE_URL}/name/{encoded}?fullText=true"

    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode("utf-8"))
            # API returns a list — we take the first result
            return data[0] if data else None
    except Exception as e:
        print(f"  Warning: could not fetch {country_name}: {e}")
        return None

def extract_fields(raw):
    """
    Extract only the fields we care about from the raw API response.
    The full response has 50+ fields — we keep what's analytically useful.

    This is the 'schema on write' approach — we decide what matters
    at ingestion time rather than storing everything raw.
    """
    if not raw:
        return None

    # Safely extract nested fields with .get() to handle missing data
    currencies = raw.get("currencies", {})
    currency_codes = list(currencies.keys())

    languages = raw.get("languages", {})
    language_list = list(languages.values())

    return {
        "country_code"    : raw.get("cca2", ""),           # 2-letter code e.g. KE
        "country_code_3"  : raw.get("cca3", ""),           # 3-letter code e.g. KEN
        "country_name"    : raw.get("name", {}).get("common", ""),
        "official_name"   : raw.get("name", {}).get("official", ""),
        "region"          : raw.get("region", ""),          # Africa, Europe etc.
        "subregion"       : raw.get("subRegion", ""),
        "capital"         : raw.get("capital", [""])[0] if raw.get("capital") else "",
        "population"      : raw.get("population", 0),
        "area_km2"        : raw.get("area", 0),
        "currency_codes"  : currency_codes,
        "languages"       : language_list,
        "timezones"       : raw.get("timezones", []),
        "is_un_member"    : raw.get("unMember", False),
        "_ingested_at"    : datetime.utcnow().isoformat(),
        "_source"         : BASE_URL
    }

def write_to_s3(records):
    """Write country records to S3 with date partitioning."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key   = f"raw/countries/{today}/countries.json"

    s3.put_object(
        Bucket      = S3_BUCKET,
        Key         = key,
        Body        = json.dumps(records, indent=2),
        ContentType = "application/json"
    )
    print(f"Written {len(records)} countries to s3://{S3_BUCKET}/{key}")
    return key

import urllib.parse

def handler(event, context):

    print(f"REST Countries ingestion started at {datetime.utcnow().isoformat()}")
    print(f"Fetching {len(CUSTOMER_COUNTRIES)} countries...")

    records = []
    for country_name in CUSTOMER_COUNTRIES:
        print(f"  Fetching: {country_name}")
        raw       = fetch_country(country_name)
        extracted = extract_fields(raw)
        if extracted:
            records.append(extracted)

    key = write_to_s3(records)

    result = {
        "statusCode"       : 200,
        "countries_fetched": len(records),
        "s3_key"           : key
    }
    print("Complete:", json.dumps(result, indent=2))
    return {"statusCode": 200, "body": json.dumps(result)}
